"""Direct-EC2 capacity coordinator and independent tagged-instance janitor.

A launch intent is persisted before RunInstances. Only a definitive capacity rejection
permits a new intent. The janitor scans before accessing DynamoDB and never relies on a
candidate heartbeat, ASG membership, or a functioning coordinator to enforce expiry.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import base64
import copy
import hashlib
import json
import os
import re
import time

from . import durable
from .ami_image import launch_template_data
from .common import (BINARIES, MAX_BINARY, REQUEST_KEY, STATUS_KEY, WINDOW_KEY,
                     get_json, put_json, error_code, retryable_io, INSTANCE_TYPE, require, timestamp, utc,
                     durable_plan_contract, RevisionConflict)

REQUEST = "control/durable-request.json"
RECEIPT = "output/durable-receipt.json"
MAX_RECEIPT = 100_000
LIVE = {"pending", "running", "stopping", "stopped", "shutting-down"}


@dataclass(frozen=True)
class Configuration:
    region: str
    account: str
    namespace: str
    table: str
    bucket: str
    lt_id: str
    lt_version: str
    ami: str
    source: str
    subnet: str
    security_group: str
    profile_arn: str
    runtime_group: str
    termination_role: str

    @classmethod
    def environment(cls, *, runtime=False):
        if runtime:
            require(os.environ.get("RCP_DURABLE_ENABLED") == "true", "direct runtime is disabled")
        return cls(**{key: os.environ["RCP_DURABLE_" + key.upper()] for key in cls.__dataclass_fields__})

    def pins(self):
        return {key: getattr(self, key) for key in durable.PIN_KEYS}


def clients(region=None):
    import boto3
    from botocore.config import Config
    config = Config(connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 1})
    return {name: boto3.client(name, region_name=region, config=config) for name in
            ("dynamodb", "ec2", "scheduler", "s3")}


class AllocationDeferred(Exception):
    """A missing or stale heartbeat blocks allocation without failing the execution."""


def transient(error):
    return isinstance(error, RevisionConflict) or retryable_io(error) or error_code(error) == "ConditionalCheckFailedException"


def capacity_rejection(error):
    response = getattr(error, "response", None) or {}
    metadata = response.get("ResponseMetadata", {})
    request_id = metadata.get("RequestId")
    return (error_code(error) == "InsufficientInstanceCapacity" and
            isinstance(request_id, str) and re.fullmatch(r"[A-Za-z0-9-]{1,64}", request_id) and
            metadata.get("HTTPStatusCode") in (400, 500, 503))


def verify_launch_data(config, data):
    require(data.get("ImageId") == config.ami and data.get("InstanceType") == INSTANCE_TYPE and
            not data.get("InstanceMarketOptions") and not data.get("InstanceRequirements") and
            not data.get("DisableApiTermination") and not data.get("DisableApiStop") and
            not data.get("KeyName") and not data.get("TagSpecifications") and
            not data.get("HibernationOptions", {}).get("Configured") and
            data.get("Placement", {}).get("Tenancy", "default") == "default" and
            data.get("InstanceInitiatedShutdownBehavior") == "terminate", "unexpected instance launch path")
    require(data.get("IamInstanceProfile") == {"Arn": config.profile_arn}, "instance profile differs")
    require(data.get("MetadataOptions") == dict(HttpTokens="required", HttpPutResponseHopLimit=1,
            HttpEndpoint="enabled", InstanceMetadataTags="disabled"), "unexpected metadata access settings")
    mappings = data.get("BlockDeviceMappings", [])
    require(len(mappings) == 1 and mappings[0].get("DeviceName") == "/dev/sda1" and
            mappings[0].get("Ebs") == dict(DeleteOnTermination=True, Encrypted=True, VolumeSize=16,
                                          VolumeType="gp3", KmsKeyId="alias/aws/ebs"),
            "unexpected or retained root volume")
    interfaces = data.get("NetworkInterfaces")
    require(interfaces == [dict(DeviceIndex=0, AssociatePublicIpAddress=True, DeleteOnTermination=True,
                                Groups=[config.security_group])], "unexpected network interface")
    return data


class Service:
    receipt_limit = MAX_RECEIPT

    def __init__(self, api, config, now=lambda: datetime.now(timezone.utc), *, sleep=time.sleep,
                 monotonic=time.monotonic):
        self.api, self.config, self.now = api, config, now
        self.sleep, self.monotonic = sleep, monotonic
        self.retry_until = monotonic() + 40

    def backoff(self, error, retry, *, reserve=16):
        # leave room for one bounded SDK call; reconciliation can use the remaining invocation budget
        delay = 2 ** retry
        if retry >= 2 or not transient(error) or self.retry_until - self.monotonic() < delay + reserve:
            return False
        self.sleep(delay)
        return self.retry_until - self.monotonic() >= reserve

    def load(self):
        response = self.api["dynamodb"].get_item(TableName=self.config.table,
            Key={"pk": {"S": "lease"}}, ConsistentRead=True)
        item = response.get("Item")
        require(item is not None, "no operator-armed lease")
        state = json.loads(item["payload"]["S"])
        complete = state["phase"] == "complete"
        durable.validate(state["request"], exact=not complete)
        pins = state["request"]["pins"]
        if complete:
            require(all(pins[key] == getattr(self.config, key) for key in ("account", "region", "namespace", "bucket")),
                    "completed lease namespace changed")
        else:
            require(pins == self.config.pins(), "deployment pins changed")
        return state, int(item["revision"]["N"])

    def save(self, state, revision):
        data = json.dumps(state, sort_keys=True, separators=(",", ":"))
        # account for the same wrapper and encoding used by the bounded collector
        self.receipt_bytes(state)
        self.api["dynamodb"].update_item(TableName=self.config.table, Key={"pk": {"S": "lease"}},
            ConditionExpression="#revision = :previous", UpdateExpression="SET payload = :payload, #revision = :next",
            ExpressionAttributeNames={"#revision": "revision"}, ExpressionAttributeValues={
                ":previous": {"N": str(revision)}, ":next": {"N": str(revision + 1)}, ":payload": {"S": data}})
        return revision + 1

    def save_launch_intent(self, state, revision):
        """Persist the allocation fence; production additionally checks central admission."""
        return self.save(state, revision)

    def validate_inputs(self, state):
        request = state["request"]
        for key in BINARIES:
            head = self.api["s3"].head_object(Bucket=self.config.bucket, Key="input/" + key, ChecksumMode="ENABLED")
            expected = base64.b64encode(bytes.fromhex(request["sha256"][key])).decode("ascii")
            require(type(head.get("ContentLength")) is int and 0 < head["ContentLength"] <= MAX_BINARY and
                    head.get("ChecksumSHA256") == expected and head.get("ChecksumType", "FULL_OBJECT") == "FULL_OBJECT",
                    "uploaded binary digest or size differs from operator manifest")

    def read_worker_status(self, state):
        return get_json(self.api["s3"], self.config.bucket, STATUS_KEY)

    def watchdog_state(self, state, instances):
        """Read mode-specific policy before useful binding; physical observations still follow."""

    def fresh_guard(self):
        response = self.api["dynamodb"].get_item(TableName=self.config.table,
            Key={"pk": {"S": "guard"}}, ConsistentRead=True)
        item = response.get("Item")
        if item is None:
            raise AllocationDeferred("independent janitor has no successful scan")
        proof = json.loads(item["payload"]["S"])
        require(proof.get("namespace") == self.config.namespace and proof.get("healthy") is True,
                "independent janitor scan is invalid")
        age = (self.now() - utc(proof["observed_at"])).total_seconds()
        require(age >= 0, "independent janitor scan is in the future")
        if age > durable.GUARD_GAP_SECONDS:
            raise AllocationDeferred("independent janitor scan is stale")
        return proof["observed_at"]

    def inventory(self, request=None):
        values, token = [], None
        for _ in range(10):
            args = {"Filters": [{"Name": "tag:" + durable.TAG_NAMESPACE, "Values": [self.config.namespace]}],
                    "MaxResults": 1000}
            if token:
                args["NextToken"] = token
            response = self.api["ec2"].describe_instances(**args)
            for reservation in response["Reservations"]:
                for instance in reservation["Instances"]:
                    raw = instance.get("Tags", [])
                    # an unexpected response must never give termination authority over another namespace
                    if not any(t.get("Key") == durable.TAG_NAMESPACE and t.get("Value") == self.config.namespace
                               for t in raw if isinstance(t, dict)):
                        continue
                    if request is None or instance["State"]["Name"] in LIVE or any(
                            t.get("Key") == durable.TAG_RUN and t.get("Value") == request["run"] for t in raw):
                        values.append(instance)
            token = response.get("NextToken")
            if not token:
                return values
        raise ValueError("instance inventory pagination exceeds bound; janitor scan incomplete")

    def terminate(self, identities):
        errors = []
        for identity in sorted(set(identities)):
            try:
                require(re.fullmatch(r"i-[0-9a-f]{17}", identity), "invalid managed instance identity")
                self.api["ec2"].terminate_instances(InstanceIds=[identity])
            except Exception as error:
                errors.append(error)
        if errors:
            raise errors[0]

    def independent_scan(self):
        started = self.now()
        instances = self.inventory()
        live = [i for i in instances if i["State"]["Name"] in LIVE]
        terminate = []
        for instance in live:
            try:
                expiry = durable.tagged_expiry(instance, self.config.namespace, self.now())
                invalid = self.now() >= expiry or instance.get("InstanceType") != INSTANCE_TYPE
            except (ValueError, KeyError, TypeError, OverflowError):
                invalid = True
            if invalid or len(live) > 1:
                terminate.append(instance["InstanceId"])
        self.terminate(terminate)
        # only a complete scan with all required terminations accepted can refresh admission
        self.api["dynamodb"].put_item(TableName=self.config.table, Item={"pk": {"S": "guard"},
            "payload": {"S": json.dumps(dict(namespace=self.config.namespace, observed_at=timestamp(started), healthy=True))}})
        return instances

    def verify_startup(self, data):
        # legacy startup remains operator-verified; production overrides this hook
        pass

    def launch_parameters(self, request, allocation):
        data = verify_launch_data(self.config, launch_template_data(
            self.api["ec2"], self.config.lt_id, self.config.lt_version))
        self.verify_startup(data)
        interface = dict(data["NetworkInterfaces"][0], SubnetId=self.config.subnet)
        tags = [{"Key": k, "Value": v} for k, v in sorted(durable.tags(request, allocation).items())]
        return dict(LaunchTemplate={"LaunchTemplateId": self.config.lt_id, "Version": self.config.lt_version},
            MinCount=1, MaxCount=1, ClientToken=allocation["token"], NetworkInterfaces=[interface],
            IamInstanceProfile={"Arn": self.config.profile_arn},
            TagSpecifications=[{"ResourceType": kind, "Tags": copy.deepcopy(tags)} for kind in ("instance", "volume")])

    def schedule(self, *, binding):
        return dict(Name="runtime-" + binding["attempt_id"], GroupName=self.config.runtime_group,
            ScheduleExpression="at(" + utc(binding["close_at"]).strftime("%Y-%m-%dT%H:%M:%S") + ")",
            ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
            ActionAfterCompletion="NONE", Target={"Arn": "arn:aws:scheduler:::aws-sdk:ec2:terminateInstances",
                "RoleArn": self.config.termination_role, "Input": json.dumps({"InstanceIds": [binding["instance_id"]]}, sort_keys=True),
                "RetryPolicy": {"MaximumEventAgeInSeconds": 300, "MaximumRetryAttempts": 3}})

    def verify_schedule(self, expected):
        actual = self.api["scheduler"].get_schedule(Name=expected["Name"], GroupName=expected["GroupName"])
        require(all(actual.get(k) == v for k, v in expected.items() if k != "Target"), "independent cutoff metadata differs")
        a, e = actual["Target"], expected["Target"]
        require(set(a) == set(e) and all(a[k] == e[k] for k in e if k != "Input") and
                json.loads(a["Input"]) == json.loads(e["Input"]), "independent cutoff target differs")
        return actual

    def runtime_schedule(self, binding):
        expected = self.schedule(binding=binding)
        try:
            return self.verify_schedule(expected)
        except Exception as error:
            if error_code(error) != "ResourceNotFoundException":
                raise
        token = hashlib.sha256((binding["attempt_id"] + binding["close_at"]).encode()).hexdigest()
        try:
            self.api["scheduler"].create_schedule(**expected, ClientToken=token)
        except Exception as primary:
            try:
                return self.verify_schedule(expected)
            except Exception as observed:
                if error_code(observed) == "ResourceNotFoundException" or retryable_io(observed):
                    raise primary from observed
                raise
        return self.verify_schedule(expected)

    def reconcile_launch(self, run, allocation, mutate, *, not_sent=False):
        # all outcomes reconcile the same evidence and revision; only this loop retries state writes
        for retry in range(3):
            try:
                state, revision = self.load()
                require(state["request"]["run"] == run, "run changed during launch reconciliation")
                if not_sent and state["attempt"] != allocation:
                    require(state["attempts"] < allocation["ordinal"] and not state["allocation_consumed"],
                            "another allocation replaced an unsent intent")
                    return
                require(state["attempt"] == allocation, "allocation changed during launch reconciliation")
                original = copy.deepcopy(state)
                mutate(state)
                if state != original:
                    self.save(state, revision)
                return
            except Exception as error:
                if not self.backoff(error, retry):
                    raise

    def expire_capacity(self, state):
        if (not state["binding"] and state["launch_outcome"] != "accepted" and
                self.now() >= utc(state["request"]["capacity_deadline"])):
            state.update(phase="closing", close_reason=state["close_reason"] or "capacity wait expired")
            return True
        return False

    def capacity_open(self, state, revision):
        if not self.expire_capacity(state):
            return True
        self.save(state, revision)
        return False

    def finish_launch(self, run, allocation, *, response=None, failure=None, not_sent=False):
        require(sum((response is not None, failure is not None, not_sent is True)) == 1,
                "exactly one launch outcome is required")
        def mutate(state):
            if response is not None:
                instances = response.get("Instances", [])
                require(len(instances) == 1 and re.fullmatch(r"i-[0-9a-f]{17}", instances[0].get("InstanceId", "")),
                        "launch response did not bind exactly one instance")
                identity = instances[0]["InstanceId"]
                require(not state["known_instance_ids"] or state["known_instance_ids"] == [identity],
                        "accepted response conflicts with observed allocation")
                state["known_instance_ids"] = [identity]
                durable.launch_outcome(state, "accepted")
                if "LaunchTime" in instances[0] and "State" in instances[0]:
                    try:
                        self.remember_instance(state, instances[0])
                    except (ValueError, KeyError, TypeError):
                        state["close_reason"] = state["close_reason"] or "invalid accepted launch observation"
                state["phase"] = "requested" if not state["close_reason"] else "closing"
            elif state["launch_outcome"] == "accepted":
                # positive evidence is monotonic, even after an uncertain response or write
                return
            elif (not_sent or capacity_rejection(failure)) and state.get("launch_replayed") is True:
                # this sender's negative result cannot disprove a later same-token replay.
                return
            elif not_sent or capacity_rejection(failure):
                require(not state["known_instance_ids"] and not state["seen"] and not state["binding"],
                        "no-allocation evidence conflicts with observed allocation")
                if not not_sent:
                    proof = dict(attempt=allocation["ordinal"], at=allocation["started_at"],
                                 request_id=failure.response["ResponseMetadata"]["RequestId"])
                    previous = [p for p in state["capacity_failures"] if p["attempt"] == allocation["ordinal"]]
                    require(not previous or previous == [proof], "capacity rejection evidence changed")
                    if not previous:
                        state["capacity_failures"].append(proof)
                durable.launch_outcome(state, "not-sent" if not_sent else "capacity-rejected")
                self.expire_capacity(state)
                state.update(phase="closing" if state["close_reason"] else "armed",
                             next_attempt_at=timestamp(self.now() + timedelta(seconds=durable.RETRY_SECONDS)))
            elif state["launch_outcome"] not in ("capacity-rejected", "not-sent"):
                durable.launch_outcome(state, "uncertain")
                state["phase"] = "closing" if state["close_reason"] else "uncertain"
        self.reconcile_launch(run, allocation, mutate, not_sent=not_sent)

    def launch_allowed(self, request, observed, allocation=None):
        return (self.now() < utc(request["capacity_deadline"]) and
                0 <= (self.now() - utc(observed)).total_seconds() <= durable.GUARD_GAP_SECONDS and
                (allocation is None or (utc(allocation["close_at"]) - self.now()).total_seconds() >=
                 durable.minimum_runtime(request["plan"])))

    def launch_recovery_open(self, state):
        # the legacy controller never replays persisted unknown outcomes
        return False

    def observe_closed_worker(self, state):
        """Allow production to preserve output commitments independently of useful runtime."""

    def launch(self, request, allocation, observed, *, prior_uncertain=False):
        uncertain = TimeoutError("previous launch outcome is unresolved") if prior_uncertain else None
        for retry in range(3):
            try:
                # every retry uses the identical persisted parameters, token, region, subnet and cutoff
                response = self.api["ec2"].run_instances(**allocation["parameters"])
            except Exception as error:
                if capacity_rejection(error) and uncertain is None:
                    self.finish_launch(request["run"], allocation, failure=error)
                    return
                uncertain = uncertain or error
                if (not retryable_io(error) or not self.launch_allowed(request, observed, allocation) or
                        not self.backoff(error, retry, reserve=24) or not self.launch_allowed(request, observed, allocation)):
                    # no later negative response can erase an earlier unknown allocation
                    self.finish_launch(request["run"], allocation, failure=uncertain)
                    return
            else:
                self.finish_launch(request["run"], allocation, response=response)
                return

    def coordinator(self, event):
        require(set(event) == {"run", "execution_arn", "execution_name"}, "unexpected submission fields")
        state, revision = self.load()
        request = state["request"]
        require(event["run"] == request["run"] and event["execution_name"] == request["run"] and
                event["execution_arn"].endswith(":" + request["run"]), "execution does not match armed attempt")
        require(state["execution_arn"] in (None, event["execution_arn"]), "another execution owns this lease")
        if state["phase"] == "complete":
            return dict(state="complete", run=request["run"])
        if state["phase"] != "armed":
            # a duplicate or interrupted invocation never replays or advances a possibly allocated request
            return dict(state="wait", run=request["run"])
        require(not state["allocation_consumed"], "possible allocation cannot be retried")
        if self.now() < utc(state["next_attempt_at"]):
            return dict(state="wait", run=request["run"])
        if not self.capacity_open(state, revision):
            return dict(state="wait", run=request["run"])
        self.fresh_guard()
        require(not self.inventory(request), "previous or unexpected managed instance exists")
        self.validate_inputs(state)
        if not self.capacity_open(state, revision):
            return dict(state="wait", run=request["run"])
        allocation = durable.attempt(request, state["attempts"] + 1, self.now())
        allocation["parameters"] = self.launch_parameters(request, allocation)
        fresh, current_revision = self.load()
        if current_revision != revision:
            return dict(state="wait", run=request["run"])
        require(fresh["phase"] == "armed" and fresh["request"] == request and
                fresh["attempts"] == state["attempts"] and not fresh["allocation_consumed"] and
                not fresh["close_reason"], "lease changed during artifact validation")
        state = fresh
        observed = self.fresh_guard()
        if not self.capacity_open(state, revision):
            return dict(state="wait", run=request["run"])
        state.update(phase="launching", execution_arn=event["execution_arn"], attempt=allocation,
                     attempts=allocation["ordinal"])
        durable.launch_outcome(state, "uncertain")
        try:
            self.save_launch_intent(state, revision)
        except Exception as error:
            if retryable_io(error):
                # RunInstances has not been called, including when this intent write committed invisibly
                self.finish_launch(request["run"], allocation, not_sent=True)
            raise
        # a delayed intent write cannot refresh the saved deadline or bypass admission
        if not self.launch_allowed(request, observed):
            self.finish_launch(request["run"], allocation, not_sent=True)
            return dict(state="wait", run=request["run"])
        self.launch(request, allocation, observed)
        return dict(state="wait", run=request["run"])

    def remember_instance(self, state, instance):
        identity, launch = instance["InstanceId"], timestamp(instance["LaunchTime"])
        roots = [v["Ebs"]["VolumeId"] for v in instance.get("BlockDeviceMappings", [])
                 if "Ebs" in v and v.get("DeviceName") == instance.get("RootDeviceName")]
        prior = state["seen"].get(identity)
        if prior and ((prior["launch_at"] is not None and prior["launch_at"] != launch) or
                      (prior["root_volumes"] and roots and prior["root_volumes"] != roots)):
            state["close_reason"] = state["close_reason"] or "instance restarted or root binding changed"
        state["seen"][identity] = dict(launch_at=(prior["launch_at"] or launch) if prior else launch,
            root_volumes=roots or (prior["root_volumes"] if prior else []),
            state="terminated" if prior and prior["state"] == "terminated" else instance["State"]["Name"])

    def observe(self, state, instances, *, allow_binding=True):
        request, now = state["request"], self.now()
        for instance in instances:
            self.remember_instance(state, instance)
        reason = state["close_reason"]
        state["known_instance_ids"] = sorted(set(state["known_instance_ids"]) | set(state["seen"]))
        if len(state["known_instance_ids"]) > 1:
            reason = reason or "multiple managed instances; no new allocation permitted"
        if instances and not state["allocation_consumed"]:
            reason = reason or "instance appeared without an allocated intent"
        if (len(instances) == 1 and len(state["known_instance_ids"]) == 1 and
                state["attempt"] is not None and state["allocation_consumed"]):
            try:
                require(utc(instances[0]["LaunchTime"]) <= now, "future launch timestamp")
                durable.validate_allocation_instance(request, instances[0], state["attempt"], reconciliation=True)
                # positive identity evidence survives closure and later EC2 record expiry;
                # it does not grant runtime, publish a mailbox or reopen a closing lease.
                durable.launch_outcome(state, "accepted")
                if state["phase"] in ("launching", "uncertain") and not reason:
                    state["phase"] = "requested"
                if instances[0]["State"]["Name"] not in ("pending", "running"):
                    reason = reason or "allocated instance stopped or terminated"
                elif not state["binding"] and not reason and allow_binding:
                    state["binding"] = durable.bind(request, instances[0], state["attempt"])
                    state["phase"] = "requested"
            except (ValueError, KeyError, TypeError) as error:
                reason = reason or str(error)
        if (state["launch_outcome"] != "accepted" and state["phase"] in ("launching", "uncertain") and
                state["attempt"] is not None and not self.launch_recovery_open(state) and (
                now - utc(state["attempt"]["started_at"])).total_seconds() >= durable.LAUNCH_ACK_SECONDS):
            reason = reason or "launch acknowledgement expired; allocation outcome uncertain"
        if state["attempt"] is not None and state["allocation_consumed"] and now >= utc(state["attempt"]["close_at"]):
            reason = reason or "immutable allocation runtime exhausted"
        if state["binding"]:
            bound = [i for i in instances if i["InstanceId"] == state["binding"]["instance_id"]]
            if bound and bound[0]["State"]["Name"] not in ("pending", "running"):
                reason = reason or "bound instance stopped or terminated"
        if reason:
            state.update(phase="closing", close_reason=reason)
        self.expire_capacity(state)

    def discover_roots(self, state):
        # tags can recover an identity missed in the instance mapping; empty results prove nothing
        if not state["attempt"] or len(state["known_instance_ids"]) != 1:
            return
        identity = state["known_instance_ids"][0]
        seen = state["seen"].get(identity)
        if seen is None:
            return
        if seen["root_volumes"]:
            return
        expected = durable.tags(state["request"], state["attempt"])
        roots, token = [], None
        for _ in range(10):
            parameters = dict(Filters=[{"Name": "tag:" + key, "Values": [value]}
                                      for key, value in sorted(expected.items())], MaxResults=500)
            if token:
                parameters["NextToken"] = token
            response = self.api["ec2"].describe_volumes(**parameters)
            for volume in response["Volumes"]:
                observed = durable.instance_tags(volume)
                require(all(observed.get(k) == v for k, v in expected.items()), "root volume tags differ")
                require(re.fullmatch(r"vol-[0-9a-f]{17}", volume["VolumeId"]), "invalid root volume identity")
                require(all(a.get("InstanceId") == identity and
                            a.get("Device") == "/dev/sda1" for a in volume.get("Attachments", [])),
                        "root volume attachment differs")
                roots.append(volume["VolumeId"])
            token = response.get("NextToken")
            if not token:
                require(len(roots) <= 1, "multiple tagged root volumes; manual reconciliation required")
                if roots:
                    seen["root_volumes"] = roots
                return
        raise ValueError("root inventory pagination exceeds bound")

    def admit_publication(self, state):
        request, binding = state["request"], state["binding"]
        intent = {key: binding[key] for key in ("attempt_id", "instance_id", "deadline")}
        if state.get("publication"):
            require(state["publication"] == intent, "worker publication binding changed")
        else:
            require((utc(binding["deadline"]) - self.now()).total_seconds() >= durable.worker_minimum(request["plan"]),
                    "remaining allocation time does not fit workload and explicit headroom")
            state["publication"] = intent

    def publish_worker(self, state):
        request, binding = state["request"], state["binding"]
        put_json(self.api["s3"], self.config.bucket, REQUEST_KEY, durable.worker_command(request, binding))
        put_json(self.api["s3"], self.config.bucket, WINDOW_KEY, durable.window(request, binding))

    def root_cleanup(self, state):
        require(set(state["known_instance_ids"]) == set(state["seen"]), "known instance is absent from physical proof")
        require(not state["allocation_consumed"] or bool(state["known_instance_ids"]),
                "uncertain allocation cannot be disproved by an empty Describe response")
        require(not state["allocation_consumed"] or state["launch_outcome"] == "accepted",
                "observed instance identity has not reconciled the uncertain allocation")
        deleted = []
        for seen in state["seen"].values():
            require(seen["state"] == "terminated", "physical worker is not terminated")
            require(len(seen["root_volumes"]) == 1, "root inventory is incomplete; manual reconciliation required")
            volume = seen["root_volumes"][0]
            try:
                self.api["ec2"].describe_volumes(VolumeIds=[volume])
            except Exception as error:
                if error_code(error) == "InvalidVolume.NotFound":
                    deleted.append(volume)
                    continue
                raise
            raise ValueError("worker root volume remains")
        return sorted(deleted)

    def receipt_bytes(self, state):
        observed = state["cleanup"]["verified_at"] if state["phase"] == "complete" else timestamp(self.now())
        data = json.dumps(dict(protocol=durable.PROTOCOL, observed_at=observed, **state),
                          sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        require(len(data) <= self.receipt_limit, "durable receipt exceeds collector bound; manual reconciliation required")
        return data

    def receipt(self, state):
        self.api["s3"].put_object(Bucket=self.config.bucket, Key=RECEIPT,
                                  Body=self.receipt_bytes(state), ContentType="application/json")

    def watchdog(self, *, instances=None):
        # expiration is enforced before DynamoDB access, even if the lease is missing or unreadable
        if instances is None:
            instances = self.independent_scan()
        try:
            state, revision = self.load()
        except ValueError as error:
            if str(error) == "no operator-armed lease":
                return dict(state="idle")
            raise
        relevant = [i for i in instances if i["State"]["Name"] in LIVE or
                    durable.instance_tags(i).get(durable.TAG_RUN) == state["request"]["run"]]
        live = [i["InstanceId"] for i in relevant if i["State"]["Name"] in LIVE]
        saved, errors, roots_attempted = copy.deepcopy(state), [], False
        def failed(error, *, close=True):
            errors.append(error)
            if close and state["phase"] != "complete" and not transient(error):
                state.update(phase="closing", close_reason=state["close_reason"] or "guard failure: " + type(error).__name__)
        def checkpoint():
            nonlocal saved, revision
            if state != saved:
                revision = self.save(state, revision)
                saved = copy.deepcopy(state)
        try:
            if state["phase"] != "complete":
                policy_ready = True
                try:
                    self.watchdog_state(state, relevant)
                except Exception as error:
                    policy_ready = False
                    failed(error)
                # policy availability gates useful runtime, never physical observations.
                self.observe(state, relevant, allow_binding=policy_ready)
                if policy_ready and not state["close_reason"]:
                    checkpoint()
                    roots_attempted = True
                    self.discover_roots(state)
                    if state["binding"] and state["phase"] == "requested":
                        if not state["runtime_verified"]:
                            self.runtime_schedule(state["binding"])
                            state["runtime_verified"] = True
                        if not state["worker_published"]:
                            self.admit_publication(state)
                            # durable intent precedes both PUTs; a lost final save replays only this binding
                            checkpoint()
                            self.publish_worker(state)
                            state["worker_published"] = True
                        status = self.read_worker_status(state)
                        if status.get("run") == state["request"]["run"] and status.get("state") == "completed":
                            state.update(phase="closing",
                                close_reason="worker completed; result validity is separate from physical cleanup")
        except Exception as error:
            failed(error)
        try:
            self.observe_closed_worker(state)
        except Exception as error:
            failed(error)
        # one tail attempts every independent obligation; a save failure cannot skip termination,
        # and a failed termination cannot hide the original contract failure or its state write
        if state["close_reason"] or state["phase"] == "complete":
            try:
                self.terminate(live)
            except Exception as error:
                failed(error)
            if state["phase"] != "complete":
                try:
                    if not roots_attempted:
                        self.discover_roots(state)
                    drain = durable_plan_contract(state["request"]["plan"])["delivery_drain_seconds"]
                    after = utc(state["request"]["outer_deadline"]) + timedelta(seconds=drain)
                    if self.now() >= after and not live:
                        deleted = self.root_cleanup(state)
                        state["cleanup"] = dict(verified_at=timestamp(self.now()), deleted_roots=deleted,
                            live_instances=[], allocation_consumed=state["allocation_consumed"],
                            unresolved_allocation=False, lease_released=False)
                        state["phase"] = "complete"
                except Exception as error:
                    failed(error)
        try:
            checkpoint()
        except Exception as error:
            failed(error, close=False)
        try:
            # only committed cleanup evidence is eligible for a terminal receipt
            self.receipt(saved)
        except Exception as error:
            failed(error, close=False)
        if errors:
            primary = next((error for error in errors if not transient(error)), errors[0])
            if not transient(primary):
                raise primary
            return dict(state="deferred", run=state["request"]["run"])
        return dict(state=state["phase"], run=state["request"]["run"])


def coordinator(event, context):
    config = Configuration.environment(runtime=True)
    service = Service(clients(config.region), config)
    if context is not None:
        service.retry_until = service.monotonic() + max(0, context.get_remaining_time_in_millis() / 1000 - 5)
    try:
        return service.coordinator(event)
    except Exception as error:
        if isinstance(error, AllocationDeferred) or transient(error):
            return dict(state="wait", run=event["run"])
        raise


def watchdog(event, context):
    config = Configuration.environment()
    service = Service(clients(config.region), config)
    try:
        return service.watchdog()
    except Exception as error:
        if transient(error):
            return dict(state="deferred")
        raise
