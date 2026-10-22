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

from . import durable
from .common import (BINARIES, MAX_BINARY, REQUEST_KEY, STATUS_KEY, WINDOW_KEY,
                     get_json, put_json, error_code, retryable_io, INSTANCE_TYPE, require, timestamp, utc)

REQUEST = "control/durable-request.json"
RECEIPT = "output/durable-receipt.json"
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
    def __init__(self, api, config, now=lambda: datetime.now(timezone.utc)):
        self.api, self.config, self.now = api, config, now

    def load(self):
        response = self.api["dynamodb"].get_item(TableName=self.config.table,
            Key={"pk": {"S": "lease"}}, ConsistentRead=True)
        item = response.get("Item")
        require(item is not None, "no operator-armed lease")
        state = json.loads(item["payload"]["S"])
        durable.validate(state["request"])
        require(state["request"]["pins"] == self.config.pins(), "deployment pins changed")
        return state, int(item["revision"]["N"])

    def save(self, state, revision):
        data = json.dumps(state, sort_keys=True, separators=(",", ":"))
        require(len(data.encode()) <= 100_000, "lease evidence exceeds bound; manual reconciliation required")
        self.api["dynamodb"].update_item(TableName=self.config.table, Key={"pk": {"S": "lease"}},
            ConditionExpression="#revision = :previous", UpdateExpression="SET payload = :payload, #revision = :next",
            ExpressionAttributeNames={"#revision": "revision"}, ExpressionAttributeValues={
                ":previous": {"N": str(revision)}, ":next": {"N": str(revision + 1)}, ":payload": {"S": data}})
        return revision + 1

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

    def launch_parameters(self, request, allocation):
        response = self.api["ec2"].describe_launch_template_versions(LaunchTemplateId=self.config.lt_id,
                                                                  Versions=[self.config.lt_version])
        versions = response["LaunchTemplateVersions"]
        require(len(versions) == 1 and str(versions[0]["VersionNumber"]) == self.config.lt_version,
                "numeric launch version not found")
        data = verify_launch_data(self.config, versions[0]["LaunchTemplateData"])
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
        except Exception:
            return self.verify_schedule(expected)
        return self.verify_schedule(expected)

    def finish_launch(self, run, allocation, *, response=None, failure=None, not_sent=False):
        # retries reconcile the same evidence after a conditional conflict or an unknown write result
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
                    # a concurrent positive observation or already committed response must never regress
                    return
                elif not_sent or failure is not None and capacity_rejection(failure):
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
                    if self.now() >= utc(state["request"]["capacity_deadline"]):
                        state["close_reason"] = state["close_reason"] or "capacity wait expired"
                    state.update(phase="closing" if state["close_reason"] else "armed",
                                 next_attempt_at=timestamp(self.now() + timedelta(seconds=durable.RETRY_SECONDS)))
                elif state["launch_outcome"] not in ("capacity-rejected", "not-sent"):
                    durable.launch_outcome(state, "uncertain")
                    state["phase"] = "closing" if state["close_reason"] else "uncertain"
                if state != original:
                    self.save(state, revision)
                return
            except Exception as error:
                if retry == 2 or not (retryable_io(error) or error_code(error) == "ConditionalCheckFailedException"):
                    raise

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
        if self.now() >= utc(request["capacity_deadline"]):
            state.update(phase="closing", close_reason=state["close_reason"] or "capacity wait expired")
            self.save(state, revision)
            return dict(state="wait", run=request["run"])
        self.fresh_guard()
        require(not self.inventory(request), "previous or unexpected managed instance exists")
        for key in BINARIES:
            head = self.api["s3"].head_object(Bucket=self.config.bucket, Key="input/" + key, ChecksumMode="ENABLED")
            expected = base64.b64encode(bytes.fromhex(request["sha256"][key])).decode("ascii")
            require(type(head.get("ContentLength")) is int and 0 < head["ContentLength"] <= MAX_BINARY and
                    head.get("ChecksumSHA256") == expected and head.get("ChecksumType", "FULL_OBJECT") == "FULL_OBJECT",
                    "uploaded binary digest or size differs from operator manifest")
        if self.now() >= utc(request["capacity_deadline"]):
            state.update(phase="closing", close_reason="capacity wait expired during artifact validation")
            self.save(state, revision)
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
        if self.now() >= utc(request["capacity_deadline"]):
            state.update(phase="closing", close_reason="capacity wait expired during validation")
            self.save(state, revision)
            return dict(state="wait", run=request["run"])
        state.update(phase="launching", execution_arn=event["execution_arn"], attempt=allocation,
                     attempts=allocation["ordinal"])
        durable.launch_outcome(state, "uncertain")
        try:
            self.save(state, revision)
        except Exception as error:
            if retryable_io(error):
                # RunInstances has not been called, including when this intent write committed invisibly
                self.finish_launch(request["run"], allocation, not_sent=True)
            raise
        # a delayed intent write cannot refresh the saved deadline or bypass admission
        if not (self.now() < utc(request["capacity_deadline"]) and
                0 <= (self.now() - utc(observed)).total_seconds() <= durable.GUARD_GAP_SECONDS):
            self.finish_launch(request["run"], allocation, not_sent=True)
            return dict(state="wait", run=request["run"])
        try:
            response = self.api["ec2"].run_instances(**allocation["parameters"])
        except Exception as error:
            self.finish_launch(request["run"], allocation, failure=error)
            return dict(state="wait", run=request["run"])
        self.finish_launch(request["run"], allocation, response=response)
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

    def observe(self, state, instances):
        request, now = state["request"], self.now()
        for instance in instances:
            self.remember_instance(state, instance)
        reason = state["close_reason"]
        state["known_instance_ids"] = sorted(set(state["known_instance_ids"]) | set(state["seen"]))
        if len(state["known_instance_ids"]) > 1:
            reason = reason or "multiple managed instances; no new allocation permitted"
        if instances and not state["allocation_consumed"]:
            reason = reason or "instance appeared without an allocated intent"
        if instances and state["attempt"] is not None and not state["binding"] and not reason:
            try:
                require(utc(instances[0]["LaunchTime"]) <= now, "future launch timestamp")
                binding = durable.bind(request, instances[0], state["attempt"])
                state["binding"] = binding
                durable.launch_outcome(state, "accepted")
                state["phase"] = "requested"
            except (ValueError, KeyError, TypeError) as error:
                reason = str(error)
        if state["phase"] in ("launching", "uncertain") and state["attempt"] is not None and (
                now - utc(state["attempt"]["started_at"])).total_seconds() >= durable.LAUNCH_ACK_SECONDS:
            reason = reason or "launch acknowledgement expired; allocation outcome uncertain"
        if state["binding"]:
            if now >= utc(state["binding"]["close_at"]):
                reason = reason or "immutable allocation runtime exhausted"
            bound = [i for i in instances if i["InstanceId"] == state["binding"]["instance_id"]]
            if bound and bound[0]["State"]["Name"] not in ("pending", "running"):
                reason = reason or "bound instance stopped or terminated"
        elif now >= utc(request["capacity_deadline"]):
            reason = reason or "capacity wait expired"
        if reason:
            state.update(phase="closing", close_reason=reason)

    def discover_roots(self, state):
        # tags can recover an identity missed in the instance mapping; empty results prove nothing
        binding = state["binding"]
        if not binding or len(state["known_instance_ids"]) != 1:
            return
        seen = state["seen"][binding["instance_id"]]
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
                require(all(a.get("InstanceId") == binding["instance_id"] and
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

    def publish_worker(self, state):
        request, binding = state["request"], state["binding"]
        require((utc(binding["deadline"]) - self.now()).total_seconds() >= durable.worker_minimum(request["plan"]),
                "remaining allocation time does not fit workload and explicit headroom")
        put_json(self.api["s3"], self.config.bucket, REQUEST_KEY, durable.worker_command(request, binding))
        put_json(self.api["s3"], self.config.bucket, WINDOW_KEY, durable.window(request, binding))

    def root_cleanup(self, state):
        require(set(state["known_instance_ids"]) == set(state["seen"]), "known instance is absent from physical proof")
        require(not state["allocation_consumed"] or bool(state["known_instance_ids"]),
                "uncertain allocation cannot be disproved by an empty Describe response")
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

    def receipt(self, state):
        put_json(self.api["s3"], self.config.bucket, RECEIPT,
                 dict(protocol=durable.PROTOCOL, observed_at=(state["cleanup"]["verified_at"] if state["phase"] == "complete"
                      else timestamp(self.now())), **state))

    def watchdog(self):
        # expiration is enforced before DynamoDB access, even if the lease is missing or unreadable
        instances = self.independent_scan()
        try:
            state, revision = self.load()
        except ValueError as error:
            if str(error) == "no operator-armed lease":
                return dict(state="idle")
            raise
        relevant = [i for i in instances if i["State"]["Name"] in LIVE or
                    durable.instance_tags(i).get(durable.TAG_RUN) == state["request"]["run"]]
        if state["phase"] == "complete":
            self.terminate([i["InstanceId"] for i in relevant if i["State"]["Name"] in LIVE])
            # a lost final receipt write is repairable without changing completed evidence or its time
            self.receipt(state)
            return dict(state="complete", run=state["request"]["run"])
        original = copy.deepcopy(state)
        self.observe(state, relevant)
        if state != original:
            revision = self.save(state, revision)
        saved = copy.deepcopy(state)
        try:
            self.discover_roots(state)
            if state["binding"] and state["phase"] == "requested" and not state["close_reason"]:
                if not state["runtime_verified"]:
                    self.runtime_schedule(state["binding"])
                    state["runtime_verified"] = True
                if not state["worker_published"]:
                    self.publish_worker(state)
                    state["worker_published"] = True
                status = get_json(self.api["s3"], self.config.bucket, STATUS_KEY)
                if status.get("run") == state["request"]["run"] and status.get("state") == "completed":
                    state.update(phase="closing", close_reason="worker completed; result validity is separate from physical cleanup")
            if state["close_reason"]:
                self.terminate([i["InstanceId"] for i in relevant if i["State"]["Name"] in LIVE])
                after = utc(state["request"]["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
                if self.now() >= after and not any(i["State"]["Name"] in LIVE for i in relevant):
                    deleted = self.root_cleanup(state)
                    state["cleanup"] = dict(verified_at=timestamp(self.now()), deleted_roots=deleted,
                        live_instances=[], allocation_consumed=state["allocation_consumed"],
                        unresolved_allocation=False, lease_released=False)
                    state["phase"] = "complete"
        except Exception as error:
            if error_code(error) == "ConditionalCheckFailedException":
                raise
            if retryable_io(error):
                if state != saved:
                    self.save(state, revision)
                self.receipt(state)
                return dict(state="deferred", run=state["request"]["run"])
            state.update(phase="closing", close_reason=state["close_reason"] or "guard failure: " + type(error).__name__)
            if state != saved:
                self.save(state, revision)
            self.terminate([i["InstanceId"] for i in relevant if i["State"]["Name"] in LIVE])
            self.receipt(state)
            raise
        if state != saved:
            self.save(state, revision)
        self.receipt(state)
        return dict(state=state["phase"], run=state["request"]["run"])


def coordinator(event, context):
    config = Configuration.environment(runtime=True)
    service = Service(clients(config.region), config)
    try:
        return service.coordinator(event)
    except Exception as error:
        if isinstance(error, AllocationDeferred) or retryable_io(error) or error_code(error) == "ConditionalCheckFailedException":
            return dict(state="wait", run=event["run"])
        raise


def watchdog(event, context):
    config = Configuration.environment()
    service = Service(clients(config.region), config)
    try:
        return service.watchdog()
    except Exception as error:
        if retryable_io(error) or error_code(error) == "ConditionalCheckFailedException":
            return dict(state="deferred")
        raise
