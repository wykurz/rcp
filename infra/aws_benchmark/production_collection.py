"""Version-bound production results and fenced retirement, with injected readers.

Collection never launches, terminates, changes guards or refunds a reservation.
The active pointer is released only after the fixed outer deadline plus delivery
drain, fresh physical reconciliation, a terminal original/current GitHub producer
and Standard execution (or separately labeled never-submitted/never-started proof),
and read-back of bounded retained evidence. An expired
EC2 terminated record is accepted only for exact InvalidInstanceID.NotFound with
service-only committed watchdog termination/root evidence; an empty successful
response is not absence proof. These are control-plane observations, not a claim
that simultaneous control-plane failures cannot conceal resources.

The GitHub reader is trusted integration code, not request data. Its read(producer)
returns attempt/current run documents, complete attempt_jobs/current_jobs responses
and pending_deployments. It must use fixed public GitHub endpoints, bounded reads
and an outer execution timeout; failures hold ownership. The deployed reader uses
its separately approved, narrowly scoped GitHub App installation credential.
"""

from datetime import datetime, timedelta, timezone
from html import escape
import json
from pathlib import Path
import tempfile

from . import durable, durable_client, production_projection as projection, results
from .common import MAX_RESULT, digest, durable_plan_contract, error_code, require, timestamp, utc
from .production_store import MAX_STATE_BYTES as MAX_RECEIPT
from .production_mailbox import MailboxPaths
from .production_objects import Objects

SCHEMA = "rcp-production-collection-v1"
MAX_DOCUMENT = 1024 * 1024
MAX_REPORT = 2 * 1024 * 1024
# mailbox lifecycle eligibility starts after seven days; nine days also allow
# UTC lifecycle rounding plus the one-day noncurrent version retention. this is
# a recovery grace period, not a guarantee that S3 has deleted any object.
MAILBOX_EXPIRY_SECONDS = 7 * 24 * 3600
EVIDENCE_RECOVERY_POLICIES = {"mailbox-loss-v1": 9 * 24 * 3600}
EVIDENCE_RECOVERY_POLICY = "mailbox-loss-v1"
EVIDENCE_RECOVERY_SECONDS = EVIDENCE_RECOVERY_POLICIES[EVIDENCE_RECOVERY_POLICY]
TERMINAL = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"})
CONCLUSIONS = results.SUBMISSION_CONCLUSIONS


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def identity(record):
    payload = record.admission["initial_payload"]
    producer, request = payload["producer"], record.state["request"]
    require(request == payload["state"]["request"], "admitted request changed")
    require(request["run"] == producer["run_id"] + "-" + producer["run_attempt"] and
            request["controller"] == producer["sha"] == producer["workflow_sha"],
            "admitted producer differs from request")
    return {**{key: request[key] for key in ("run", "baseline", "candidate", "suite", "controller")},
            "ref": producer["ref"], "repository": producer["repository"],
            "workflow": producer["workflow_ref"], "workflow_sha": producer["workflow_sha"],
            "event": producer["event_name"],
            "run_url": f"https://github.com/{producer['repository']}/actions/runs/{producer['run_id']}/attempts/{producer['run_attempt']}"}


def verify_producer(documents, producer, now):
    require(isinstance(documents, dict) and set(documents) == {
        "attempt", "current", "attempt_jobs", "current_jobs", "pending_deployments"}, "producer evidence is incomplete")
    require(len(encoded(documents)) <= MAX_DOCUMENT, "producer evidence exceeds bound")
    original = int(producer["run_attempt"])
    for key in ("attempt", "current"):
        run = documents[key]
        require(isinstance(run, dict), "invalid producer run")
        repo = run.get("repository", {})
        require(type(run.get("id")) is int and str(run["id"]) == producer["run_id"] and
                type(run.get("run_attempt")) is int and
                (run["run_attempt"] == original if key == "attempt" else run["run_attempt"] >= original) and
                type(repo.get("id")) is int and str(repo["id"]) == producer["repository_id"] and
                repo.get("full_name") == producer["repository"] and
                str(repo.get("owner", {}).get("id")) == producer["repository_owner_id"] and
                run.get("head_sha") == producer["sha"] and
                "refs/heads/" + str(run.get("head_branch")) == producer["ref"] and
                producer["workflow_ref"] == producer["repository"] + "/" + str(run.get("path")) + "@" + producer["ref"] and
                run.get("event") == producer["event_name"] and run.get("status") == "completed" and
                run.get("conclusion") in CONCLUSIONS, "producer is not the exact terminal run")
        if key == "attempt":
            require(str(run.get("actor", {}).get("id")) == producer["actor_id"], "original producer actor differs")
        jobs = documents[key + "_jobs" if key == "attempt" else "current_jobs"]
        empty_cancelled_rerun = (key == "current" and run["run_attempt"] > original and
                                 run["conclusion"] in {"cancelled", "startup_failure"})
        require(isinstance(jobs, dict) and set(jobs) == {"total_count", "jobs"} and
                type(jobs["total_count"]) is int and (0 if empty_cancelled_rerun else 1) <= jobs["total_count"] <= 100 and
                isinstance(jobs["jobs"], list) and len(jobs["jobs"]) == jobs["total_count"],
                "producer jobs inventory is incomplete")
        seen = set()
        for job in jobs["jobs"]:
            require(isinstance(job, dict) and type(job.get("id")) is int and job["id"] > 0 and job["id"] not in seen and
                    job.get("run_id") == run["id"] and job.get("run_attempt") == run["run_attempt"] and
                    job.get("head_sha") == producer["sha"] and job.get("status") == "completed" and
                    job.get("conclusion") in CONCLUSIONS and utc(job["completed_at"]) <= now,
                    "producer job is not terminal and bound")
            seen.add(job["id"])
    require(documents["pending_deployments"] == [], "producer has pending deployment writes")
    return documents["attempt"]["conclusion"]


def validate_retained(record, manifest, payloads):
    """Pure validation of retained bytes; no live reads, rendering or workload parsing."""
    binding = dict(schema=SCHEMA, run_key=record.run_key, epoch=record.epoch,
        state_sha256=digest(encoded(record.state)), admission_sha256=digest(encoded(record.admission)),
        identity=identity(record))
    require(isinstance(manifest, dict) and set(manifest) == set(binding) | {
        "execution", "producer_conclusion", "sources", "retained", "benchmark_valid"} and
        all(manifest.get(k) == v for k, v in binding.items()) and
        type(manifest["benchmark_valid"]) is bool and manifest["producer_conclusion"] in CONCLUSIONS and
        isinstance(manifest["execution"], dict) and
        manifest["execution"].get("status") in TERMINAL | {"NEVER_SUBMITTED", "NEVER_STARTED"},
        "retained manifest belongs to different evidence")
    prefix = "production/runs/" + record.run_key + "/collection/"
    limits = {"durable.json": MAX_RECEIPT, "status.json": MAX_DOCUMENT, "results.zip": MAX_RESULT,
              "controller.json": MAX_DOCUMENT, "report.json": MAX_REPORT, "summary.html": MAX_REPORT}
    refs = manifest["retained"]
    require(isinstance(refs, dict) and {"durable.json", "controller.json", "report.json", "summary.html"}
            <= set(refs) <= set(limits) and isinstance(payloads, dict) and set(payloads) == set(refs),
            "retained file set differs")
    for name, ref in refs.items():
        raw = payloads[name]
        require(isinstance(ref, dict) and set(ref) == {"key", "version", "sha256", "bytes"} and
                ref["key"] == prefix + name and isinstance(ref["version"], str) and
                0 < len(ref["version"]) <= 1024 and ref["version"] != "null" and
                all(32 < ord(char) < 127 for char in ref["version"]) and
                isinstance(raw, bytes) and 0 < len(raw) <= limits[name] and type(ref["bytes"]) is int and
                ref["bytes"] == len(raw) and ref["sha256"] == digest(raw), "retained reference differs")
    Collector.retained_provenance(record, manifest, payloads)
    sources = manifest["sources"]
    require(isinstance(sources, dict) and set(sources) == set(refs) - {
        "controller.json", "report.json", "summary.html"}, "source references differ")
    paths = None if record.state.get("binding") is None else MailboxPaths.for_instance(
        record.state["request"]["pins"]["account"], record.state["request"]["pins"]["region"],
        record.state["binding"]["instance_id"])
    keys = {"durable.json": "production/runs/" + record.run_key + "/durable-receipt.json"}
    if paths:
        keys.update({"status.json": paths.status, "results.zip": paths.results})
    for name, ref in sources.items():
        require(isinstance(ref, dict) and set(ref) == {"key", "version", "sha256", "bytes"} and
                ref["key"] == keys[name] and isinstance(ref["version"], str) and
                ref["version"] not in ("", "null") and len(ref["version"]) <= 1024 and
                all(32 < ord(char) < 127 for char in ref["version"]) and ref["sha256"] == digest(payloads[name]) and
                type(ref["bytes"]) is int and ref["bytes"] == len(payloads[name]), "source binding differs")
        field = {"status.json": "status", "results.zip": "results"}.get(name)
        committed = (record.state.get("worker_output") or {}).get(field) if field else None
        if committed is not None:
            require(ref == committed, "retained source differs from committed worker version")
    return binding["identity"]


class Collector:
    """Trusted service integration: collect(), then optionally retire through a fenced transaction.

    No caller-supplied proof or result is accepted. Clients and producer_reader are
    constructed by the server. The two writes are immutable evidence objects and
    RunStore.retire; all launch/guard operations are outside this component.
    """
    def __init__(self, service, machine, producer_reader, *, temp_root=None,
                 now=lambda: datetime.now(timezone.utc)):
        self.service, self.store, self.machine = service, service.store, machine
        self.reader, self.temp_root, self.now = producer_reader, temp_root, now
        self.objects = Objects(service.api["s3"], service.config.bucket)
        self.prefix = "production/runs/" + service.run_key + "/collection/"

    @staticmethod
    def unstarted(state, *, sealed):
        absent = dict(execution_arn=None, binding=None, attempt=None, attempts=0,
            allocation_consumed=False, launch_outcome="none", known_instance_ids=[], seen={},
            runtime_verified=False, worker_published=False)
        require(all(key in state and type(state[key]) is type(value) and state[key] == value
                    for key, value in absent.items()) and state.get("worker_output") is None,
                "unstarted allocation state is ambiguous")
        if sealed:
            require(isinstance(state.get("sealed_inputs"), dict) and
                    set(state["sealed_inputs"]) == set(state["request"]["sha256"]),
                    "never-started inputs are ambiguous")
        else:
            require("sealed_inputs" in state and "submission_arn" in state and
                    state["sealed_inputs"] is None and state["submission_arn"] is None,
                    "never-submitted state is ambiguous")

    def execution(self, record):
        state, request = record.state, record.state["request"]
        durable_client.verify_machine(request, self.machine)
        expected = durable_client.execution_arn(self.machine, request["run"])
        api = self.service.api["stepfunctions"]
        machine = api.describe_state_machine(stateMachineArn=self.machine)
        require(machine.get("stateMachineArn") == self.machine and machine.get("type") == "STANDARD",
                "collection requires the exact Standard state machine")
        if state.get("submission_arn") is None:
            self.unstarted(state, sealed=False)
            try:
                api.describe_execution(executionArn=expected)
            except Exception as error:
                require(error_code(error) == "ExecutionDoesNotExist", "execution absence is unproven")
            else:
                raise ValueError("an execution exists for the never-submitted run")
            require(self.service.inventory() == [], "never-submitted namespace is not empty")
            return dict(proof="never-submitted", status="NEVER_SUBMITTED", executionArn=expected,
                        stateMachineArn=self.machine, name=request["run"], input=None)
        require(state["submission_arn"] == expected, "sealed execution differs")
        try:
            execution = api.describe_execution(executionArn=expected)
        except Exception as error:
            if error_code(error) != "ExecutionDoesNotExist":
                raise
            self.unstarted(state, sealed=True)
            require(record.terminal and state["phase"] == "complete" and
                    self.now() >= utc(request["outer_deadline"]) + timedelta(
                        seconds=durable_plan_contract(request["plan"])["delivery_drain_seconds"]),
                    "never-started delivery horizon has not closed")
            require(self.service.inventory() == [], "never-started namespace is not empty")
            return dict(proof="sealed-never-started", status="NEVER_STARTED", executionArn=expected,
                        stateMachineArn=self.machine, name=request["run"], input=None)
        require(execution.get("executionArn") == expected and execution.get("stateMachineArn") == self.machine and
                execution.get("name") == request["run"] and results.json_value(execution["input"].encode()) == {
                    "run_key": record.run_key, "epoch": record.epoch, "run": request["run"]}, "execution input differs")
        require(execution.get("status") in TERMINAL, "execution is not terminal")
        require(state.get("execution_arn") == expected or
                state.get("execution_arn") is None and state.get("allocation_consumed") is False and
                state.get("launch_outcome") in {"none", "not-sent", "capacity-rejected"}, "execution binding differs")
        return dict(proof="terminal-execution", **{key: execution[key] for key in
            ("executionArn", "stateMachineArn", "name", "status", "input")})

    def physical(self, state):
        request, binding = state["request"], state.get("binding")
        observation = results.json_value(self.service.receipt_bytes(state), MAX_RECEIPT)
        require(durable_client.verify_cleanup(observation, request, binding), "committed cleanup is incomplete")
        require(utc(state["cleanup"]["verified_at"]) <= self.now(), "cleanup is in the future")
        require(state.get("launch_outcome") != "uncertain", "allocation remains uncertain")
        if state["allocation_consumed"]:
            require(state.get("launch_outcome") == "accepted", "consumed allocation is not reconciled")
        else:
            require(not state["known_instance_ids"] and state.get("binding") is None and
                    state.get("launch_outcome") in {"none", "not-sent", "capacity-rejected"}, "no-allocation proof conflicts")
        require(all(item["State"]["Name"] == "terminated" for item in self.service.inventory()),
                "namespace has live or unclassified instances")
        ec2 = self.service.api["ec2"]
        for instance_id, seen in state["seen"].items():
            try:
                response = ec2.describe_instances(InstanceIds=[instance_id])
            except Exception as error:
                # accepted only with exact, service-only committed seen/cleanup evidence above
                if error_code(error) == "InvalidInstanceID.NotFound":
                    continue
                raise
            found = [item for group in response["Reservations"] for item in group["Instances"]]
            require(len(found) == 1 and not response.get("NextToken"), "known instance read is incomplete")
            item = found[0]
            require(item.get("InstanceId") == instance_id and item.get("State", {}).get("Name") == "terminated" and
                    timestamp(item["LaunchTime"]) == seen["launch_at"], "known instance is not the committed termination")
            if binding and instance_id == binding["instance_id"]:
                require(item.get("ImageId") == request["pins"]["ami"] and item.get("InstanceType") == "c5d.24xlarge" and
                        item.get("ClientToken") == state["attempt"]["token"] and
                        all(durable.instance_tags(item).get(key) == val for key, val in durable.tags(request, state["attempt"]).items()),
                        "known worker immutable identity differs")
        require(self.service.root_cleanup(state) == sorted(state["cleanup"]["deleted_roots"]), "known root deletion differs")
        token, tokens = None, set()
        for _ in range(10):
            args = dict(Filters=[{"Name": "tag:" + durable.TAG_NAMESPACE, "Values": [self.service.config.namespace]}], MaxResults=500)
            if token:
                args["NextToken"] = token
            page = ec2.describe_volumes(**args)
            require(isinstance(page.get("Volumes"), list) and not page["Volumes"], "namespace has an orphan root or volume")
            token = page.get("NextToken")
            if not token:
                break
            require(isinstance(token, str) and token not in tokens, "volume pagination repeats")
            tokens.add(token)
        else:
            raise ValueError("volume inventory pagination exceeds bound")
        if state.get("runtime_verified"):
            require(binding is not None, "runtime schedule has no instance binding")
            self.service.verify_schedule(self.service.schedule(binding=binding))

    def runtime_proof(self, record):
        self.service.validate_state(record.state)
        require(record.terminal and record.state["phase"] == "complete", "cleanup is still pending")
        expected = identity(record)
        execution = self.execution(record)
        self.physical(record.state)
        return expected, execution

    def proof(self, record):
        expected, execution = self.runtime_proof(record)
        producer = record.admission["initial_payload"]["producer"]
        documents = self.reader.read(producer)
        conclusion = verify_producer(documents, producer, self.now())
        return expected, execution, conclusion

    def raw(self, record):
        state, payloads, sources = record.state, {}, {}
        receipt_key = "production/runs/" + record.run_key + "/durable-receipt.json"
        data, source = self.objects.read(receipt_key, MAX_RECEIPT)
        require(results.json_value(data, MAX_RECEIPT) == results.json_value(self.service.receipt_bytes(state), MAX_RECEIPT),
                "published cleanup differs from committed state")
        payloads["durable.json"], sources["durable.json"] = data, source
        committed = state.get("worker_output")
        if committed is not None:
            require(isinstance(committed, dict) and set(committed) == {"status", "results"} and
                    isinstance(committed["status"], dict) and
                    (committed["results"] is None or isinstance(committed["results"], dict)),
                    "worker output commitment differs")
        missing_committed = set()
        if state.get("binding"):
            paths = MailboxPaths.for_instance(state["request"]["pins"]["account"],
                state["request"]["pins"]["region"], state["binding"]["instance_id"])
            for name, field, key, limit in (("status.json", "status", paths.status, MAX_DOCUMENT),
                                           ("results.zip", "results", paths.results, MAX_RESULT)):
                ref = committed[field] if committed else None
                if ref is not None:
                    # a commitment names a specific version, even behind a delete marker.
                    # only exact service absence is recoverable; denied reads and checksum
                    # errors remain held, regardless of the age of the run.
                    require(set(ref) == {"key", "version", "sha256", "bytes"} and ref["key"] == key,
                            "committed worker source differs")
                    try:
                        raw = self.objects.referenced(ref, key, limit)
                    except Exception as error:
                        if error_code(error) not in {"NoSuchKey", "NoSuchVersion"}:
                            raise
                        missing_committed.add(name)
                        continue
                    value = raw, ref
                else:
                    value = self.objects.read(key, limit, optional=True)
                if value:
                    payloads[name], sources[name] = value
        try:
            status = results.json_value(payloads.get("status.json", b"{}"))
        except (ValueError, UnicodeError):
            status = {}
        missing_archive = isinstance(status, dict) and status.get("sha256") is not None and "results.zip" not in payloads
        missing = {name for name in ("status.json", "results.zip") if name not in payloads}
        availability = None
        if state.get("binding") and missing:
            started = utc(state["binding"]["attempt_started_at"])
            recovery = utc(state["binding"]["deadline"]) + timedelta(seconds=EVIDENCE_RECOVERY_SECONDS)
            possibly_expired = (state.get("worker_published") is True and
                                self.now() >= started + timedelta(seconds=MAILBOX_EXPIRY_SECONDS))
            if missing_committed or missing_archive or possibly_expired:
                require(self.now() >= recovery, "worker evidence is missing; bounded recovery grace has not elapsed")
                availability = dict(outcome="evidence-unavailable", policy=EVIDENCE_RECOVERY_POLICY, missing=sorted(missing),
                    recovery_deadline=timestamp(recovery),
                    reason="confirmed missing output after recovery grace; workload success or failure is unknown")
        return payloads, sources, availability

    def analyze(self, record, expected, execution, conclusion, payloads, availability=None):
        state, request, binding = record.state, record.state["request"], record.state.get("binding")
        try:
            status = results.json_value(payloads.get("status.json", b"{}"))
        except (ValueError, UnicodeError):
            status = {}
        worker_ok = isinstance(status, dict) and status.get("state") == "completed" and status.get("success") is True
        controller = dict(schema_version=1, identity=expected, cleanup="verified",
            outcome="success" if worker_ok else "failure",
            instance=None if binding is None else dict(instance_id=binding["instance_id"], image_id=request["pins"]["ami"], instance_type="c5d.24xlarge"),
            window=None if binding is None else durable.window(request, binding, exact=False),
            request=None if binding is None else durable.worker_command(request, binding, exact=False),
            status_sha256=digest(payloads["status.json"]) if "status.json" in payloads else None,
            archive_sha256=digest(payloads["results.zip"]) if "results.zip" in payloads else None)
        with tempfile.TemporaryDirectory(prefix="production-collection-", dir=self.temp_root) as temporary:
            root = Path(temporary)
            for name in ("status.json", "results.zip"):
                if name in payloads:
                    (root / name).write_bytes(payloads[name])
            (root / "controller.json").write_bytes(encoded(controller))
            evidence, result, _ = results.inspect(root, expected, conclusion)
        valid = False
        try:
            results.validate_qualification(evidence, result, dict(schema_version=1, identity=expected,
                archive_sha256=controller["archive_sha256"], ami_id=request["pins"]["ami"], harness_revision=request["pins"]["source"]), producer_role="submission")
            valid = availability is None
        except (ValueError, KeyError, TypeError) as error:
            evidence["errors"].append("qualification: " + results.diagnostic(error))
        outcome = "evidence-unavailable" if availability else "passed" if valid else "failed"
        report = dict(schema=SCHEMA, identity=expected, benchmark_valid=valid, cleanup_verified=True,
            qualification_outcome=outcome, evidence_availability=availability,
            validation="metadata/size only; does not prove content integrity", execution_proof=execution, evidence=evidence,
            presentation=dict(report="full", summary="full"),
            public_projection=projection.freeze(record.run_key, expected, request["pins"], valid, evidence, result,
                                                outcome=outcome, execution_status=execution["status"]))
        try:
            if availability:
                summary = ("<!doctype html><meta charset=utf-8><title>Benchmark evidence unavailable</title>"
                    "<h1>Benchmark " + escape(expected["run"]) + "</h1><p>Evidence unavailable after bounded recovery. "
                    "Workload success or failure is unknown; no performance measurements are published. "
                    "Physical cleanup is verified.</p>").encode()
            else:
                summary = results.summary(evidence, result).encode()
            require(len(summary) <= MAX_REPORT, "rendered summary exceeds bound")
        except (ValueError, KeyError, TypeError, UnicodeError):
            report["presentation"]["summary"] = "bounded-fallback"
            summary = ("<!doctype html><meta charset=utf-8><title>Benchmark collection</title>"
                "<h1>Benchmark " + escape(expected["run"]) + "</h1><p>Validation: " +
                ("passed" if valid else "failed") + ". Cleanup verified.</p>"
                "<p>The detailed display exceeded its bound or could not be rendered. "
                "The original raw evidence and its versioned manifest are retained.</p>"
                "<p>Metadata/size checks do not prove content integrity.</p>").encode()
        raw_report = encoded(report)
        if len(raw_report) > MAX_REPORT:
            # archive member names and diagnostic strings are attacker-controlled. Retain
            # their original bytes, but publish a fixed-size projection instead of turning
            # a valid physical cleanup into a permanent oversized-report hold.
            details = dict(schema_version=evidence["schema_version"], identity=expected,
                producer_conclusion=conclusion, files=evidence["files"], controller=controller,
                expected_verification=evidence["expected_verification"],
                errors=[str(item)[:256] for item in evidence["errors"][:16]],
                errors_total=len(evidence["errors"]), members_total=len(evidence["members"]),
                diagnostic_projection=True, unabridged_report_sha256=digest(raw_report),
                unabridged_report_bytes=len(raw_report))
            report = dict(report, evidence=details,
                          presentation=dict(report="bounded-diagnostic", summary=report["presentation"]["summary"]))
            raw_report = encoded(report)
        require(len(raw_report) <= MAX_REPORT and len(summary) <= MAX_REPORT, "bounded report projection exceeds limit")
        return valid, {"controller.json": encoded(controller), "report.json": raw_report, "summary.html": summary}

    @staticmethod
    def retained_provenance(record, manifest, payloads):
        """Check historical byte bindings without rerunning a renderer or workload parser."""
        state, expected = record.state, manifest["identity"]
        observed = results.json_value(payloads["durable.json"], MAX_RECEIPT)
        require(set(observed) == set(state) | {"protocol", "observed_at"} and
                all(observed.get(key) == value for key, value in state.items()) and
                observed["protocol"] == state["request"]["protocol"] and
                observed["observed_at"] == state["cleanup"]["verified_at"], "retained cleanup differs")
        controller = results.json_value(payloads["controller.json"])
        require(controller.get("schema_version") == 1 and controller.get("identity") == expected and
                controller.get("cleanup") == "verified" and controller.get("outcome") in {"success", "failure"},
                "retained controller provenance differs")
        bound, request = state.get("binding"), state["request"]
        if bound is None:
            require(all(controller.get(key) is None for key in ("instance", "window", "request")),
                    "retained unallocated controller differs")
        else:
            require(controller.get("instance") == dict(instance_id=bound["instance_id"],
                image_id=request["pins"]["ami"], instance_type="c5d.24xlarge"), "retained instance differs")
            window = controller.get("window", {})
            require(all(window.get(key) == request[key] for key in
                ("protocol", "run", "baseline", "candidate", "suite", "runtime_seconds", "plan", "outer_deadline")) and
                all(window.get(key) == bound[key] for key in
                ("instance_id", "launch_at", "deadline", "attempt_id", "attempt_started_at")) and
                window.get("opens") == bound["attempt_started_at"] and
                window.get("request_expires") == request["capacity_deadline"], "retained worker window differs")
            command = controller.get("request", {})
            require(all(command.get(key) == request[key] for key in ("run", "baseline", "candidate", "suite", "sha256")) and
                    command.get("deadline") == bound["deadline"], "retained worker request differs")
        for field, name in (("status_sha256", "status.json"), ("archive_sha256", "results.zip")):
            require(controller.get(field) == (digest(payloads[name]) if name in payloads else None),
                    "retained controller digest differs")
        report = results.json_value(payloads["report.json"], MAX_REPORT)
        evidence = report.get("evidence", {})
        require(report.get("schema") == manifest["schema"] and report.get("identity") == expected and
                type(report.get("benchmark_valid")) is bool and report["benchmark_valid"] == manifest["benchmark_valid"] and
                report.get("cleanup_verified") is True and report.get("execution_proof") == manifest["execution"] and
                evidence.get("identity") == expected and evidence.get("producer_conclusion") == manifest["producer_conclusion"] and
                evidence.get("controller") == controller and evidence.get("files") == {
                    name: digest(payloads[name]) for name in ("controller.json", "status.json", "results.zip") if name in payloads},
                "retained report provenance differs")
        outcome = report.get("qualification_outcome", "passed" if manifest["benchmark_valid"] else "failed")
        require(outcome in {"passed", "failed", "evidence-unavailable"} and
                (outcome == "passed") == manifest["benchmark_valid"], "retained qualification outcome differs")
        availability = report.get("evidence_availability")
        require((outcome == "evidence-unavailable") == (availability is not None),
                "retained evidence availability differs")
        if availability is not None:
            require(isinstance(availability, dict) and set(availability) == {
                "outcome", "policy", "missing", "recovery_deadline", "reason"} and
                availability["policy"] in EVIDENCE_RECOVERY_POLICIES and availability["outcome"] == outcome and state.get("binding") is not None and
                availability["missing"] == sorted({"status.json", "results.zip"} - set(payloads)) and
                availability["missing"] and availability["recovery_deadline"] == timestamp(
                    utc(state["binding"]["deadline"]) + timedelta(seconds=EVIDENCE_RECOVERY_POLICIES[availability["policy"]])),
                    "retained evidence loss proof differs")
        if "public_projection" in report:
            public = projection.bind(report["public_projection"], "0" * 64)
            require(public["run_key"] == record.run_key and public["benchmark_valid"] == manifest["benchmark_valid"] and
                    public["identity"] == {key: expected[key] for key in
                        ("run", "run_url", "suite", "baseline", "candidate", "controller")} and
                    public["comparison"] == results.comparison_label(expected, controller) and
                    public.get("qualification_outcome", "passed" if public["benchmark_valid"] else "failed") == outcome and
                    (not manifest["benchmark_valid"] or public["verification"] == evidence.get("expected_verification")),
                    "retained public projection provenance differs")


    def retained(self, record, expected, execution, conclusion):
        key = self.prefix + "manifest.json"
        existing = self.objects.read(key, MAX_DOCUMENT, optional=True)
        binding = dict(schema=SCHEMA, run_key=record.run_key, epoch=record.epoch,
            state_sha256=digest(encoded(record.state)), admission_sha256=digest(encoded(record.admission)),
            identity=expected, execution=execution, producer_conclusion=conclusion)
        limits = {"durable.json": MAX_RECEIPT, "status.json": MAX_DOCUMENT, "results.zip": MAX_RESULT,
                  "controller.json": MAX_DOCUMENT, "report.json": MAX_REPORT, "summary.html": MAX_REPORT}
        if existing:
            manifest = results.json_value(existing[0])
            require(set(manifest) == set(binding) | {"sources", "retained", "benchmark_valid"} and
                    all(manifest.get(k) == v for k, v in binding.items()) and type(manifest["benchmark_valid"]) is bool,
                    "retained manifest belongs to different evidence")
            refs = manifest["retained"]
            require(isinstance(refs, dict) and {"durable.json", "controller.json", "report.json", "summary.html"} <= set(refs)
                    <= set(limits), "retained file set differs")
            payloads = {name: self.objects.referenced(ref, self.prefix + name, limits[name]) for name, ref in refs.items()}
            validate_retained(record, manifest, payloads)
            if not record.closed:
                # an open run still needs today's qualification verdict before retirement.
                # Presentation bytes are historical and are never regenerated for equality.
                saved_report = results.json_value(payloads["report.json"], MAX_REPORT)
                valid, _ = self.analyze(record, expected, execution, conclusion, payloads, saved_report.get("evidence_availability"))
                require(valid == manifest["benchmark_valid"], "retained qualification verdict differs")
            return manifest, existing[1]
        payloads, sources, availability = self.raw(record)
        valid, derived = self.analyze(record, expected, execution, conclusion, payloads, availability)
        payloads.update(derived)
        retained = {name: self.objects.once(self.prefix + name, raw, limits[name]) for name, raw in payloads.items()}
        manifest = dict(binding, sources=sources, retained=retained, benchmark_valid=valid)
        return manifest, self.objects.once(key, encoded(manifest), MAX_DOCUMENT)

    def outcome(self, manifest, ref, *, retired):
        report_ref = manifest["retained"]["report.json"]
        report = results.json_value(self.objects.referenced(report_ref, self.prefix + "report.json", MAX_REPORT), MAX_REPORT)
        outcome = report.get("qualification_outcome", "passed" if manifest["benchmark_valid"] else "failed")
        require(outcome in {"passed", "failed", "evidence-unavailable"} and
                (outcome == "passed") == manifest["benchmark_valid"], "retained qualification outcome differs")
        return dict(state="success" if outcome == "passed" else "unavailable" if outcome == "evidence-unavailable" else "failed",
                    retired=retired, benchmark_valid=manifest["benchmark_valid"],
                    qualification_outcome=outcome, retention=ref)

    def collect(self, *, retire=False):
        """Return pending/held/unavailable/failed/success; only complete proof closes ownership."""
        known_retired = False
        try:
            record = self.store.read()
            require(record is not None and record.run_key == self.service.run_key and record.epoch == self.service.epoch,
                    "run record is absent or replaced")
            known_retired = record.closed is True
            if not record.terminal:
                require(not record.closed, "closed record is not terminal")
                return dict(state="pending", retired=False, benchmark_valid=False, reason="cleanup has not committed")
            if record.closed:
                # replay reads the retained proof, not the next owner's live namespace. No
                # producer or runtime action is possible after this fenced tombstone exists.
                raw, ref = self.objects.read(self.prefix + "manifest.json", MAX_DOCUMENT)
                require(ref["sha256"] == record.retirement_sha256, "closed tombstone evidence differs")
                saved = results.json_value(raw)
                require(saved.get("producer_conclusion") in CONCLUSIONS and
                        saved.get("execution", {}).get("status") in TERMINAL | {"NEVER_SUBMITTED", "NEVER_STARTED"}, "closed proof is not terminal")
                manifest, verified = self.retained(record, identity(record), saved["execution"], saved["producer_conclusion"])
                require(verified == ref, "closed retention version differs")
                return self.outcome(manifest, ref, retired=True)
            expected, execution, conclusion = self.proof(record)
            manifest, ref = self.retained(record, expected, execution, conclusion)
            # repeat physical/execution observations after retention; the final store CAS is the atomic
            # fence. Reuse this invocation's terminal producer observation: a later rerun cannot
            # resume uploads or submit the already complete, deduplicated run.
            require(self.runtime_proof(record) == (expected, execution), "runtime proof changed during retention")
            current = self.store.read()
            require(current == record, "run changed during collection")
            outcome = self.outcome(manifest, ref, retired=retire)
            if retire:
                closed = self.store.retire(ref["sha256"], record.revision)
                require(closed.closed and closed.retirement_sha256 == ref["sha256"], "retirement outcome is uncertain")
            return outcome
        except Exception as error:
            # avoid exposing SDK response bodies, URLs or credentials in user-visible diagnostics
            return dict(state="held", retired=known_retired, benchmark_valid=False, reason=results.diagnostic(error))
