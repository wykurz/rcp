"""Version-bound production results and fenced retirement, with injected readers.

Collection never launches, terminates, changes guards or refunds a reservation.
The active pointer is released only after the fixed outer deadline plus delivery
drain, fresh physical reconciliation, a terminal original/current GitHub producer
and Standard execution (or separately labeled never-submitted/never-started proof),
and read-back of bounded retained evidence. An expired
EC2 terminated record may age out to exact InvalidInstanceID.NotFound or a complete
HTTP 200 empty response. These require service-only committed watchdog termination/root
evidence; an empty response alone is never absence proof. These are control-plane observations, not a claim
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
from .production_objects import Objects, committed_status
from .production_errors import transient
import re

SCHEMA = "rcp-production-collection-v1"
CHECKPOINT_SCHEMA = "rcp-production-collection-checkpoint-v1"
CHECKPOINT_RESERVE = 32 * 1024
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


def expected_worker_files(payloads):
    """An archive is required only when terminal status advertises one."""
    expected = {"status.json"}
    if "status.json" in payloads:
        try:
            status = results.json_value(payloads["status.json"])
        except (ValueError, UnicodeError):
            status = {}
        if isinstance(status, dict) and status.get("sha256") is not None:
            expected.add("results.zip")
    return expected


def held_diagnostic(error):
    """Expose bounded categories and service codes, never SDK response text."""
    detail = dict(category=type(error).__name__, reason=results.diagnostic(error)[:256],
                  retryable=transient(error))
    code = error_code(error)
    if isinstance(code, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9.]{0,79}", code):
        detail["service_error"] = code
    return detail


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
    report = results.json_value(payloads["report.json"], MAX_REPORT)
    if "collection_checkpoint" in report:
        checkpoint_binding = {key: manifest[key] for key in (*binding, "execution", "producer_conclusion")}
        require(report["collection_checkpoint"] == dict(schema=CHECKPOINT_SCHEMA, binding=checkpoint_binding,
            sources=manifest["sources"], derived={name: Collector.fingerprint(payloads[name])
                for name in ("controller.json", "summary.html")}), "retained collection checkpoint differs")
        collected_at = report["public_projection"]["collected_at"]
        require(timestamp(utc(collected_at)) == collected_at and
            utc(collected_at) >= utc(record.state["cleanup"]["verified_at"]), "retained collection time differs")
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
    commitment = record.state.get("worker_output")
    if commitment is not None:
        require(paths is not None, "committed status has no bound worker")
        raw, reference, status = committed_status(commitment, paths.status)
        require(payloads.get("status.json") == raw and sources.get("status.json") == reference,
                "retained status differs from committed bytes")
        if "results.zip" in payloads and status.get("sha256") is not None:
            require(digest(payloads["results.zip"]) == status["sha256"],
                    "retained archive differs from committed status digest")
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
                state.get("launch_outcome") in {"none", "not-sent", "capacity-rejected", "rejected"}, "execution binding differs")
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
                    state.get("launch_outcome") in {"none", "not-sent", "capacity-rejected", "rejected"}, "no-allocation proof conflicts")
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
            if response.get("Reservations") == []:
                metadata = response.get("ResponseMetadata", {})
                require(isinstance(metadata, dict) and type(metadata.get("HTTPStatusCode")) is int and
                        metadata["HTTPStatusCode"] == 200 and
                        isinstance(metadata.get("RequestId"), str) and bool(metadata["RequestId"].strip()) and
                        response.get("NextToken") is None and
                        state.get("launch_outcome") == "accepted" and state.get("allocation_consumed") is True and
                        state.get("known_instance_ids") == [instance_id] and seen.get("state") == "terminated",
                        "empty known instance response lacks committed termination proof")
                allocation = state["attempt"]
                expected = durable.attempt(request, state["attempts"], utc(allocation["started_at"]))
                launch = utc(seen["launch_at"])
                require(all(allocation.get(key) == value for key, value in expected.items() if key != "parameters") and
                        isinstance(seen["launch_at"], str) and timestamp(launch) == seen["launch_at"] and
                        utc(allocation["started_at"]) <= launch <= utc(state["cleanup"]["verified_at"]) and
                        (binding is None or binding["instance_id"] == instance_id and binding["launch_at"] == seen["launch_at"]),
                        "aged-out instance lacks exact accepted allocation identity")
                continue
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
        if state.get("binding"):
            paths = MailboxPaths.for_instance(state["request"]["pins"]["account"],
                state["request"]["pins"]["region"], state["binding"]["instance_id"])
            if committed is not None:
                raw, ref, status = committed_status(committed, paths.status)
                payloads["status.json"], sources["status.json"] = raw, ref
            else:
                value = self.objects.read(paths.status, MAX_DOCUMENT, optional=True)
                if value:
                    payloads["status.json"], sources["status.json"] = value
            value = self.objects.read(paths.results, MAX_RESULT, optional=True)
            if value:
                if committed is not None and status.get("sha256") is not None:
                    require(value[1]["sha256"] == status["sha256"], "archive differs from committed status digest")
                payloads["results.zip"], sources["results.zip"] = value
        missing = expected_worker_files(payloads) - set(payloads)
        availability = None
        if state.get("binding") and missing:
            started = utc(state["binding"]["attempt_started_at"])
            recovery = utc(state["binding"]["deadline"]) + timedelta(seconds=EVIDENCE_RECOVERY_SECONDS)
            possibly_expired = (state.get("publication") is not None and
                                 self.now() >= started + timedelta(seconds=MAILBOX_EXPIRY_SECONDS))
            if "results.zip" in missing or possibly_expired:
                require(self.now() >= recovery, "worker evidence is missing; bounded recovery grace has not elapsed")
                availability = dict(outcome="evidence-unavailable", policy=EVIDENCE_RECOVERY_POLICY, missing=sorted(missing),
                    recovery_deadline=timestamp(recovery),
                    reason="confirmed missing output after recovery grace; workload success or failure is unknown")
        return payloads, sources, availability

    def analyze(self, record, expected, execution, conclusion, payloads, availability=None, *, collected_at=None):
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
                                                outcome=outcome, execution_status=execution["status"],
                                                collected_at=collected_at or timestamp(self.now())))
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
        if len(raw_report) > MAX_REPORT - CHECKPOINT_RESERVE:
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
        outcome = report["qualification_outcome"]
        require(outcome in {"passed", "failed", "evidence-unavailable"} and
                (outcome == "passed") == manifest["benchmark_valid"], "retained qualification outcome differs")
        availability = report.get("evidence_availability")
        require((outcome == "evidence-unavailable") == (availability is not None),
                "retained evidence availability differs")
        if availability is not None:
            require(isinstance(availability, dict) and set(availability) == {
                "outcome", "policy", "missing", "recovery_deadline", "reason"} and
                availability["policy"] in EVIDENCE_RECOVERY_POLICIES and availability["outcome"] == outcome and state.get("binding") is not None and
                availability["missing"] == sorted(expected_worker_files(payloads) - set(payloads)) and
                availability["missing"] and availability["recovery_deadline"] == timestamp(
                    utc(state["binding"]["deadline"]) + timedelta(seconds=EVIDENCE_RECOVERY_POLICIES[availability["policy"]])),
                    "retained evidence loss proof differs")
        public = projection.bind(report["public_projection"], "0" * 64)
        require(public["run_key"] == record.run_key and public["benchmark_valid"] == manifest["benchmark_valid"] and
                public["identity"] == {key: expected[key] for key in
                    ("run", "run_url", "suite", "baseline", "candidate", "controller")} and
                public["comparison"] == projection.comparison_label(expected, controller) and
                projection.qualification_outcome(public) == outcome and
                (not manifest["benchmark_valid"] or public["verification"] == evidence.get("expected_verification")),
                "retained public projection provenance differs")
        return outcome


    @staticmethod
    def fingerprint(raw):
        return dict(sha256=digest(raw), bytes=len(raw))

    def checkpoint_sources(self, record, sources, limits):
        """Recover the frozen input snapshot, preferring already retained copies."""
        require(isinstance(sources, dict) and {"durable.json"} <= set(sources) <= {
            "durable.json", "status.json", "results.zip"}, "checkpoint source set differs")
        paths = None if record.state.get("binding") is None else MailboxPaths.for_instance(
            record.state["request"]["pins"]["account"], record.state["request"]["pins"]["region"],
            record.state["binding"]["instance_id"])
        keys = {"durable.json": "production/runs/" + record.run_key + "/durable-receipt.json"}
        if paths:
            keys.update({"status.json": paths.status, "results.zip": paths.results})
        payloads = {}
        for name, ref in sources.items():
            require(isinstance(ref, dict) and set(ref) == {"key", "version", "sha256", "bytes"} and
                ref["key"] == keys.get(name) and isinstance(ref["version"], str) and
                ref["version"] not in ("", "null") and len(ref["version"]) <= 1024 and
                all(32 < ord(char) < 127 for char in ref["version"]) and
                isinstance(ref["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", ref["sha256"]) and
                type(ref["bytes"]) is int and 0 < ref["bytes"] <= limits[name], "checkpoint source reference differs")
            retained = self.objects.read(self.prefix + name, limits[name], optional=True)
            if retained is not None:
                raw = retained[0]
            elif name == "status.json" and record.state.get("worker_output") is not None:
                raw, actual, _ = committed_status(record.state["worker_output"], paths.status)
                require(actual == ref, "checkpoint committed source differs")
            else:
                # never substitute a new source version or add a dependency on mailbox
                # version reads when the frozen current version is no longer available
                raw, actual = self.objects.read(ref["key"], limits[name])
                require(actual == ref, "checkpoint source version changed")
            require(self.fingerprint(raw) == {key: ref[key] for key in ("sha256", "bytes")},
                    "checkpoint source bytes changed")
            payloads[name] = raw
        return payloads

    def prepared(self, record, binding, limits):
        """Commit one input/output identity before writing the rest of the immutable set."""
        saved = self.objects.read(self.prefix + "report.json", MAX_REPORT, optional=True)
        expected, execution, conclusion = (binding[key] for key in ("identity", "execution", "producer_conclusion"))
        if saved is None:
            # no new transaction may adopt or omit an older partial set with no root
            for name in limits.keys() - {"report.json"}:
                require(self.objects.read(self.prefix + name, limits[name], optional=True) is None,
                        "partial collection has no checkpoint; explicit recovery is required")
            payloads, sources, availability = self.raw(record)
            collected_at = timestamp(self.now())
        else:
            report = results.json_value(saved[0], MAX_REPORT)
            checkpoint = report.get("collection_checkpoint")
            require(isinstance(checkpoint, dict) and set(checkpoint) == {
                "schema", "binding", "sources", "derived"} and checkpoint["schema"] == CHECKPOINT_SCHEMA and
                checkpoint["binding"] == binding, "collection checkpoint belongs to different evidence or is incompatible")
            collected_at = report["public_projection"]["collected_at"]
            require(timestamp(utc(collected_at)) == collected_at and
                utc(record.state["cleanup"]["verified_at"]) <= utc(collected_at) <= self.now(),
                "checkpoint collection time differs")
            sources, availability = checkpoint["sources"], report.get("evidence_availability")
            payloads = self.checkpoint_sources(record, sources, limits)
            for name in {"durable.json", "status.json", "results.zip"} - set(sources):
                require(self.objects.read(self.prefix + name, limits[name], optional=True) is None,
                        "checkpoint has unexpected retained source")
        valid, derived = self.analyze(record, expected, execution, conclusion, payloads, availability,
                                      collected_at=collected_at)
        if saved is None:
            report = results.json_value(derived["report.json"], MAX_REPORT)
            report["collection_checkpoint"] = dict(schema=CHECKPOINT_SCHEMA, binding=binding, sources=sources,
                derived={name: self.fingerprint(derived[name]) for name in ("controller.json", "summary.html")})
            derived["report.json"] = encoded(report)
            require(len(derived["report.json"]) <= MAX_REPORT, "collection checkpoint exceeds report bound")
        else:
            require(valid == report["benchmark_valid"], "checkpoint qualification verdict differs")
            current_report = results.json_value(derived["report.json"], MAX_REPORT)
            require(report["public_projection"] == current_report["public_projection"],
                    "checkpoint public measurements or qualification identity differ")
            require(isinstance(checkpoint["derived"], dict) and set(checkpoint["derived"]) == {
                "controller.json", "summary.html"}, "checkpoint derived set differs")
            for name in checkpoint["derived"]:
                prior = self.objects.read(self.prefix + name, limits[name], optional=True)
                if prior is not None:
                    derived[name] = prior[0]
                require(self.fingerprint(derived[name]) == checkpoint["derived"][name],
                        "checkpoint derived evidence changed or is unavailable")
            # saved presentation is historical; cosmetic renderer changes cannot rewrite it
            # or block completion when all committed presentation bytes already exist
            derived["report.json"] = saved[0]
        payloads.update(derived)
        # validate before the first write; placeholder versions are used only by this pure
        # byte/provenance check and are replaced by exact S3 versions in the final manifest
        planned = {name: dict(key=self.prefix + name, version="pending", **self.fingerprint(raw))
                   for name, raw in payloads.items()}
        validate_retained(record, dict(binding, sources=sources, retained=planned, benchmark_valid=valid), payloads)
        self.objects.once(self.prefix + "report.json", derived["report.json"], MAX_REPORT)
        return payloads, sources, valid

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
            outcome = results.json_value(payloads["report.json"], MAX_REPORT)["qualification_outcome"]
            if not record.closed:
                # an open run still needs today's qualification verdict before retirement.
                # Presentation bytes are historical and are never regenerated for equality.
                saved_report = results.json_value(payloads["report.json"], MAX_REPORT)
                valid, _ = self.analyze(record, expected, execution, conclusion, payloads, saved_report.get("evidence_availability"))
                require(valid == manifest["benchmark_valid"], "retained qualification verdict differs")
            return manifest, existing[1], outcome
        payloads, sources, valid = self.prepared(record, binding, limits)
        retained = {name: self.objects.once(self.prefix + name, raw, limits[name]) for name, raw in payloads.items()}
        manifest = dict(binding, sources=sources, retained=retained, benchmark_valid=valid)
        validate_retained(record, manifest, payloads)
        outcome = results.json_value(payloads["report.json"], MAX_REPORT)["qualification_outcome"]
        return manifest, self.objects.once(key, encoded(manifest), MAX_DOCUMENT), outcome

    @staticmethod
    def outcome(manifest, ref, outcome, *, retired):
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
                manifest, verified, outcome = self.retained(record, identity(record), saved["execution"], saved["producer_conclusion"])
                require(verified == ref, "closed retention version differs")
                return self.outcome(manifest, ref, outcome, retired=True)
            expected, execution, conclusion = self.proof(record)
            manifest, ref, outcome = self.retained(record, expected, execution, conclusion)
            # repeat physical/execution observations after retention; the final store CAS is the atomic
            # fence. Reuse this invocation's terminal producer observation: a later rerun cannot
            # resume uploads or submit the already complete, deduplicated run.
            require(self.runtime_proof(record) == (expected, execution), "runtime proof changed during retention")
            current = self.store.read()
            require(current == record, "run changed during collection")
            outcome = self.outcome(manifest, ref, outcome, retired=retire)
            if retire:
                closed = self.store.retire(ref["sha256"], record.revision)
                require(closed.closed and closed.retirement_sha256 == ref["sha256"], "retirement outcome is uncertain")
            return outcome
        except Exception as error:
            # avoid exposing SDK response bodies, URLs or credentials in user-visible diagnostics
            return dict(state="held", retired=known_retired, benchmark_valid=False, **held_diagnostic(error))
