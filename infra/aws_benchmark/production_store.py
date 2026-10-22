"""Transactional ownership, reservations and retained production run tombstones.

Only trusted admission code constructs AdmissionDecision. This module does not accept
workflow quotes, evaluate credentials or prove cleanup. Its transactions bind an
already validated decision to the enabled policy and fence every state write against
the same active owner. Reservations and deduplication records never expire or refund.
"""

from dataclasses import dataclass
from datetime import date
import json
import re
import uuid

from benchmarks.strict_json import parse_json

from .common import require

MAX_INTEGER = 2 ** 63 - 1
MAX_STATE_BYTES = 128 * 1024
MAX_ADMISSION_BYTES = 48_000
MAX_ITEM_BYTES = 400 * 1024
PHASES = frozenset({"awaiting-inputs", "armed", "launching", "uncertain", "requested", "closing", "complete"})


class ReconciliationRequired(RuntimeError):
    """The exact operation must be reconciled before any further side effect."""


def _integer(value, *, minimum=0):
    require(type(value) is int and minimum <= value <= MAX_INTEGER, "invalid bounded integer")
    return value


def _text(value, pattern, description):
    require(isinstance(value, str) and re.fullmatch(pattern, value) is not None, description)
    return value


def _encoded(value, limit):
    result = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    require(len(result.encode()) <= limit, "production record exceeds its bound")
    return result


def _item_bytes(item):
    # include UTF-8 attribute names and use a conservative string-size bound for numbers
    size = 128
    for name, value in item.items():
        require(isinstance(name, str) and isinstance(value, dict) and len(value) == 1, "invalid stored attribute")
        size += len(name.encode())
        if set(value) in ({"S"}, {"N"}):
            text = next(iter(value.values()))
            require(isinstance(text, str), "invalid stored scalar")
            size += len(text.encode())
        else:
            require(set(value) == {"BOOL"} and type(value["BOOL"]) is bool, "invalid stored attribute type")
            size += 1
    require(size <= MAX_ITEM_BYTES, "complete DynamoDB item exceeds 400 KiB")
    return size


def _key(value):
    return {"pk": {"S": value}}


def _number(value):
    return {"N": str(value)}


def _string(value):
    return {"S": value}


def _state(value, request=None):
    require(isinstance(value, dict) and isinstance(value.get("request"), dict), "state has no immutable request")
    require(value.get("phase") in PHASES, "invalid production state phase")
    if request is not None:
        require(value["request"] == request, "state changed the immutable request")
    return _encoded(value, MAX_STATE_BYTES)


@dataclass(frozen=True)
class AdmissionDecision:
    run_key: str
    epoch: str
    fingerprint: str
    policy_version: str
    reserve_microusd: int
    total_cap_microusd: int
    monthly_cap_microusd: int
    daily_run_limit: int
    month: str
    day: str
    initial_payload: dict
    schedule_slot: str | None = None

    def __post_init__(self):
        _text(self.run_key, r"repo-[1-9][0-9]{0,19}-run-[1-9][0-9]{0,19}", "invalid production run key")
        for value in (self.epoch, self.fingerprint):
            _text(value, r"[0-9a-f]{64}", "invalid admission digest")
        _text(self.policy_version, r"[A-Za-z0-9_.-]{1,128}", "invalid policy version")
        for value in (self.reserve_microusd, self.total_cap_microusd, self.monthly_cap_microusd,
                      self.daily_run_limit):
            _integer(value, minimum=1)
        require(self.reserve_microusd <= min(self.total_cap_microusd, self.monthly_cap_microusd),
                "reservation exceeds configured limits")
        _text(self.day, r"[0-9]{4}-[0-9]{2}-[0-9]{2}", "invalid admission day")
        date.fromisoformat(self.day)
        require(self.month == self.day[:7], "admission month differs from day")
        require(isinstance(self.initial_payload, dict) and "state" in self.initial_payload,
                "initial payload has no state")
        _state(self.initial_payload["state"])
        require(self.initial_payload["state"]["phase"] != "complete", "cannot admit terminal state")
        if self.schedule_slot is not None:
            _text(self.schedule_slot, r"[A-Za-z0-9_.:/-]{1,128}", "invalid schedule slot")
        self.immutable_json()

    def immutable_json(self):
        # retain the initial request and producer, not only a caller-supplied digest
        return _encoded({"run_key": self.run_key, "epoch": self.epoch, "fingerprint": self.fingerprint,
            "policy_version": self.policy_version, "reserve_microusd": self.reserve_microusd,
            "total_cap_microusd": self.total_cap_microusd, "monthly_cap_microusd": self.monthly_cap_microusd,
            "daily_run_limit": self.daily_run_limit, "month": self.month, "day": self.day,
            "schedule_slot": self.schedule_slot, "initial_payload": self.initial_payload}, MAX_ADMISSION_BYTES)

    def accounting_keys(self):
        return ["budget#total", "budget#month#" + self.month, "rate#day#" + self.day] + (
            ["schedule#" + self.schedule_slot] if self.schedule_slot is not None else [])


@dataclass(frozen=True)
class RunRecord:
    run_key: str
    epoch: str
    fingerprint: str
    admission: dict
    state: dict
    revision: int
    closed: bool
    terminal: bool
    retirement_sha256: str | None


class RunStore:
    def __init__(self, dynamodb, table, run_key, epoch):
        _text(table, r"[A-Za-z0-9_.-]{3,255}", "invalid production table")
        _text(run_key, r"repo-[1-9][0-9]{0,19}-run-[1-9][0-9]{0,19}", "invalid production run key")
        _text(epoch, r"[0-9a-f]{64}", "invalid owner epoch")
        self.api, self.table, self.run_key, self.epoch = dynamodb, table, run_key, epoch
        self.pk = "run#" + run_key

    def _read_items(self, keys):
        response = self.api.transact_get_items(TransactItems=[
            {"Get": {"TableName": self.table, "Key": _key(key)}} for key in keys])
        values = response.get("Responses")
        require(isinstance(values, list) and len(values) == len(keys), "incomplete transactional read")
        result = []
        for value in values:
            require(value is None or isinstance(value, dict), "invalid transactional response")
            item = (value or {}).get("Item")
            require(item is None or isinstance(item, dict), "invalid transactional item")
            result.append(item or None)
        return result

    def _record(self, item):
        if item is None:
            return None
        _item_bytes(item)
        expected = {"pk", "epoch", "fingerprint", "admission", "state", "revision", "closed", "terminal"}
        require(set(item) in (expected, expected | {"retirement_sha256"}), "invalid run record fields")
        require(item["pk"] == _string(self.pk) and item["epoch"] == _string(self.epoch), "run owner differs")
        admission = parse_json(item["admission"]["S"])
        decision = AdmissionDecision(**admission)
        require(decision.run_key == self.run_key and decision.epoch == self.epoch and
                decision.fingerprint == item["fingerprint"]["S"], "run admission binding differs")
        require(item["admission"]["S"] == decision.immutable_json(), "admission encoding differs")
        state = parse_json(item["state"]["S"])
        _state(state, decision.initial_payload["state"]["request"])
        require(re.fullmatch(r"0|[1-9][0-9]*", item["revision"]["N"]) is not None, "invalid revision")
        revision = _integer(int(item["revision"]["N"]))
        closed, terminal = item["closed"].get("BOOL"), item["terminal"].get("BOOL")
        require(type(closed) is bool and type(terminal) is bool and terminal == (state["phase"] == "complete"),
                "terminal state differs")
        retirement = item.get("retirement_sha256", {}).get("S")
        require((retirement is not None) == closed and (not closed or terminal), "invalid retired record")
        if retirement is not None:
            _text(retirement, r"[0-9a-f]{64}", "invalid retirement proof digest")
        return RunRecord(self.run_key, self.epoch, decision.fingerprint, admission, state, revision,
                         closed, terminal, retirement)

    def _owner(self, item):
        if item is None:
            return False
        require(set(item) == {"pk", "run_key", "epoch"} and item["pk"] == _string("active"),
                "invalid active owner record")
        _text(item["run_key"]["S"], r"repo-[1-9][0-9]{0,19}-run-[1-9][0-9]{0,19}", "invalid active run key")
        _text(item["epoch"]["S"], r"[0-9a-f]{64}", "invalid active epoch")
        return item == dict(_key("active"), run_key=_string(self.run_key), epoch=_string(self.epoch))

    def read(self):
        """Read the retained record without requiring it still owns the active slot."""
        response = self.api.get_item(TableName=self.table, Key=_key(self.pk), ConsistentRead=True)
        return self._record(response.get("Item"))

    def load(self):
        """Read the state and owner in one serializable snapshot."""
        item, owner = self._read_items([self.pk, "active"])
        record = self._record(item)
        require(record is not None and not record.closed and self._owner(owner), "run does not own the active slot")
        return record.state, record.revision

    def _owner_condition(self):
        return {"TableName": self.table, "Key": _key("active"),
            "ConditionExpression": "#run = :run AND #epoch = :epoch",
            "ExpressionAttributeNames": {"#run": "run_key", "#epoch": "epoch"},
            "ExpressionAttributeValues": {":run": _string(self.run_key), ":epoch": _string(self.epoch)}}

    def _write(self, items):
        _encoded(items, 512_000)
        # a fresh call must reevaluate fences, not replay an earlier token's success
        # there is no write retry here; permanent records reconcile ambiguous responses
        self.api.transact_write_items(TransactItems=items, ClientRequestToken=str(uuid.uuid4()))

    def _policy_condition(self, version):
        return {"TableName": self.table, "Key": _key("policy"),
            "ConditionExpression": "#version = :version AND #enabled = :enabled",
            "ExpressionAttributeNames": {"#version": "version", "#enabled": "enabled"},
            "ExpressionAttributeValues": {":version": _string(version), ":enabled": {"BOOL": True}}}

    def _counter_update(self, key, field, increment, limit, *, existing=False):
        condition = "#counter BETWEEN :zero AND :remaining"
        if not existing:
            condition = "attribute_not_exists(pk) OR (" + condition + ")"
        return {"Update": {"TableName": self.table, "Key": _key(key),
            "UpdateExpression": "ADD #counter :increment",
            "ConditionExpression": condition,
            "ExpressionAttributeNames": {"#counter": field}, "ExpressionAttributeValues": {
                ":increment": _number(increment), ":zero": _number(0), ":remaining": _number(limit - increment)}}}

    def _admission_items(self, decision):
        immutable = decision.immutable_json()
        state = _state(decision.initial_payload["state"])
        record = dict(_key(self.pk), epoch=_string(self.epoch), fingerprint=_string(decision.fingerprint),
            admission=_string(immutable), state=_string(state), revision=_number(0),
            closed={"BOOL": False}, terminal={"BOOL": False})
        _item_bytes(record)
        owner = dict(_key("active"), run_key=_string(self.run_key), epoch=_string(self.epoch))
        result = [{"ConditionCheck": self._policy_condition(decision.policy_version)}]
        result += [{"Put": {"TableName": self.table, "Item": item,
                            "ConditionExpression": "attribute_not_exists(pk)"}} for item in (record, owner)]
        result += [self._counter_update("budget#total", "reserved", decision.reserve_microusd,
                                       decision.total_cap_microusd, existing=True),
                   self._counter_update("budget#month#" + decision.month, "reserved", decision.reserve_microusd,
                                       decision.monthly_cap_microusd),
                   self._counter_update("rate#day#" + decision.day, "runs", 1, decision.daily_run_limit)]
        if decision.schedule_slot is not None:
            result.append({"Put": {"TableName": self.table,
                "Item": dict(_key("schedule#" + decision.schedule_slot), run_key=_string(self.run_key),
                             epoch=_string(self.epoch)), "ConditionExpression": "attribute_not_exists(pk)"}})
        return result

    def _admitted(self, decision):
        values = self._read_items([self.pk, "active"] + decision.accounting_keys())
        record, owner = self._record(values[0]), values[1]
        if record is None:
            return None
        require(_encoded(record.admission, MAX_ADMISSION_BYTES) == decision.immutable_json(),
                "existing run differs from the complete admission decision")
        require((record.closed and not self._owner(owner)) or (not record.closed and self._owner(owner)),
                "existing run and active slot disagree")
        for item, field, minimum in zip(values[2:5], ("reserved", "reserved", "runs"),
                                        (decision.reserve_microusd, decision.reserve_microusd, 1)):
            require(isinstance(item, dict) and field in item and
                    re.fullmatch(r"0|[1-9][0-9]*", item[field].get("N", "")) is not None and
                    _integer(int(item[field]["N"])) >= minimum, "admitted accounting proof is missing")
        if decision.schedule_slot is not None:
            require(values[5] == dict(_key("schedule#" + decision.schedule_slot), run_key=_string(self.run_key),
                                     epoch=_string(self.epoch)), "schedule slot ownership differs")
        return record

    def admit(self, decision):
        """Reserve and own one durable run, or return its exact existing outcome."""
        require(isinstance(decision, AdmissionDecision) and decision.run_key == self.run_key and
                decision.epoch == self.epoch, "decision differs from store identity")
        # revalidate mutable dictionaries before constructing any transaction
        AdmissionDecision(**parse_json(decision.immutable_json()))
        try:
            existing = self._admitted(decision)
            if existing is not None:
                return existing
        except Exception as error:
            raise ReconciliationRequired("cannot reconcile the existing admission") from error
        try:
            self._write(self._admission_items(decision))
        except Exception as error:
            try:
                recorded = self._admitted(decision)
                if recorded is not None:
                    return recorded
            except Exception as observed:
                raise ReconciliationRequired("admission failed; recorded outcome is inconsistent or unavailable") from observed
            raise ReconciliationRequired("admission rejected or uncertain; no committed matching run observed") from error
        try:
            result = self._admitted(decision)
            require(result is not None, "successful transaction has no matching run")
            return result
        except Exception as error:
            raise ReconciliationRequired("admission committed; readback requires reconciliation") from error

    def _state_update(self, state, revision, *, retire=None):
        _integer(revision)
        require(revision < MAX_INTEGER, "revision is exhausted")
        condition = "#epoch = :epoch AND #revision = :previous AND #closed = :false AND #terminal = :terminal"
        values = {":epoch": _string(self.epoch), ":previous": _number(revision), ":next": _number(revision + 1),
                  ":false": {"BOOL": False}, ":terminal": {"BOOL": retire is not None}}
        names = {"#epoch": "epoch", "#revision": "revision", "#closed": "closed", "#terminal": "terminal"}
        if retire is None:
            update = "SET #state = :state, #revision = :next, #terminal = :next_terminal"
            names["#state"] = "state"
            values.update({":state": _string(_state(state)), ":next_terminal": {"BOOL": state["phase"] == "complete"}})
        else:
            update = "SET #closed = :true, #revision = :next, #proof = :proof"
            names["#proof"] = "retirement_sha256"
            values.update({":true": {"BOOL": True}, ":proof": _string(retire)})
        return {"Update": {"TableName": self.table, "Key": _key(self.pk),
            "ConditionExpression": condition, "UpdateExpression": update,
            "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}

    def save(self, state, revision):
        """Save cleanup/progress without requiring admission to remain enabled."""
        require(isinstance(state, dict) and state.get("phase") != "launching",
                "launching state requires the policy-fenced launch intent method")
        return self._save(state, revision, launch_intent=False)

    def save_launch_intent(self, state, revision):
        """Commit a new launch intent only while its exact policy remains enabled."""
        require(isinstance(state, dict) and state.get("phase") == "launching", "launch intent must enter launching")
        return self._save(state, revision, launch_intent=True)

    def _save(self, state, revision, *, launch_intent):
        item, owner = self._read_items([self.pk, "active"])
        record = self._record(item)
        require(record is not None and not record.closed and self._owner(owner), "run no longer owns the active slot")
        _state(state, record.admission["initial_payload"]["state"]["request"])
        require(record.revision == revision and not record.terminal, "state is stale or already terminal")
        _item_bytes(dict(item, state=_string(_state(state)), revision=_number(revision + 1),
                         terminal={"BOOL": state["phase"] == "complete"}))
        items = [{"ConditionCheck": self._owner_condition()}, self._state_update(state, revision)]
        if launch_intent:
            require(record.state["phase"] == "armed", "launch intent requires an armed run")
            items.append({"ConditionCheck": self._policy_condition(record.admission["policy_version"])})
        try:
            self._write(items)
        except Exception as error:
            try:
                item, owner = self._read_items([self.pk, "active"])
                observed = self._record(item)
                if (observed is not None and not observed.closed and self._owner(owner) and
                        observed.revision == revision + 1 and observed.state == state):
                    return observed.revision
            except Exception as observed_error:
                raise ReconciliationRequired("state write failed and its outcome is unreadable") from observed_error
            raise ReconciliationRequired("state write rejected or uncertain; reconcile the exact revision") from error
        return revision + 1

    def retire(self, evidence_sha256, revision):
        """Close a proven terminal run and release only its own active slot.

        The caller must validate the semantic cleanup, result and producer evidence.
        This store binds the resulting digest to one revision; it cannot prove EC2 cleanup.
        """
        _text(evidence_sha256, r"[0-9a-f]{64}", "invalid retirement evidence digest")
        _integer(revision)
        item, owner = self._read_items([self.pk, "active"])
        record = self._record(item)
        if record is not None and record.closed:
            require(record.retirement_sha256 == evidence_sha256 and record.revision == revision + 1 and
                    not self._owner(owner), "retirement differs from the retained tombstone")
            return record
        require(record is not None and record.revision == revision and record.terminal and self._owner(owner),
                "retirement requires the exact terminal owner revision")
        _item_bytes(dict(item, closed={"BOOL": True}, revision=_number(revision + 1),
                         retirement_sha256=_string(evidence_sha256)))
        try:
            self._write([self._state_update(record.state, revision, retire=evidence_sha256),
                         {"Delete": self._owner_condition()}])
        except Exception as error:
            try:
                item, owner = self._read_items([self.pk, "active"])
                observed = self._record(item)
                if (observed is not None and observed.closed and observed.revision == revision + 1 and
                        observed.retirement_sha256 == evidence_sha256 and not self._owner(owner)):
                    return observed
            except Exception as observed_error:
                raise ReconciliationRequired("retirement failed and its outcome is unreadable") from observed_error
            raise ReconciliationRequired("retirement rejected or uncertain; retain the ownership hold") from error
        try:
            item, owner = self._read_items([self.pk, "active"])
            observed = self._record(item)
            require(observed is not None and observed.closed and observed.revision == revision + 1 and
                    observed.retirement_sha256 == evidence_sha256 and not self._owner(owner), "retirement readback differs")
            return observed
        except Exception as error:
            raise ReconciliationRequired("retirement committed; readback requires reconciliation") from error
