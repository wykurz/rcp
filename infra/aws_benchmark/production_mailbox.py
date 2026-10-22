"""Explicit fixed mailboxes for legacy runs or one EC2 credential origin.

The full instance ARN remains in the object prefix so an IAM resource can use
${ec2:SourceInstanceArn} directly. Paths alone do not enforce access isolation;
the production worker role must grant only that instance-origin prefix.
"""

from dataclasses import dataclass
import re

from .common import BINARIES, REQUEST_KEY, RESULTS_KEY, STATUS_KEY, WINDOW_KEY, require


@dataclass(frozen=True)
class MailboxPaths:
    account: str | None = None
    region: str | None = None
    instance_id: str | None = None

    def __post_init__(self):
        if self.account is self.region is self.instance_id is None:
            return
        require(isinstance(self.account, str) and re.fullmatch(r"[0-9]{12}", self.account),
                "invalid production mailbox account")
        require(isinstance(self.region, str) and not self.region.startswith("cn-") and
                re.fullmatch(r"[a-z]{2}-[a-z]+-[1-9][0-9]?", self.region),
                "invalid standard-partition mailbox region")
        require(isinstance(self.instance_id, str) and re.fullmatch(r"i-[0-9a-f]{17}", self.instance_id),
                "invalid production mailbox instance")

    @classmethod
    def legacy(cls):
        return cls()

    @classmethod
    def for_instance(cls, account, region, instance_id):
        require(account is not None and region is not None and instance_id is not None,
                "production mailbox requires a complete instance identity")
        return cls(account, region, instance_id)

    @property
    def scoped(self):
        return self.instance_id is not None

    @property
    def prefix(self):
        if not self.scoped:
            return ""
        return f"production/instances/arn:aws:ec2:{self.region}:{self.account}:instance/{self.instance_id}/"

    @property
    def window(self):
        return self.prefix + WINDOW_KEY

    @property
    def request(self):
        return self.prefix + REQUEST_KEY

    @property
    def results(self):
        return self.prefix + RESULTS_KEY

    @property
    def status(self):
        return self.prefix + STATUS_KEY

    def binary(self, name):
        require(isinstance(name, str) and name in BINARIES, "unexpected mailbox binary")
        return self.prefix + "input/" + name

    @property
    def write_conditions(self):
        return {"IfNoneMatch": "*", "ServerSideEncryption": "AES256"} if self.scoped else {}
