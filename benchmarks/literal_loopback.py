"""Literal-loopback execution contract, separate from local and shaped RTT records.

The first native executor accepts reviewed binaries and fixed tiny fixtures only.
This is not the production sandbox or a general hostile-code isolation boundary.
No AWS clients, credentials, admission or publication actions belong here.
"""
from dataclasses import dataclass
import math
from pathlib import PurePosixPath
import re

SCHEMA = "rcp-owned-literal-loopback-v1"
PROFILE = "systemd-private-ipv4-loopback-per-copy-v1"
GUEST = "/opt/rcp-loopback"
UNIT_PREFIX = "rcp-literal-loopback-"
CPU_SCOPE = "whole fresh copy cgroup user+system; master, both endpoints, SSH and support processes"
MEMORY_SCOPE = "fresh copy cgroup memory.peak including cache/kernel/support charges; not RSS"
WALL_SCOPE = "unit main-process lifetime including private SSH setup and teardown; excludes fixture/key preparation and validation"
MAX_NATIVE_SECONDS = 180


def need(value, message):
    if not value:
        raise ValueError(message)


def unit_name(identifier):
    need(isinstance(identifier, str) and re.fullmatch(r"[0-9a-f]{32}", identifier),
         "one UUID-derived trial identity required")
    return UNIT_PREFIX + identifier + ".service"


def account_view(text, name, uid, gid):
    """Change only the chosen account's private HOME and login shell."""
    need(isinstance(name, str) and re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", name) and
         name != "root" and type(uid) is int and uid > 0 and type(gid) is int and gid > 0,
         "existing nonroot account required")
    rows, found = [], 0
    for line in text.splitlines():
        if not line or line.startswith("#"):
            rows.append(line)
            continue
        fields = line.split(":")
        need(len(fields) == 7 and fields[0] and re.fullmatch(r"[0-9]{1,10}", fields[2]) and
             re.fullmatch(r"[0-9]{1,10}", fields[3]),
             "malformed account database row")
        need(fields[1] in {"x", "*", "!", "!!", ""},
             "private account view must not copy password credential material")
        need(int(fields[2]) != uid or fields[0] == name, "selected UID has another account alias")
        if fields[0] == name:
            need(len(fields) == 7 and fields[2:4] == [str(uid), str(gid)],
                 "selected account identity differs")
            fields[5:7] = [GUEST + "/home", "/bin/sh"]
            line = ":".join(fields)
            found += 1
        rows.append(line)
    need(found == 1, "selected account absent or duplicated")
    return "\n".join(rows) + "\n"


def service_properties(*, identifier, account, uid, gid, host_root):
    """Plan a transient native-smoke unit; never start or modify one."""
    name = unit_name(identifier)
    need(isinstance(host_root, str) and host_root.startswith("/") and not host_root.startswith("//") and
         str(PurePosixPath(host_root)) == host_root and
         PurePosixPath(host_root).name == identifier and
         len(PurePosixPath(host_root).parts) > 3 and ".." not in PurePosixPath(host_root).parts and
         re.fullmatch(r"/[A-Za-z0-9_./-]+", host_root) and
         type(uid) is int and uid > 0 and type(gid) is int and gid > 0 and
         isinstance(account, str) and account != "root" and
         re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", account),
         "canonical owned fixture root and nonroot account required")
    # Keep the host fixture hidden behind one private mount alias. Do not expose
    # the host home, WSL drives, runtime/control sockets or the host temporary dirs.
    return name, {
        "Type": "exec", "RemainAfterExit": "yes",
        "CPUAccounting": "yes", "MemoryAccounting": "yes", "SupplementaryGroups": "",
        "User": str(uid), "Group": str(gid),
        "RuntimeMaxSec": str(MAX_NATIVE_SECONDS), "TimeoutStopSec": "10",
        "KillMode": "control-group", "SendSIGKILL": "yes",
        "NoNewPrivileges": "yes", "CapabilityBoundingSet": "",
        "PrivateNetwork": "yes", "PrivateDevices": "yes", "PrivateMounts": "yes",
        "ProtectSystem": "strict", "ProtectHome": "yes",
        "ProtectKernelTunables": "yes", "ProtectKernelModules": "yes",
        "ProtectControlGroups": "yes", "RestrictSUIDSGID": "yes",
        "RestrictNamespaces": "yes", "LockPersonality": "yes",
        "RestrictAddressFamilies": "AF_UNIX AF_INET",
        "MemoryMax": "512M", "TasksMax": "256", "LimitNOFILE": "4096",
        "LimitCORE": "0", "LimitFSIZE": "4194304", "UMask": "0077",
        "TemporaryFileSystem": "/opt:ro /run:ro /tmp:size=16m,mode=1777 /var/tmp:size=16m,mode=1777",
        "BindReadOnlyPaths": host_root + ":" + GUEST + " " + host_root + "/passwd:/etc/passwd",
        "BindPaths": host_root + "/home:" + GUEST + "/home " +
                     host_root + "/destination:" + GUEST + "/destination",
        "ReadWritePaths": GUEST + "/home " + GUEST + "/destination",
        "InaccessiblePaths": "-/mnt -/media -/srv -/root -/home -/var/lib/cloud -/var/lib/docker -/var/lib/containers",
        "WorkingDirectory": GUEST,
        "StandardOutput": "null", "StandardError": "null",
    }


def copy_argv():
    """Both operands and every rcp listener use actual IPv4 loopback."""
    return [GUEST + "/bin/rcp", "--force-remote", "--bind-ip=127.0.0.1",
            "--rcpd-path=" + GUEST + "/bin/rcpd", "--summary",
            "--max-files-in-flight=4", "--max-connections=4",
            "127.0.0.1:" + GUEST + "/source",
            "127.0.0.1:" + GUEST + "/destination/copied"]


def ssh_wrapper():
    """Current rcp adds StrictHostKeyChecking=no; prepend the harness constraint."""
    return ("#!/bin/sh\nexec /usr/bin/ssh -F " + GUEST +
        "/ssh_config -o StrictHostKeyChecking=yes -o UserKnownHostsFile=" + GUEST +
        "/home/known_hosts -o GlobalKnownHostsFile=/dev/null "
        "-o HostName=127.0.0.1 -o Port=2222 -o IdentityAgent=none "
        "-o IdentitiesOnly=yes -o ProxyCommand=none -o ProxyJump=none \"$@\"\n")


def ssh_server_config(account):
    need(isinstance(account, str) and isinstance(account, str) and re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", account),
         "existing nonroot account required")
    return "\n".join([
        "Port 2222", "ListenAddress 127.0.0.1",
        "HostKey " + GUEST + "/home/hostkey",
        "PidFile " + GUEST + "/home/sshd.pid",
        "AuthorizedKeysFile " + GUEST + "/home/authorized_keys",
        "AllowUsers " + account, "AuthenticationMethods publickey",
        "PasswordAuthentication no", "KbdInteractiveAuthentication no",
        "UsePAM no", "PermitRootLogin no", "StrictModes yes",
        "PermitUserEnvironment no", "PermitUserRC no", "DisableForwarding yes",
        "UseDNS no", "PrintMotd no", "LogLevel ERROR",
        "LoginGraceTime 10", "MaxAuthTries 2", "MaxSessions 8",
    ]) + "\n"


def ssh_client_config(account):
    need(isinstance(account, str) and isinstance(account, str) and re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", account),
         "existing nonroot account required")
    return "\n".join([
        "Host *", "  User " + account, "  HostName 127.0.0.1", "  Port 2222",
        "  IdentityFile " + GUEST + "/home/identity",
        "  IdentityAgent none", "  IdentitiesOnly yes",
        "  UserKnownHostsFile " + GUEST + "/home/known_hosts",
        "  GlobalKnownHostsFile /dev/null", "  StrictHostKeyChecking yes",
        "  BatchMode yes", "  ConnectTimeout 5", "  ForwardAgent no",
        "  ClearAllForwardings yes", "  ProxyCommand none", "  ProxyJump none",
        "  ControlPersist no", "  ForkAfterAuthentication no",
    ]) + "\n"


def _fields(text):
    result = {}
    for line in text.splitlines():
        fields = line.split()
        need(len(fields) == 2 and fields[0] not in result and
             re.fullmatch(r"[0-9]{1,20}", fields[1]), "invalid cgroup counter")
        result[fields[0]] = int(fields[1])
    return result


@dataclass(frozen=True)
class RetainedUnit:
    unit: str
    invocation_id: str
    cgroup: str

    @classmethod
    def capture(cls, identifier, properties):
        unit = unit_name(identifier)
        invocation = properties.get("InvocationID", "")
        cgroup = "/system.slice/" + unit
        need(re.fullmatch(r"[0-9a-f]{32}", invocation) and invocation != "0" * 32 and
             properties.get("Id") == unit and properties.get("ControlGroup") == cgroup and
             properties.get("Type") == "exec" and properties.get("RemainAfterExit") == "yes",
             "newly created retained unit identity differs")
        return cls(unit, invocation, cgroup)

    def check(self, properties):
        need(properties.get("Id") == self.unit and
             properties.get("InvocationID") == self.invocation_id and
             properties.get("ControlGroup") == self.cgroup, "retained unit identity changed")

    def final_metrics(self, properties, *, events, cpu_stat, memory_peak):
        """Require recursive emptiness and the same retained cgroup; no fallback."""
        self.check(properties)
        need(properties.get("ActiveState") == "active" and
             properties.get("SubState") == "exited" and properties.get("Result") == "success" and
             properties.get("ExecMainStatus") == "0", "copy unit did not finish successfully")
        counters, cpu = _fields(events), _fields(cpu_stat)
        need(counters.get("populated") == 0, "copy cgroup retains descendants")
        need({"user_usec", "system_usec"} <= cpu.keys(), "per-copy CPU counters unavailable")
        need(isinstance(memory_peak, str) and re.fullmatch(r"[0-9]{1,20}\n?", memory_peak),
             "per-copy peak memory unavailable")
        need(int(memory_peak) > 0 and cpu["user_usec"] + cpu["system_usec"] > 0,
             "per-copy resource counters are empty")
        start, end = properties.get("ExecMainStartTimestampMonotonic", ""), properties.get("ExecMainExitTimestampMonotonic", "")
        need(re.fullmatch(r"[0-9]{1,20}", start) and re.fullmatch(r"[0-9]{1,20}", end) and
             0 < int(start) < int(end), "unit lifetime timestamps unavailable")
        duration = (int(end) - int(start)) / 1_000_000
        need(math.isfinite(duration) and duration <= MAX_NATIVE_SECONDS + 10,
             "unit lifetime exceeds the native smoke contract")
        return {
            "schema": SCHEMA, "profile": PROFILE,
            "unit_duration_seconds": duration,
            "cpu_user_seconds": cpu["user_usec"] / 1_000_000,
            "cpu_system_seconds": cpu["system_usec"] / 1_000_000,
            "cpu_total_seconds": (cpu["user_usec"] + cpu["system_usec"]) / 1_000_000,
            "peak_cgroup_charged_memory_bytes": int(memory_peak),
            "cpu_scope": CPU_SCOPE, "memory_scope": MEMORY_SCOPE, "wall_scope": WALL_SCOPE,
            "all_descendants_exited": True,
        }
