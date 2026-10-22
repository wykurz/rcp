"""Trusted operator only: arm a fresh, bounded window without launching an instance."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import re

from .account_security import dhmc_arn, verify_dhmc_disabled
from .ami_image import launch_template_data, verify_startup
from .common import (INSTANCE_TYPE, MAX_OPEN_LEAD, MIN_WINDOW, STATUS_KEY, SUSPEND, WINDOW_KEY, Budget,
                     checked_clients, get_json, group, live_instances, pending_status, put_json,
                     require, retry_io, scheduled, timestamp, utc,
                     validate_admission, validate_window, verify_close, verify_group)
from .mailbox import claim, verify_previous_window


def operator_clients(region):
    import boto3
    from botocore.config import Config
    config = Config(connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 1})
    session = boto3.Session(region_name=region)
    api = checked_clients(session, config)
    # ssm is an operator prerequisite read only; runtime and worker receive no client or grant
    api["ssm"] = session.client("ssm", config=config)
    return api


def verify_instance_management_disabled(ssm, group_arn, budget):
    parts = group_arn.split(":", 5)
    require(len(parts) == 6 and parts[0] == "arn" and parts[2] == "autoscaling"
            and parts[1] in {"aws", "aws-us-gov", "aws-cn"}
            and re.fullmatch(r"[a-z0-9-]+", parts[3]) and re.fullmatch(r"[0-9]{12}", parts[4]),
            "invalid ASG ARN for account prerequisite check")
    setting_arn = dhmc_arn(parts[3], parts[4], parts[1])
    setting = retry_io(lambda: ssm.get_service_setting(SettingId=setting_arn), budget)["ServiceSetting"]
    verify_dhmc_disabled(setting, setting_arn)


def inspect_closed(api, name, bucket, budget):
    def read(operation):
        return retry_io(operation, budget)
    asg, ec2 = api["autoscaling"], api["ec2"]
    g = read(lambda: group(asg, name))
    require((g["MinSize"], g["DesiredCapacity"], g["MaxSize"]) == (0, 0, 0)
            and not g["Instances"], "refusing to alter an active ASG")
    verify_instance_management_disabled(api["ssm"], g["AutoScalingGroupARN"], budget)
    require(not read(lambda: live_instances(ec2, name)), "prior instances have not terminated")
    require(not read(lambda: scheduled(asg, name)), "refusing to replace or extend an existing window")
    for operation, field in (("describe_policies", "ScalingPolicies"),
                             ("describe_lifecycle_hooks", "LifecycleHooks")):
        result = read(lambda: getattr(asg, operation)(AutoScalingGroupName=name))
        require(not result.get(field) and not result.get("NextToken"), f"unexpected {field}")
    warm = read(lambda: asg.describe_warm_pool(AutoScalingGroupName=name))
    require(not warm.get("WarmPoolConfiguration") and not warm.get("Instances"), "warm pool is forbidden")
    refreshes = read(lambda: asg.describe_instance_refreshes(AutoScalingGroupName=name))["InstanceRefreshes"]
    require(all(r["Status"] in {"Successful", "Failed", "Cancelled", "RollbackSuccessful", "RollbackFailed"}
                for r in refreshes), "instance refresh is active")
    lt = g["LaunchTemplate"]
    data = launch_template_data(ec2, lt["LaunchTemplateId"], lt["Version"], read=read)
    require(data["InstanceType"] == INSTANCE_TYPE and data["ImageId"].startswith("ami-")
            and not data.get("InstanceMarketOptions") and not data.get("InstanceRequirements")
            and not data.get("DisableApiTermination"), "launch template is not a disposable fixed On-Demand VM")
    verify_startup(data, "operator-window", bucket, g["AutoScalingGroupARN"].split(":")[3])
    budget.left()
    asg.suspend_processes(AutoScalingGroupName=name, ScalingProcesses=sorted(SUSPEND))
    verify_group(read(lambda: group(asg, name)), 0)


def arm(api, name, bucket, window, now=lambda: datetime.now(timezone.utc)):
    require(isinstance(window, dict) and "protocol" not in window,
            "operator window cannot use a direct or historical protocol")
    validate_window(window, now(), minimum=MIN_WINDOW)
    validate_admission(window, window=True)
    require(120 <= (utc(window["opens"]) - now()).total_seconds() <= MAX_OPEN_LEAD,
            "opening must be 2–5 minutes in the future")
    budget = Budget(datetime.now(timezone.utc) + timedelta(seconds=60))
    def read(operation):
        return retry_io(operation, budget)
    inspect_closed(api, name, bucket, budget)
    verify_previous_window(api["s3"], bucket, window["run"], now, budget)
    budget.left()
    owner = claim(api["s3"], bucket, "operator-window", window)
    asg = api["autoscaling"]
    # never replay a schedule mutation after an uncertain response; reconcile the armed cutoff
    budget.left()
    asg.put_scheduled_update_group_action(AutoScalingGroupName=name,
        ScheduledActionName="close-" + window["run"], StartTime=utc(window["deadline"]),
        MinSize=0, DesiredCapacity=0, MaxSize=0)
    verify_close(read(lambda: scheduled(asg, name)), window)
    retry_io(lambda: put_json(api["s3"], bucket, WINDOW_KEY, window), budget)
    require(read(lambda: get_json(api["s3"], bucket, WINDOW_KEY)) == window, "window read-back mismatch")
    pending = pending_status(window["run"])
    retry_io(lambda: put_json(api["s3"], bucket, STATUS_KEY, pending), budget)
    require(read(lambda: get_json(api["s3"], bucket, STATUS_KEY)) == pending, "pending status read-back mismatch")
    require((utc(window["opens"]) - now()).total_seconds() >= 30, "opening lead time exhausted; leave shutdown armed")
    # scheduled open cannot arrive late and reopen an already closed lease: past
    # start times are rejected, and actions within one group execute in time order
    budget.left()
    asg.put_scheduled_update_group_action(AutoScalingGroupName=name,
        ScheduledActionName="open-" + window["run"], StartTime=utc(window["opens"]),
        MinSize=0, DesiredCapacity=0, MaxSize=1)
    actions = read(lambda: scheduled(asg, name))
    verify_close(actions, window)
    opening = [a for a in actions if a["ScheduledActionName"] == "open-" + window["run"]]
    require(len(opening) == 1 and utc(opening[0]["StartTime"]) == utc(window["opens"])
            and all(opening[0].get(k) == v for k, v in (("MinSize", 0), ("DesiredCapacity", 0), ("MaxSize", 1)))
            and not opening[0].get("Recurrence"), "opening read-back mismatch; inspect schedules")
    return owner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ("region", "asg", "bucket", "run", "baseline", "candidate", "suite", "opens", "deadline"):
        parser.add_argument("--" + field, required=True)
    args = parser.parse_args()
    window = {field: getattr(args, field) for field in ("run", "baseline", "candidate", "suite")}
    window.update(opens=timestamp(args.opens), deadline=timestamp(args.deadline))
    owner = arm(operator_clients(args.region), args.asg, args.bucket, window)
    print(json.dumps(dict(window=window, mailbox_owner=owner), indent=2))
    print("Armed. Keep both schedules; no instance was requested. Approve the waiting GitHub environment job.")


if __name__ == "__main__":
    main()
