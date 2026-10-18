"""Trusted operator only: arm a fresh, bounded window without launching an instance."""

import argparse
from datetime import datetime, timezone
import json

from .common import (MAX_OPEN_LEAD, MIN_WINDOW, STATUS_KEY, SUSPEND, WINDOW_KEY, clients, error_code,
                     get_json, group, live_instances, put_json, require, scheduled, timestamp, utc,
                     validate_window, verify_close, verify_group)


def inspect_closed(api, name):
    asg, ec2 = api["autoscaling"], api["ec2"]
    g = group(asg, name)
    require((g["MinSize"], g["DesiredCapacity"], g["MaxSize"]) == (0, 0, 0)
            and not g["Instances"], "refusing to alter an active ASG")
    require(not live_instances(ec2, name), "prior instances have not terminated")
    require(not scheduled(asg, name), "refusing to replace or extend an existing window")
    for operation, field in (("describe_policies", "ScalingPolicies"),
                             ("describe_lifecycle_hooks", "LifecycleHooks")):
        result = getattr(asg, operation)(AutoScalingGroupName=name)
        require(not result.get(field) and not result.get("NextToken"), f"unexpected {field}")
    warm = asg.describe_warm_pool(AutoScalingGroupName=name)
    require(not warm.get("WarmPoolConfiguration") and not warm.get("Instances"), "warm pool is forbidden")
    refreshes = asg.describe_instance_refreshes(AutoScalingGroupName=name)["InstanceRefreshes"]
    require(all(r["Status"] in {"Successful", "Failed", "Cancelled", "RollbackSuccessful", "RollbackFailed"}
                for r in refreshes), "instance refresh is active")
    lt = g["LaunchTemplate"]
    require(lt["Version"].isdigit(), "launch template version must be numeric")
    data = ec2.describe_launch_template_versions(LaunchTemplateId=lt["LaunchTemplateId"],
            Versions=[lt["Version"]])["LaunchTemplateVersions"][0]["LaunchTemplateData"]
    require(data["InstanceType"] == "c5d.24xlarge" and data["ImageId"].startswith("ami-")
            and not data.get("InstanceMarketOptions") and not data.get("InstanceRequirements")
            and not data.get("DisableApiTermination"), "launch template is not a disposable fixed On-Demand VM")
    asg.suspend_processes(AutoScalingGroupName=name, ScalingProcesses=sorted(SUSPEND))
    verify_group(group(asg, name), 0)


def arm(api, name, bucket, window, now=lambda: datetime.now(timezone.utc)):
    validate_window(window, now(), minimum=MIN_WINDOW)
    require(120 <= (utc(window["opens"]) - now()).total_seconds() <= MAX_OPEN_LEAD,
            "opening must be 2–5 minutes in the future")
    inspect_closed(api, name)
    # a single operator is required; never overwrite a still-valid control document
    try:
        previous = get_json(api["s3"], bucket, WINDOW_KEY)
    except Exception as error:
        if error_code(error) != "NoSuchKey":
            raise
    else:
        require(utc(previous["deadline"]) < now(), "previous window has not expired")
        require(previous["run"] != window["run"], "a run attempt cannot be rearmed")
    asg = api["autoscaling"]
    asg.put_scheduled_update_group_action(AutoScalingGroupName=name,
        ScheduledActionName="close-" + window["run"], StartTime=utc(window["deadline"]),
        MinSize=0, DesiredCapacity=0, MaxSize=0)
    verify_close(scheduled(asg, name), window)
    put_json(api["s3"], bucket, WINDOW_KEY, window)
    require(get_json(api["s3"], bucket, WINDOW_KEY) == window, "window read-back mismatch")
    pending = {"run": window["run"], "state": "pending"}
    put_json(api["s3"], bucket, STATUS_KEY, pending)
    require(get_json(api["s3"], bucket, STATUS_KEY) == pending, "pending status read-back mismatch")
    require((utc(window["opens"]) - now()).total_seconds() >= 30, "opening lead time exhausted; leave shutdown armed")
    # scheduled open cannot arrive late and reopen an already closed lease: past
    # start times are rejected, and actions within one group execute in time order
    asg.put_scheduled_update_group_action(AutoScalingGroupName=name,
        ScheduledActionName="open-" + window["run"], StartTime=utc(window["opens"]),
        MinSize=0, DesiredCapacity=0, MaxSize=1)
    actions = scheduled(asg, name)
    verify_close(actions, window)
    opening = [a for a in actions if a["ScheduledActionName"] == "open-" + window["run"]]
    require(len(opening) == 1 and utc(opening[0]["StartTime"]) == utc(window["opens"])
            and all(opening[0].get(k) == v for k, v in (("MinSize", 0), ("DesiredCapacity", 0), ("MaxSize", 1)))
            and not opening[0].get("Recurrence"), "opening read-back mismatch; inspect schedules")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for field in ("region", "asg", "bucket", "run", "baseline", "candidate", "suite", "opens", "deadline"):
        parser.add_argument("--" + field, required=True)
    args = parser.parse_args()
    window = {field: getattr(args, field) for field in ("run", "baseline", "candidate", "suite")}
    window.update(opens=timestamp(args.opens), deadline=timestamp(args.deadline))
    arm(clients(args.region), args.asg, args.bucket, window)
    print(json.dumps(window, indent=2))
    print("Armed. Keep both schedules; no instance was requested. Approve the waiting GitHub environment job.")


if __name__ == "__main__":
    main()
