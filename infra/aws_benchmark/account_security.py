"""Pure account prerequisite validation shared by all trusted operators."""

import re

DHMC_SETTING = "/ssm/managed-instance/default-ec2-instance-management-role"


def dhmc_arn(region, account, partition="aws"):
    if (partition not in {"aws", "aws-us-gov", "aws-cn"}
            or not re.fullmatch(r"[a-z0-9-]+", region) or not re.fullmatch(r"[0-9]{12}", account)):
        raise ValueError("invalid account or region for management prerequisite")
    return f"arn:{partition}:ssm:{region}:{account}:servicesetting{DHMC_SETTING}"


def verify_dhmc_disabled(setting, arn):
    if not (isinstance(setting, dict) and setting.get("ARN") == arn
            and setting.get("SettingId") in {DHMC_SETTING, arn}
            and setting.get("Status") == "Default" and setting.get("SettingValue") == "$None"):
        raise ValueError("Default Host Management Configuration must already be disabled; no settings were changed")
