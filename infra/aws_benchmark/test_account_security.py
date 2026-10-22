"""Shared DHMC identity checks do not perform cloud operations."""

import unittest

from infra.aws_benchmark.account_security import DHMC_SETTING, dhmc_arn, verify_dhmc_disabled


class AccountSecurityTests(unittest.TestCase):
    def test_all_supported_partitions_bind_exact_response_arn(self):
        for partition, region in (("aws", "us-east-1"), ("aws-us-gov", "us-gov-west-1"), ("aws-cn", "cn-north-1")):
            arn = dhmc_arn(region, "123456789012", partition)
            for identifier in (DHMC_SETTING, arn):
                verify_dhmc_disabled(dict(ARN=arn, SettingId=identifier, Status="Default", SettingValue="$None"), arn)

    def test_enabled_incomplete_and_cross_account_settings_are_rejected(self):
        arn = dhmc_arn("us-east-1", "123456789012")
        valid = dict(ARN=arn, SettingId=DHMC_SETTING, Status="Default", SettingValue="$None")
        for key in valid:
            value = dict(valid)
            value.pop(key)
            with self.subTest(missing=key), self.assertRaises(ValueError):
                verify_dhmc_disabled(value, arn)
        for changes in ({"Status": "Customized"}, {"SettingValue": "management-role"},
                        {"ARN": arn.replace("123456789012", "999999999999")},
                        {"SettingId": "/another-setting"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                verify_dhmc_disabled({**valid, **changes}, arn)


if __name__ == "__main__":
    unittest.main()
