"""Validate actual launch and shutdown payloads using local AWS SDK service models."""

import json
import unittest

from infra.aws_benchmark import durable
from infra.aws_benchmark.test_durable_coordinator import Cloud

try:
    import botocore.session
    from botocore.validate import validate_parameters
except ImportError:
    botocore = None


@unittest.skipIf(botocore is None, "pinned AWS SDK is not installed")
class SdkContracts(unittest.TestCase):
    def setUp(self):
        self.models = botocore.session.get_session()
        self.cloud = Cloud()
        self.attempt = durable.attempt(self.cloud.request, 1, self.cloud.now)

    def validate(self, service, operation, parameters):
        shape = self.models.get_service_model(service).operation_model(operation).input_shape
        validate_parameters(parameters, shape)

    def test_generated_allocation_is_an_ec2_runinstances_request(self):
        parameters = self.cloud.service.launch_parameters(self.cloud.request, self.attempt)
        self.validate("ec2", "RunInstances", parameters)
        self.assertEqual(parameters["ClientToken"], self.attempt["token"])
        self.assertEqual(parameters["MinCount"], parameters["MaxCount"])
        self.assertEqual({v["ResourceType"] for v in parameters["TagSpecifications"]}, {"instance", "volume"})

    def test_checksum_and_tag_inventory_requests_match_sdk_schemas(self):
        self.cloud.coordinator()
        for call in self.cloud.api["s3"].head_object.call_args_list:
            self.validate("s3", "HeadObject", call.kwargs)
        self.cloud.state["seen"]["i-" + "1" * 17]["root_volumes"] = []
        self.cloud.instances[0]["BlockDeviceMappings"] = []
        self.cloud.service.watchdog()
        for call in self.cloud.api["ec2"].describe_volumes.call_args_list:
            self.validate("ec2", "DescribeVolumes", call.kwargs)

    def test_direct_cutoff_payload_matches_scheduler_and_ec2_schemas(self):
        binding = dict(attempt_id=self.attempt["id"], close_at=self.attempt["close_at"],
                       instance_id="i-" + "1" * 17)
        schedule = self.cloud.service.schedule(binding=binding)
        self.validate("scheduler", "CreateSchedule", dict(schedule, ClientToken="a" * 64))
        self.validate("scheduler", "GetSchedule", {k: schedule[k] for k in ("Name", "GroupName")})
        payload = json.loads(schedule["Target"]["Input"])
        self.validate("ec2", "TerminateInstances", payload)
        self.assertEqual(payload, {"InstanceIds": [binding["instance_id"]]})


if __name__ == "__main__":
    unittest.main()
