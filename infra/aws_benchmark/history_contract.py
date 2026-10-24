"""Fixed public-history identities shared by setup, preparation and acceptance."""

import re

from .common import require

REPOSITORY = "wykurz/rcp"
REPOSITORY_ID = "655073755"
OWNER_ID = 483465
HISTORY_REF = "refs/heads/aws-benchmark-history"
HISTORY_NAMESPACE = HISTORY_REF + "/**/*"
ENVIRONMENT = "aws-benchmark-history"
WRITER_SECRET_NAME = "RCP_AWS_HISTORY_APP_PRIVATE_KEY"
CLIENT_VARIABLE = "RCP_AWS_HISTORY_APP_CLIENT_ID"
INSTALLATION_VARIABLE = "RCP_AWS_HISTORY_APP_INSTALLATION_ID"
PUBLICATION_FLAG = "RCP_AWS_PRODUCTION_PUBLISH_ENABLED"
TRUST_FLAG = "RCP_AWS_HISTORY_TRUST_READY"
CANARY_FLAG = "RCP_AWS_HISTORY_CANARY_ENABLED"
REPOSITORY_FLAGS = (PUBLICATION_FLAG, TRUST_FLAG, CANARY_FLAG)
PUBLICATION_WORKFLOW = "aws-benchmark-publish.yml"
CANARY_WORKFLOW = "aws-benchmark-history-canary.yml"


def identifier(value):
    require(isinstance(value, str) and re.fullmatch(r"[1-9][0-9]{0,19}", value), "a real numeric identifier is required")
    return int(value)
