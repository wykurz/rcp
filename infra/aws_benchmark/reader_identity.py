"""Non-secret reader resource identities supported by the production runtime."""

import re

READER_RESOURCE = re.compile(r"arn:aws:secretsmanager:([a-z]{2}-[a-z]+-[1-9][0-9]?):([0-9]{12}):secret:/rcp/production/github-read-app-[A-Za-z0-9]{6}")


def reader_resource_identity(value):
    match = READER_RESOURCE.fullmatch(value) if isinstance(value, str) else None
    return match.groups() if match else None
