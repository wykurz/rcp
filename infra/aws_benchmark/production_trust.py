"""Render and check offline trust proposals; never call GitHub or AWS.

Inputs are non-secret, real identifiers supplied by the operator. Rendering does
not prove their existence or authorize applying the returned payloads. Readback
checks compare captured settings, not live enforcement or token/key custody.
"""

import argparse
import json
from pathlib import Path
import re

from .common import require

REPOSITORY = "wykurz/rcp"
OWNER_ID = 483465
ENVIRONMENT = "aws-benchmark-history"
HISTORY_REF = "refs/heads/aws-benchmark-history"
API_ROOT = "https://api.github.com/repos/" + REPOSITORY
ENVIRONMENT_URL = API_ROOT + "/environments/" + ENVIRONMENT
WRITER_SCHEMA = "rcp-history-trust-plan-v1"
CLIENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}")
ROLE = re.compile(r"arn:(aws|aws-us-gov|aws-cn):iam::([0-9]{12}):role/([A-Za-z0-9+=,.@_-]+(?:/[A-Za-z0-9+=,.@_-]+)*)")
SECRET = re.compile(r"arn:(aws|aws-us-gov|aws-cn):secretsmanager:([a-z]{2}(?:-[a-z]+)+-[1-9][0-9]?):([0-9]{12}):secret:/rcp/production/github-read-app-[A-Za-z0-9]{6}")


def identifier(value):
    require(isinstance(value, str) and re.fullmatch(r"[1-9][0-9]{0,19}", value), "a real numeric identifier is required")
    return int(value)


def writer_config(client_id, installation_id):
    require(isinstance(client_id, str) and CLIENT.fullmatch(client_id), "a real App client ID is required")
    return client_id, identifier(installation_id)


def writer_plan(app_id, client_id, installation_id):
    client_id, installation = writer_config(client_id, installation_id)
    app = identifier(app_id)
    conditions = {"ref_name": {"include": [HISTORY_REF], "exclude": []}}
    rulesets = [
        dict(name="rcp-aws-history-writer", target="branch", enforcement="active", conditions=conditions,
            bypass_actors=[dict(actor_id=app, actor_type="Integration", bypass_mode="always")],
            rules=[dict(type="creation"), dict(type="update", parameters=dict(update_allows_fetch_and_merge=False))]),
        dict(name="rcp-aws-history-preserve", target="branch", enforcement="active", conditions=conditions,
            bypass_actors=[], rules=[dict(type="deletion"), dict(type="non_fast_forward")]),
    ]
    return dict(schema=WRITER_SCHEMA, repository=REPOSITORY,
        app=dict(id=app, client_id=client_id, installation_id=installation,
                 permissions=dict(contents="write", metadata="read"), selected_repositories=[REPOSITORY]),
        environment=dict(name=ENVIRONMENT, settings=dict(wait_timer=0, prevent_self_review=False,
            reviewers=[dict(type="User", id=OWNER_ID)],
            deployment_branch_policy=dict(protected_branches=False, custom_branch_policies=True)),
            branch_policy=dict(name="main", type="branch"),
            manual_admin_bypass="Disable administrator bypass in the environment UI; verify readback."),
        rulesets=rulesets,
        variables=dict(RCP_AWS_HISTORY_APP_CLIENT_ID=client_id,
            RCP_AWS_HISTORY_APP_INSTALLATION_ID=str(installation),
            RCP_AWS_HISTORY_TRUST_READY="false", RCP_AWS_PRODUCTION_PUBLISH_ENABLED="false"),
        secret=dict(name="RCP_AWS_HISTORY_APP_PRIVATE_KEY", scope="environment", environment=ENVIRONMENT),
        live_acceptance_required=True)


def same(actual, expected, message):
    require(json.dumps(actual, sort_keys=True) == json.dumps(expected, sort_keys=True), message)


def check_writer(plan, environment, branches, rulesets):
    """Require complete privileged readbacks; missing bypass fields never mean none."""
    require(isinstance(environment, dict) and environment.get("name") == ENVIRONMENT, "environment identity differs")
    require(environment.get("url") == ENVIRONMENT_URL, "environment repository identity is unproven")
    expected = plan["environment"]
    same(environment.get("deployment_branch_policy"), expected["settings"]["deployment_branch_policy"],
         "environment must select only explicitly configured branches")
    require(environment.get("can_admins_bypass") is False, "administrator bypass disabled is unproven")
    protections = environment.get("protection_rules")
    require(isinstance(protections, list) and all(isinstance(x, dict) for x in protections), "environment protections missing")
    require(all(x.get("type") in {"required_reviewers", "wait_timer", "branch_policy"} for x in protections),
            "unexpected environment protection; review before applying the plan")
    reviewers = [x for x in protections if x.get("type") == "required_reviewers"]
    require(len(reviewers) == 1 and reviewers[0].get("prevent_self_review") is False,
            "owner-only review must permit the owner to approve their manual request")
    entries = reviewers[0].get("reviewers")
    require(isinstance(entries, list) and len(entries) == 1 and isinstance(entries[0], dict) and entries[0].get("type") == "User" and
            isinstance(entries[0].get("reviewer"), dict) and type(entries[0]["reviewer"].get("id")) is int and
            entries[0]["reviewer"]["id"] == OWNER_ID, "environment reviewer must be the selected owner")
    require(all(x.get("wait_timer") == 0 for x in protections if x.get("type") == "wait_timer"), "unexpected wait timer")
    require(isinstance(branches, dict) and branches.get("endpoint") == ENVIRONMENT_URL + "/deployment-branch-policies" and
            isinstance(branches.get("response"), dict), "branch-policy capture endpoint differs")
    branches = branches["response"]
    require(branches.get("total_count") == 1 and
            isinstance(branches.get("branch_policies"), list) and len(branches["branch_policies"]) == 1,
            "complete single-branch policy readback is required")
    branch = branches["branch_policies"][0]
    require(isinstance(branch, dict), "branch readback missing")
    same({key: branch.get(key) for key in ("name", "type")}, expected["branch_policy"], "only the main branch may access the environment")
    require(isinstance(rulesets, list) and len(rulesets) == 2 and all(isinstance(x, dict) for x in rulesets),
            "two complete ruleset detail readbacks are required")
    found = {x.get("name"): x for x in rulesets}
    require(set(found) == {x["name"] for x in plan["rulesets"]}, "ruleset identities differ")
    ids = [x.get("id") for x in rulesets]
    require(all(type(x) is int and x > 0 for x in ids) and len(set(ids)) == 2, "resolved distinct ruleset IDs required")
    for expected_rule in plan["rulesets"]:
        actual = found[expected_rule["name"]]
        require(actual.get("source") == REPOSITORY and actual.get("source_type") == "Repository",
                "ruleset repository identity is unproven")
        require("bypass_actors" in actual, "privileged ruleset readback must expose bypass actors")
        same({key: actual.get(key) for key in expected_rule if key != "rules"},
             {key: value for key, value in expected_rule.items() if key != "rules"},
             "ruleset differs from approved exact-branch proposal")
        rules = actual.get("rules")
        require(isinstance(rules, list) and all(isinstance(x, dict) and isinstance(x.get("type"), str) for x in rules),
                "ruleset rules missing")
        same(sorted(rules, key=lambda x: x["type"]), sorted(expected_rule["rules"], key=lambda x: x["type"]),
             "ruleset rules differ from approved proposal")
    return dict(settings_match=True, live_enforcement_proven=False, repository=REPOSITORY,
                ruleset_ids=ids, app_id=plan["app"]["id"], installation_id=plan["app"]["installation_id"])


def reader_policy(secret_arn, admission_role, collector_role, recovery_role):
    require(isinstance(secret_arn, str) and (secret := SECRET.fullmatch(secret_arn)), "exact reader secret ARN required")
    principals = [admission_role, collector_role, recovery_role]
    require(all(isinstance(x, str) for x in principals) and len(set(principals)) == 3, "three distinct explicitly selected role ARNs required")
    for principal in principals:
        require(isinstance(principal, str) and (role := ROLE.fullmatch(principal)) and
                (role[1], role[2]) == (secret[1], secret[3]), "roles must be exact IAM role ARNs in the secret account/partition")
    return {"Version": "2012-10-17", "Statement": [
        dict(Sid="DenyReadOutsideSelectedRoles", Effect="Deny", Principal="*",
             Action="secretsmanager:GetSecretValue", Resource=secret_arn,
             Condition={"ArnNotEquals": {"aws:PrincipalArn": principals}}),
        dict(Sid="DenyReaderNonCurrentStage", Effect="Deny", Principal="*",
             Action="secretsmanager:GetSecretValue", Resource=secret_arn,
             Condition={"ArnEquals": {"aws:PrincipalArn": principals[:2]},
                        "StringNotEquals": {"secretsmanager:VersionStage": "AWSCURRENT"}}),
    ]}


def read(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(1024 * 1024 + 1)
    require(len(raw) <= 1024 * 1024, "readback exceeds limit")
    return json.loads(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("writer-plan", "writer-check"):
        command = commands.add_parser(name)
        for field in ("app-id", "client-id", "installation-id"):
            command.add_argument("--" + field, required=True)
        if name == "writer-check":
            for field in ("environment-readback", "branches-readback", "rulesets-readback"):
                command.add_argument("--" + field, required=True, type=Path)
    command = commands.add_parser("reader-policy")
    for field in ("secret-arn", "admission-role", "collector-role", "recovery-role"):
        command.add_argument("--" + field, required=True)
    args = parser.parse_args()
    if args.command == "reader-policy":
        value = reader_policy(args.secret_arn, args.admission_role, args.collector_role, args.recovery_role)
    else:
        value = writer_plan(args.app_id, args.client_id, args.installation_id)
        if args.command == "writer-check":
            value = check_writer(value, read(args.environment_readback), read(args.branches_readback), read(args.rulesets_readback))
    print(json.dumps(value, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, TypeError, KeyError):
        print("Trust proposal rejected: invalid or incomplete non-secret inputs.")
        raise SystemExit(1) from None
