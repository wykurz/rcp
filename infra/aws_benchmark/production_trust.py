"""Render and check non-secret offline trust proposals; never call GitHub or AWS.

Captured metadata is an operator-supplied snapshot, not remote attestation.
Names-only inventories cannot prove key custody or installation scope. Applying
any proposal and testing live enforcement require separate approval.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from urllib.parse import quote

from .common import ValidationError, require
from .history_contract import (REPOSITORY, REPOSITORY_ID, OWNER_ID, ENVIRONMENT,
    HISTORY_REF, HISTORY_NAMESPACE, WRITER_SECRET_NAME, CLIENT_VARIABLE,
    INSTALLATION_VARIABLE, REPOSITORY_FLAGS, PUBLICATION_FLAG, TRUST_FLAG, CANARY_FLAG, identifier)
from .reader_identity import reader_resource_identity
from .results import json_value, read_file

API_ROOT = "https://api.github.com/repos/" + REPOSITORY
ENVIRONMENT_URL = API_ROOT + "/environments/" + ENVIRONMENT
WRITER_SCHEMA = "rcp-history-trust-plan-v2"
CLIENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}")
ROLE = re.compile(r"arn:aws:iam::([0-9]{12}):role/([A-Za-z0-9+=,.@_-]+(?:/[A-Za-z0-9+=,.@_-]+)*)")
STACK = re.compile(r"arn:aws:cloudformation:([a-z]{2}-[a-z]+-[1-9][0-9]?):([0-9]{12}):stack/([A-Za-z][A-Za-z0-9-]{0,127})/([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})")
MAX_CAPTURE = 1024 * 1024
WRITER_CAPTURES = ("environment", "branches", "rulesets", "repository", "app", "inventory", "custom_rules")


def writer_config(client_id, installation_id):
    require(isinstance(client_id, str) and CLIENT.fullmatch(client_id), "a real App client ID is required")
    return client_id, identifier(installation_id)


def writer_plan(app_id, client_id, installation_id):
    client_id, installation = writer_config(client_id, installation_id)
    app = identifier(app_id)
    # the namespace restriction prevents a child ref blocking first creation
    conditions = {"ref_name": {"include": [HISTORY_REF, HISTORY_NAMESPACE], "exclude": []}}
    rulesets = [
        dict(name="rcp-aws-history-writer", target="branch", enforcement="active", conditions=conditions,
            bypass_actors=[dict(actor_id=app, actor_type="Integration", bypass_mode="always")],
            rules=[dict(type="creation"), dict(type="update")]),
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
        variables=dict(repository={name: "false" for name in REPOSITORY_FLAGS},
            environment={CLIENT_VARIABLE: client_id, INSTALLATION_VARIABLE: str(installation)}),
        secret=dict(name=WRITER_SECRET_NAME, scope="environment", environment=ENVIRONMENT),
        live_acceptance_required=True)


def same(actual, expected, message):
    require(json.dumps(actual, sort_keys=True) == json.dumps(expected, sort_keys=True), message)


def capture(value, endpoint):
    require(isinstance(value, dict) and set(value) == {"endpoint", "response"} and
            value["endpoint"] == endpoint and isinstance(value["response"], dict), "capture endpoint or response differs")
    return value["response"]


def inventory_items(value, endpoint, key):
    """Require contiguous pages using the endpoint's documented page size."""
    page_size = 30 if key == "variables" else 100
    require(isinstance(value, dict) and set(value) == {"pages"} and
            isinstance(value["pages"], list) and 0 < len(value["pages"]) <= 100,
            "complete paginated inventory required")
    items, total = [], None
    for number, page in enumerate(value["pages"], 1):
        response = capture(page, endpoint + "?per_page=" + str(page_size) + "&page=" + str(number))
        count = response.get("total_count")
        require(type(count) is int and 0 <= count <= 10000 and (total is None or count == total),
                "inventory counts must be bounded consistent integers")
        total = count
        chunk = response.get(key)
        require(isinstance(chunk, list) and all(isinstance(x, dict) for x in chunk), "inventory items missing")
        expected = min(page_size, max(0, total - (number - 1) * page_size))
        require(len(chunk) == expected, "inventory page is incomplete")
        items.extend(chunk)
    require(len(value["pages"]) == max(1, (total + page_size - 1) // page_size) and len(items) == total,
            "inventory pagination is incomplete or duplicated")
    names = [x.get("name") for x in items]
    require(all(isinstance(x, str) and 0 < len(x) <= 255 for x in names) and len(set(names)) == len(names),
            "inventory names must be present and unique")
    if key in {"secrets", "variables"}:
        require(all(re.fullmatch(r"[A-Z_][A-Z0-9_]{0,254}", name) for name in names),
                "secret and variable metadata names must use canonical identifiers")
    if key == "secrets":
        require(all(set(x) <= {"name", "created_at", "updated_at"} for x in items),
                "secret inventory must contain metadata only")
    return {x["name"]: x for x in items}


def writer_identity(plan, repository, app):
    repo = capture(repository, API_ROOT)
    require(type(repo.get("id")) is int and str(repo["id"]) == REPOSITORY_ID and
            repo.get("full_name") == REPOSITORY and repo.get("fork") is False,
            "expected non-fork repository identity is unproven")
    owner = repo.get("owner")
    require(isinstance(owner, dict) and type(owner.get("id")) is int and owner["id"] == OWNER_ID and
            owner.get("type") == "User" and owner.get("login") == REPOSITORY.split("/")[0],
            "personal repository owner identity differs")
    require(isinstance(app, dict) and isinstance(app.get("response"), dict), "App metadata capture required")
    slug = app["response"].get("slug")
    require(isinstance(slug, str) and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,99}", slug), "App slug is invalid")
    metadata = capture(app, "https://api.github.com/apps/" + slug)
    require(type(metadata.get("id")) is int and metadata["id"] == plan["app"]["id"] and
            metadata.get("client_id") == plan["app"]["client_id"], "App numeric and client identities differ")
    app_owner = metadata.get("owner")
    require(isinstance(app_owner, dict) and type(app_owner.get("id")) is int and app_owner["id"] == OWNER_ID and
            app_owner.get("type") == "User" and app_owner.get("login") == owner["login"], "App owner differs")
    same(metadata.get("permissions"), plan["app"]["permissions"], "App permissions differ")
    same(metadata.get("events"), [], "App event subscriptions must be empty")


def writer_inventory(plan, inventory, key_state, phase):
    require(key_state in {"absent", "present"}, "invalid writer key metadata phase")
    require(isinstance(inventory, dict) and set(inventory) == {
        "environments", "repository_secrets", "dependabot_secrets", "environment_secrets",
        "repository_variables", "environment_variables"}, "complete scoped inventory required")
    environments = inventory_items(inventory["environments"], API_ROOT + "/environments", "environments")
    require(ENVIRONMENT in environments, "protected writer environment is absent")
    require(len({name.casefold() for name in environments}) == len(environments), "environment names collide")
    for name, item in environments.items():
        require(item.get("url") == API_ROOT + "/environments/" + name,
                "environment inventory identity differs")
    for key, suffix in (("repository_secrets", "/actions/secrets"), ("dependabot_secrets", "/dependabot/secrets")):
        entries = inventory_items(inventory[key], API_ROOT + suffix, "secrets")
        require(WRITER_SECRET_NAME not in entries, "writer secret name exists outside its environment")
    variables = inventory_items(inventory["repository_variables"], API_ROOT + "/actions/variables", "variables")
    require(not {CLIENT_VARIABLE, INSTALLATION_VARIABLE} & variables.keys(), "App variables must be environment scoped")
    expected_flags = dict(plan["variables"]["repository"])
    if phase == "canary":
        expected_flags[CANARY_FLAG] = "true"
    elif phase == "publication":
        expected_flags[TRUST_FLAG] = "true"
        expected_flags[PUBLICATION_FLAG] = "true"
    same({name: variables.get(name, {}).get("value") for name in REPOSITORY_FLAGS}, expected_flags,
         "repository flags differ from the selected setup/canary/publication phase")
    for key, field, suffix in (("environment_secrets", "secrets", "/secrets"),
                               ("environment_variables", "variables", "/variables")):
        values = inventory[key]
        require(isinstance(values, dict) and set(values) == set(environments), "every environment needs a complete inventory")
        for name in environments:
            entries = inventory_items(values[name], API_ROOT + "/environments/" + quote(name, safe="") + suffix, field)
            if field == "secrets":
                require((WRITER_SECRET_NAME in entries) == (name == ENVIRONMENT and key_state == "present"),
                        "writer secret name placement differs from the selected absent/present phase")
            else:
                require(not set(REPOSITORY_FLAGS) & entries.keys(), "repository gate flags must not be shadowed in environments")
                if name == ENVIRONMENT:
                    same({key: entries.get(key, {}).get("value") for key in plan["variables"]["environment"]},
                         plan["variables"]["environment"], "writer environment App variables differ")
                else:
                    require(not {CLIENT_VARIABLE, INSTALLATION_VARIABLE} & entries.keys(),
                            "writer App variables exist in another environment")


def approved_plan(plan):
    require(isinstance(plan, dict) and isinstance(plan.get("app"), dict), "approved writer plan required")
    app = plan["app"]
    require(type(app.get("id")) is int and type(app.get("installation_id")) is int,
            "approved plan identifiers must be integers")
    expected = writer_plan(str(app["id"]), app.get("client_id"), str(app["installation_id"]))
    same(plan, expected, "writer plan differs from the supported reviewed contract")
    return hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def conditions(value):
    require(isinstance(value, dict) and set(value) == {"ref_name"} and
            isinstance(value["ref_name"], dict) and set(value["ref_name"]) == {"include", "exclude"},
            "ruleset ref conditions differ")
    result = {}
    for key, values in value["ref_name"].items():
        require(isinstance(values, list) and all(isinstance(x, str) for x in values), "ruleset ref patterns missing")
        result[key] = sorted(set(values))
    return {"ref_name": result}


def check_writer(plan, environment, branches, rulesets, repository, app, inventory, custom_rules, *, key_state="present", phase="setup"):
    """Check metadata snapshots, never claim installation scope or key custody."""
    plan_hash = approved_plan(plan)
    require(phase in {"setup", "canary", "publication"}, "invalid writer settings phase")
    require(phase == "setup" or key_state == "present", "canary/publication requires present key metadata")
    writer_identity(plan, repository, app)
    writer_inventory(plan, inventory, key_state, phase)
    require(isinstance(environment, dict) and environment.get("name") == ENVIRONMENT, "environment identity differs")
    require(environment.get("url") == ENVIRONMENT_URL, "environment repository identity is unproven")
    custom = capture(custom_rules, ENVIRONMENT_URL + "/deployment_protection_rules")
    require(type(custom.get("total_count")) is int and custom["total_count"] == 0 and
            custom.get("custom_deployment_protection_rules") == [],
            "deployment:false requires an explicit empty custom protection-rule inventory")
    expected = plan["environment"]
    same(environment.get("deployment_branch_policy"), expected["settings"]["deployment_branch_policy"],
         "environment must select only explicitly configured branches")
    require(environment.get("can_admins_bypass") is False, "administrator bypass disabled is unproven")
    protections = environment.get("protection_rules")
    require(isinstance(protections, list) and all(isinstance(x, dict) for x in protections), "environment protections missing")
    require(all(x.get("type") in {"required_reviewers", "wait_timer", "branch_policy"} for x in protections),
            "unexpected environment protection; review before applying the plan")
    require(len({x["type"] for x in protections}) == len(protections), "duplicate environment protection")
    reviewers = [x for x in protections if x.get("type") == "required_reviewers"]
    require(len(reviewers) == 1 and reviewers[0].get("prevent_self_review") is expected["settings"]["prevent_self_review"],
            "owner-only review must permit the owner to approve their manual request")
    entries = reviewers[0].get("reviewers")
    require(isinstance(entries, list) and len(entries) == 1 and isinstance(entries[0], dict) and entries[0].get("type") == "User" and
            isinstance(entries[0].get("reviewer"), dict) and type(entries[0]["reviewer"].get("id")) is int and
            entries[0]["reviewer"]["id"] == expected["settings"]["reviewers"][0]["id"], "environment reviewer must be the selected owner")
    require(all(type(x.get("wait_timer")) is int and x["wait_timer"] == expected["settings"]["wait_timer"]
                for x in protections if x.get("type") == "wait_timer"), "unexpected wait timer")
    branches = capture(branches, ENVIRONMENT_URL + "/deployment-branch-policies")
    require(type(branches.get("total_count")) is int and branches["total_count"] == 1 and
            isinstance(branches.get("branch_policies"), list) and len(branches["branch_policies"]) == 1,
            "complete single-branch policy readback is required")
    branch = branches["branch_policies"][0]
    require(isinstance(branch, dict) and type(branch.get("id")) is int and branch["id"] > 0, "branch readback missing")
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
        same(conditions(actual.get("conditions")), conditions(expected_rule["conditions"]),
             "ruleset history namespace differs from approved proposal")
        same({key: actual.get(key) for key in expected_rule if key not in {"rules", "conditions"}},
             {key: value for key, value in expected_rule.items() if key not in {"rules", "conditions"}},
             "ruleset differs from approved history namespace proposal")
        rules = actual.get("rules")
        require(isinstance(rules, list) and all(isinstance(x, dict) and isinstance(x.get("type"), str) for x in rules),
                "ruleset rules missing")
        # bare update is the observed GitHub shape; explicit false is equivalent
        normalized = []
        for rule in rules:
            if rule.get("type") == "update" and set(rule) == {"type", "parameters"}:
                same(rule["parameters"], {"update_allows_fetch_and_merge": False}, "update rule parameters differ")
                rule = {"type": "update"}
            normalized.append(rule)
        same(sorted(normalized, key=lambda x: x["type"]), sorted(expected_rule["rules"], key=lambda x: x["type"]),
             "ruleset rules differ from approved proposal")
    return dict(settings_match=True, live_enforcement_proven=False, key_custody_proven=False,
                installation_scope_proven=False, configured_secret_name_placement_verified=True, key_metadata_state=key_state,
                app_metadata_identity_verified=True, repository=REPOSITORY, ruleset_ids=ids, app_id=plan["app"]["id"],
                approved_plan_sha256=plan_hash, configured_installation_id=plan["app"]["installation_id"], phase=phase)


def check_writer_blobs(blobs, *, key_state="present", phase="setup"):
    """Validate and fingerprint the same bounded bytes supplied by the operator."""
    require(isinstance(blobs, dict) and set(blobs) == {"plan", *WRITER_CAPTURES} and
            all(isinstance(value, bytes) for value in blobs.values()), "complete raw trust captures required")
    parsed = {key: json_value(value, MAX_CAPTURE) for key, value in blobs.items()}
    result = check_writer(parsed["plan"], *(parsed[key] for key in WRITER_CAPTURES), key_state=key_state, phase=phase)
    manifest = dict(schema="rcp-history-trust-captures-v1", phase=phase, key_state=key_state,
                    capture_sha256={key: hashlib.sha256(value).hexdigest() for key, value in blobs.items()})
    preimage = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")
    return dict(result, trust_manifest=manifest, trust_sha256=hashlib.sha256(preimage).hexdigest())


def reader_policy(resource_arn, admission_role, collector_role, recovery_role):
    """Deny fragment attached to one exact secret; never grants any principal access."""
    resource = reader_resource_identity(resource_arn)
    require(resource is not None, "exact supported reader secret ARN required")
    principals = [admission_role, collector_role, recovery_role]
    require(all(isinstance(x, str) for x in principals) and len(set(principals)) == 3, "three distinct explicitly selected role ARNs required")
    for principal in principals:
        require((role := ROLE.fullmatch(principal)) and role[1] == resource[1],
                "roles must be exact IAM role ARNs in the secret account/partition")
    # this policy is attached only to the selected secret. '*' also covers a replica
    # receiving the same policy, whose regional ARN differs; neither statement allows.
    return {"Version": "2012-10-17", "Statement": [
        dict(Sid="DenyReadOutsideSelectedRoles", Effect="Deny", Principal="*",
             Action="secretsmanager:GetSecretValue", Resource="*",
             Condition={"ArnNotEquals": {"aws:PrincipalArn": principals}}),
        dict(Sid="DenyReaderNonCurrentStage", Effect="Deny", Principal="*",
             Action="secretsmanager:GetSecretValue", Resource="*",
             Condition={"ArnEquals": {"aws:PrincipalArn": principals[:2]},
                        "StringNotEquals": {"secretsmanager:VersionStage": "AWSCURRENT"}}),
    ]}


def reader_proposal(resource_arn, stack_id, stack_capture, recovery_role, *, phase="bootstrap"):
    resource = reader_resource_identity(resource_arn)
    match = STACK.fullmatch(stack_id) if isinstance(stack_id, str) else None
    require(resource is not None and match and match.groups()[:2] == resource, "stack and secret account/region differ")
    require(isinstance(stack_capture, dict) and set(stack_capture) == {"operation", "region", "parameters", "response"} and
            stack_capture["operation"] == "DescribeStacks" and stack_capture["region"] == resource[0] and
            stack_capture["parameters"] == {"StackName": stack_id}, "exact DescribeStacks capture required")
    response = stack_capture["response"]
    require(isinstance(response, dict) and isinstance(response.get("Stacks"), list) and len(response["Stacks"]) == 1 and
            not response.get("NextToken"), "complete single stack readback required")
    stack = response["Stacks"][0]
    require(isinstance(stack, dict) and stack.get("StackId") == stack_id and stack.get("StackName") == match[3] and
            stack.get("StackStatus") in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}, "stack identity or terminal state differs")
    def pairs(key, name, value):
        entries = stack.get(key)
        require(isinstance(entries, list) and all(isinstance(x, dict) and isinstance(x.get(name), str) and
                isinstance(x.get(value), str) for x in entries), "stack settings or outputs missing")
        result = {x[name]: x[value] for x in entries}
        require(len(result) == len(entries), "duplicate stack setting or output")
        return result
    parameters = pairs("Parameters", "ParameterKey", "ParameterValue")
    require(all(parameters.get(key) == "false" for key in ("RuntimeEnabled", "AdmissionEnabled", "PublicationEnabled")),
            "reader bootstrap and rebinding require disabled runtime, admission and publication")
    require(phase in {"bootstrap", "rebind"}, "invalid reader policy phase")
    config = {key: parameters.get(key) for key in ("GitHubAppClientId", "GitHubAppInstallationId", "GitHubAppSecretSuffix")}
    if phase == "bootstrap":
        require(all(value == "" for value in config.values()), "reader bootstrap requires empty App parameters before key import")
    else:
        writer_config(config["GitHubAppClientId"], config["GitHubAppInstallationId"])
        require(config["GitHubAppSecretSuffix"] == resource_arn[-6:], "configured reader secret differs from policy attachment")
    outputs = pairs("Outputs", "OutputKey", "OutputValue")
    require(recovery_role not in outputs.values(), "recovery role must not be a production stack role")
    require(all(key in outputs for key in ("AdmissionRoleArn", "CollectorRoleArn")), "reader role stack outputs missing")
    roles = [outputs["AdmissionRoleArn"], outputs["CollectorRoleArn"]]
    policy = reader_policy(resource_arn, *roles, recovery_role)
    return dict(schema="rcp-reader-secret-policy-v2", stack_id=stack_id,
                attachment=dict(secret_id=resource_arn, block_public_policy=True), resource_policy=policy,
                reader_roles=roles, recovery_role=recovery_role, phase=phase, key_absence_proven=False, live_acceptance_required=True)


def read(path):
    return json_value(read_file(path, MAX_CAPTURE), MAX_CAPTURE)


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ValidationError("invalid arguments; use --help for the supported non-secret inputs")


def emit(value, output):
    payload = json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if output is None:
        print(payload, end="")
        return
    # callers own directory prefixes; publish a complete file without replacement
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(prefix=".trust-proposal-", dir=output.parent, mode="w", encoding="utf-8", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, output, follow_symlinks=False)
        except FileExistsError:
            raise ValidationError("output already exists; choose a new proposal path") from None
    finally:
        if temporary is not None:
            try:
                temporary.unlink()
            except OSError:
                pass


def main():
    parser = Parser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("writer-plan", "writer-check"):
        command = commands.add_parser(name)
        if name == "writer-plan":
            for field in ("app-id", "client-id", "installation-id"):
                command.add_argument("--" + field, required=True)
        else:
            command.add_argument("--plan", required=True, type=Path)
            command.add_argument("--phase", choices=("setup", "canary", "publication"), default="setup")
            for field in WRITER_CAPTURES:
                command.add_argument("--" + field.replace("_", "-") + "-readback", required=True, type=Path)
            command.add_argument("--key-state", choices=("absent", "present"), default="present")
        command.add_argument("--output", type=Path)
    command = commands.add_parser("reader-policy")
    command.add_argument("--secret-arn", dest="resource_arn", required=True)
    command.add_argument("--stack-id", required=True)
    command.add_argument("--stack-readback", type=Path, required=True)
    command.add_argument("--recovery-role", required=True)
    command.add_argument("--phase", choices=("bootstrap", "rebind"), default="bootstrap")
    command.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.command == "reader-policy":
        value = reader_proposal(args.resource_arn, args.stack_id, read(args.stack_readback), args.recovery_role, phase=args.phase)
    elif args.command == "writer-plan":
        value = writer_plan(args.app_id, args.client_id, args.installation_id)
    else:
        paths = dict(plan=args.plan, **{field: getattr(args, field + "_readback") for field in WRITER_CAPTURES})
        blobs = {field: read_file(path, MAX_CAPTURE) for field, path in paths.items()}
        value = check_writer_blobs(blobs, key_state=args.key_state, phase=args.phase)
    emit(value, args.output)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        reason = str(error) if isinstance(error, ValidationError) else "invalid or incomplete non-secret inputs"
        print("Trust proposal rejected: " + reason, file=sys.stderr)
        raise SystemExit(1) from None
