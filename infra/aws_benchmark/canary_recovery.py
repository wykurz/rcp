"""Read and bind an approved failed canary artifact before credential minting."""

import json
import os
from pathlib import Path
import re
import urllib.request

from .common import ValidationError, require
from .history_contract import CANARY_WORKFLOW, OWNER_ID, REPOSITORY, REPOSITORY_ID, identifier
from .publication_identity import canary_identity
from .results import json_value

API = "https://api.github.com/repos/" + REPOSITORY
LIMIT = 1024 * 1024


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


def validate(run, artifact, run_id, artifact_id, revision):
    require(isinstance(run, dict) and isinstance(artifact, dict), "canary recovery metadata is not an object")
    repository = dict(id=int(REPOSITORY_ID), full_name=REPOSITORY)
    for field in ("repository", "head_repository"):
        require(isinstance(run.get(field), dict) and all(run[field].get(k) == v for k, v in repository.items()),
                "source run repository differs")
    require(type(run.get("id")) is int and run["id"] == run_id and run.get("path") in (".github/workflows/" + CANARY_WORKFLOW, ".github/workflows/" + CANARY_WORKFLOW + "@main") and
            run.get("event") == "workflow_dispatch" and run.get("head_branch") == "main" and run.get("head_sha") == revision and
            run.get("run_attempt") == 1 and run.get("actor", {}).get("id") == OWNER_ID and
            run.get("status") == "completed" and run.get("conclusion") in ("failure", "cancelled", "timed_out"),
            "source must be a failed owner canary at this exact reviewed main revision")
    require(type(artifact.get("id")) is int and artifact["id"] == artifact_id and artifact.get("expired") is False and
            artifact.get("name") == "aws-history-canary-" + str(run_id) + "-1" and
            type(artifact.get("size_in_bytes")) is int and 0 < artifact["size_in_bytes"] <= LIMIT,
            "source artifact identity, retention or size differs")
    expected = dict(id=run_id, repository_id=int(REPOSITORY_ID), head_repository_id=int(REPOSITORY_ID),
        head_branch="main", head_sha=revision)
    require(isinstance(artifact.get("workflow_run"), dict) and
            all(artifact["workflow_run"].get(key) == value for key, value in expected.items()), "source artifact run binding differs")
    return dict(schema="rcp-history-canary-recovery-source-v1", run_id=run_id, artifact_id=artifact_id,
        revision=revision, path=run["path"], conclusion=run["conclusion"], artifact_name=artifact["name"],
        artifact_digest=artifact.get("digest"), bytes=artifact["size_in_bytes"])


def fetch(path, token):
    request = urllib.request.Request(API + path, headers={"Authorization": "Bearer " + token,
        "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2026-03-10", "Connection": "close"})
    with urllib.request.build_opener(NoRedirect()).open(request, timeout=20) as response:
        require(response.status == 200, "canary recovery metadata unavailable")
        return json_value(response.read(LIMIT + 1), LIMIT)


def main():
    revision = canary_identity(os.environ)
    run_id = identifier(os.environ.get("SOURCE_RUN_ID", ""))
    artifact_id = identifier(os.environ.get("SOURCE_ARTIFACT_ID", ""))
    require(str(run_id) != os.environ.get("GITHUB_RUN_ID"), "source must be an earlier canary run")
    require(re.fullmatch(r"[0-9a-f]{64}", os.environ.get("SOURCE_SHA256", "")), "approved source receipt hash required")
    token = os.environ.get("GH_TOKEN", "")
    require(token, "read-only Actions metadata credential missing")
    run = fetch("/actions/runs/" + str(run_id), token)
    artifact = fetch("/actions/artifacts/" + str(artifact_id), token)
    receipt = validate(run, artifact, run_id, artifact_id, revision)
    receipt["receipt_sha256"] = os.environ["SOURCE_SHA256"]
    with (Path(os.environ["RUNNER_TEMP"]) / "canary-source-metadata.json").open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, sort_keys=True)
        stream.write("\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        reason = str(error) if isinstance(error, ValidationError) else "source metadata unavailable"
        raise SystemExit("Canary recovery held: " + reason) from None
