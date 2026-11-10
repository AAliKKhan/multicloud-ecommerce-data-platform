"""Static safety checks on the Terraform sources (no Terraform binary, no cloud).

These guard the properties the spec cares about: no secrets, no long-lived keys,
private encrypted buckets, least privilege, nothing billable by default.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

TF_ROOT = Path(__file__).resolve().parents[3] / "infra" / "terraform"
AWS = TF_ROOT / "aws"
GCP = TF_ROOT / "gcp"


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def all_tf() -> dict[Path, str]:
    return {p: read(p) for p in TF_ROOT.rglob("*.tf")}


def variable_block(text: str, name: str) -> str:
    match = re.search(rf'variable "{name}" \{{(.*?)\n\}}', text, re.DOTALL)
    assert match, f"variable {name} not found"
    return match.group(1)


def test_both_stacks_and_lock_files_exist() -> None:
    for stack in (AWS, GCP):
        for name in ("versions.tf", "variables.tf", "main.tf", "outputs.tf", ".terraform.lock.hcl"):
            assert (stack / name).is_file(), f"{stack.name}/{name} missing"


def test_no_credentials_or_long_lived_keys_in_terraform() -> None:
    forbidden = [
        r"\baccess_key\b\s*=",
        r"\bsecret_key\b\s*=",
        r"\bprivate_key\b\s*=",
        r"\bcredentials\s*=",
        r'resource\s+"aws_iam_access_key"',
        r'resource\s+"aws_iam_user"',
        r'resource\s+"google_service_account_key"',
        r"AKIA[0-9A-Z]{16}",
        r"-----BEGIN",
    ]
    for path, text in all_tf().items():
        for pattern in forbidden:
            assert not re.search(pattern, text), f"{path.name} matches {pattern}"


def test_no_real_tfvars_or_state_are_committed() -> None:
    files = {p.name for p in TF_ROOT.rglob("*") if p.is_file()}
    assert not any(f.endswith(".tfvars") for f in files)
    assert not any(f.endswith(".tfstate") or ".tfstate." in f for f in files)
    for path in TF_ROOT.rglob("*.example"):
        text = read(path)
        assert "-----BEGIN" not in text and not re.search(r"AKIA[0-9A-Z]{16}", text)


def test_aws_bucket_is_private_encrypted_and_acl_free() -> None:
    text = read(AWS / "main.tf")
    for flag in (
        "block_public_acls",
        "block_public_policy",
        "ignore_public_acls",
        "restrict_public_buckets",
    ):
        assert re.search(rf"{flag}\s*=\s*true", text), flag
    assert "BucketOwnerEnforced" in text
    assert "aws_s3_bucket_server_side_encryption_configuration" in text
    assert "aws:SecureTransport" in text  # TLS-only policy
    assert re.search(r'acl\s*=\s*"public', text) is None


def test_aws_iam_is_least_privilege() -> None:
    text = read(AWS / "main.tf")
    assert "s3:DeleteObject" not in text  # overwrites only need PutObject
    # A wildcard *principal* is legitimate in the TLS-only Deny; wildcard actions/resources are not.
    principals = r'(type\s*=\s*"\*"|identifiers\s*=\s*\["\*"\])'
    assert '"*"' not in re.sub(principals, "", text)
    wildcard_actions = [m.start() for m in re.finditer(r'"s3:\*"', text)]
    assert len(wildcard_actions) == 1
    assert '"Deny"' in text[max(0, wildcard_actions[0] - 200) : wildcard_actions[0]]
    assert "s3:prefix" in text  # listing is restricted to known prefixes


def test_gcp_bucket_is_private_and_uniform() -> None:
    text = read(GCP / "main.tf")
    assert re.search(r"uniform_bucket_level_access\s*=\s*true", text)
    assert re.search(r'public_access_prevention\s*=\s*"enforced"', text)
    assert "google_storage_bucket_iam_member" in text
    assert (
        "google_storage_bucket_iam_binding" not in text
    )  # members are additive, not authoritative
    assert "allUsers" not in text and "allAuthenticatedUsers" not in text


def test_gcp_roles_are_narrow() -> None:
    text = read(GCP / "main.tf")
    roles = set(re.findall(r'role\s*=\s*"(roles/[^"]+)"', text))
    assert roles == {
        "roles/storage.objectUser",
        "roles/storage.objectViewer",
        "roles/bigquery.dataEditor",
        "roles/bigquery.dataViewer",
        "roles/bigquery.jobUser",
    }
    assert "roles/owner" not in text and "roles/editor" not in text


@pytest.mark.parametrize("stack", [AWS, GCP], ids=["aws", "gcp"])
def test_nothing_billable_or_destructive_by_default(stack: Path) -> None:
    variables = read(stack / "variables.tf")
    # Required inputs have no default, so `apply` can never silently proceed.
    for required in ("environment", "raw_bucket_name"):
        assert "default" not in variable_block(variables, required), required
    assert re.search(r"default\s*=\s*false", variable_block(variables, "force_destroy"))
    assert re.search(r"default\s*=\s*0", variable_block(variables, "monthly_budget_usd"))
    env = variable_block(variables, "environment")
    assert '"dev", "prod-like"' in env and '"local"' not in env  # local mode needs no Terraform


def test_gcp_never_disables_apis_and_has_no_key_resources() -> None:
    text = read(GCP / "main.tf")
    assert "disable_on_destroy = false" in text
    assert "google_service_account_key" not in text


def test_glue_uses_partition_projection_not_a_crawler() -> None:
    text = read(AWS / "main.tf")
    assert "aws_glue_crawler" not in text
    assert '"projection.enabled"' in text


def test_example_files_cover_both_environments() -> None:
    for stack in (AWS, GCP):
        assert (stack / "dev.tfvars.example").is_file()
        assert (stack / "prod-like.tfvars.example").is_file()
        assert (stack / "backend.tf.example").is_file()
