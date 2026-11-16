"""Shared throttler rule definitions for the PoC.

Both ``99_reset`` (staging) and notebook 02 (verification/enforcement) build
the same two rules from here so the staged rules and the demo rules can never
drift apart. Verification stays in ``poc.throttle_eval.verify_rule``.
"""
from poc import config

NOISY_RULE_ID = "demo-noisy-put"
PREFIX_RULE_ID = "demo-raw-prefix"
DEFAULT_NOISY_LIMIT = 50
DEFAULT_PREFIX_LIMIT = 10
RAW_PREFIX = "raw"


def noisy_principal():
    return f"urn:ecs:iam::{config.OBS_NAMESPACE}:user/tenant-noisy"


def noisy_put_rule(run_id, limit=DEFAULT_NOISY_LIMIT):
    return {"id": NOISY_RULE_ID, "rateUnit": "TPS", "isDisabled": False,
            "filters": {"principals": [noisy_principal()],
                        "actions": ["s3:PutObject"]},
            "limit": {"value": limit}, "scoping": {"per": ["principal"]}}


def prefix_put_rule(run_id, limit=DEFAULT_PREFIX_LIMIT):
    covered = f"{run_id}/{RAW_PREFIX}"
    return {"id": PREFIX_RULE_ID, "rateUnit": "TPS", "isDisabled": False,
            "filters": {"principals": [noisy_principal()],
                        "actions": ["s3:PutObject"],
                        "paths": {"matcher": "wildcard", "values": [
                            f"{config.OBS_NAMESPACE}/{config.BUCKET_THROTTLE}/{covered}/*"]}},
            "limit": {"value": limit}, "scoping": {"per": ["principal"]}}


def demo_rules(run_id, noisy_limit=DEFAULT_NOISY_LIMIT,
               prefix_limit=DEFAULT_PREFIX_LIMIT):
    return [noisy_put_rule(run_id, noisy_limit),
            prefix_put_rule(run_id, prefix_limit)]


def is_demo_rule(rule):
    return str((rule or {}).get("id", "")).startswith("demo-")
