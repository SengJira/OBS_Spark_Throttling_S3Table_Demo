"""Shared throttler rule definitions for the PoC.

Both ``99_reset`` (staging) and notebook 02 (verification/enforcement) build
the same two rules from here so the staged rules and the demo rules can never
drift apart. The rules are run-id agnostic: ``99_reset`` stages them minutes
before a notebook run whose ``run_id`` is not known yet, so the prefix rule
matches ``demo-*/raw/*`` (the wildcard spans ``/``). Verification stays in
``poc.throttle_eval.verify_rule``.
"""
from poc import config

NOISY_RULE_ID = "demo-noisy-put"
PREFIX_RULE_ID = "demo-raw-prefix"
DEFAULT_NOISY_LIMIT = 50
DEFAULT_PREFIX_LIMIT = 10
RAW_GLOB = "demo-*/raw/*"


def noisy_principal():
    return f"urn:ecs:iam::{config.OBS_NAMESPACE}:user/tenant-noisy"


def noisy_put_rule(limit=DEFAULT_NOISY_LIMIT):
    return {"id": NOISY_RULE_ID, "rateUnit": "TPS", "isDisabled": False,
            "filters": {"principals": [noisy_principal()],
                        "actions": ["s3:PutObject"]},
            "limit": {"value": limit}, "scoping": {"per": ["principal"]}}


def prefix_put_rule(limit=DEFAULT_PREFIX_LIMIT):
    return {"id": PREFIX_RULE_ID, "rateUnit": "TPS", "isDisabled": False,
            "filters": {"principals": [noisy_principal()],
                        "actions": ["s3:PutObject"],
                        "paths": {"matcher": "wildcard", "values": [
                            f"{config.OBS_NAMESPACE}/{config.BUCKET_THROTTLE}/{RAW_GLOB}"]}},
            "limit": {"value": limit}, "scoping": {"per": ["principal"]}}


def demo_rules(noisy_limit=DEFAULT_NOISY_LIMIT,
               prefix_limit=DEFAULT_PREFIX_LIMIT):
    return [noisy_put_rule(noisy_limit), prefix_put_rule(prefix_limit)]


def is_demo_rule(rule):
    return str((rule or {}).get("id", "")).startswith("demo-")
