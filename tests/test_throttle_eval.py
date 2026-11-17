"""verify_rule against the schema confirmed live on ObjectScale 4.4 (#16)."""
import copy, pathlib, sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from poc import throttle_eval as te

LIVE_ACTIONS = ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject",
                "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:CreateBucket",
                "s3:DeleteBucket", "s3:ListBucket", "s3:ListBucketVersions", "*"]


class Thr:
    def __init__(self, actions=LIVE_ACTIONS, fail=False):
        self.actions, self.fail = actions, fail

    def list_actions(self):
        if self.fail:
            raise ConnectionError("mgmt API down")
        return list(self.actions)


BASE = {"id": "demo-r", "rateUnit": "TPS", "isDisabled": False,
        "filters": {"principals": ["urn:ecs:iam::poc:user/tenant-noisy"],
                    "actions": ["s3:PutObject"]},
        "limit": {"value": 50}, "scoping": {"per": ["principal"]}}


def rule(**patch):
    r = copy.deepcopy(BASE)
    for k, v in patch.items():
        if k.startswith("f_"):
            if v is None:
                r["filters"].pop(k[2:], None)
            else:
                r["filters"][k[2:]] = v
        else:
            r[k] = v
    return r


def problems(r, thr=None):
    return te.verify_rule(thr or Thr(), r)["problems"]


PATHS = {"matcher": "wildcard", "values": ["poc/poc-throttle/*"]}
IPS = {"matcher": "cidr4", "values": ["192.168.1.0/24"]}

ACCEPTED = {
    "baseline": rule(),
    "TPM": rule(rateUnit="TPM"),
    "paths wildcard": rule(f_paths=PATHS),
    "paths exact": rule(f_paths={"matcher": "exact", "values": ["poc/b"]}),
    "exclude_paths with paths": rule(f_paths=PATHS, f_exclude_paths={
        "matcher": "exact", "values": ["poc/admin"]}),
    "ips exact": rule(f_ips={"matcher": "exact", "values": ["10.0.0.1"]}),
    "ips wildcard4": rule(f_ips={"matcher": "wildcard4", "values": ["10.0.0.*"]}),
    "ips cidr4": rule(f_ips=IPS),
    "exclude_ips with ips": rule(f_ips=IPS, f_exclude_ips={
        "matcher": "exact", "values": ["192.168.1.9"]}),
    "verbs only": rule(f_actions=None, f_verbs=["PUT"]),
    "wildcard action": rule(f_actions=["*"]),
    "scoping per path": rule(f_paths=PATHS, scoping={"per": ["principal", "path"]}),
    "empty scoping": rule(scoping={"per": []}),
    "verb OPTIONS": rule(f_actions=None, f_verbs=["OPTIONS"]),
    "neither verbs nor actions": rule(f_actions=None),
    "actions only, unscoped": rule(f_principals=None, scoping={"per": []}),
}

REJECTED = {
    "rateUnit BPS (400 live)": (rule(rateUnit="BPS"), "rateUnit"),
    "rateUnit bogus": (rule(rateUnit="PPS"), "rateUnit"),
    "filters.size (400 live)": (rule(f_size={"min": 1}), "unknown filter"),
    "paths matcher prefix (400 live)": (rule(f_paths={
        "matcher": "prefix", "values": ["poc/b/"]}), "paths.matcher"),
    "paths without values": (rule(f_paths={"matcher": "exact", "values": []}),
                             "paths.values"),
    "ips matcher prefix": (rule(f_ips={"matcher": "prefix", "values": ["10."]}),
                           "ips.matcher"),
    "ips with a path matcher": (rule(f_ips={"matcher": "wildcard",
                                            "values": ["10.*"]}), "ips.matcher"),
    "exclude_paths alone": (rule(f_exclude_paths=PATHS), "requires a paths"),
    "exclude_ips alone": (rule(f_exclude_ips=IPS), "requires an ips"),
    "verbs and actions": (rule(f_verbs=["PUT"]), "not both"),
    "unknown verb": (rule(f_actions=None, f_verbs=["PATCH"]), "unknown verbs"),
    "action not offered live": (rule(f_actions=["s3:Bogus"]), "not offered"),
    "scoping bogus": (rule(scoping={"per": ["tenant"]}), "scoping.per"),
    "scoping principal w/o principals": (rule(f_principals=None),
                                         "requires a principals"),
    "scoping path w/o paths": (rule(scoping={"per": ["path"]}),
                               "requires a paths"),
    "scoping verb w/o verbs": (rule(scoping={"per": ["verb"]}),
                               "requires a verbs"),
    "unknown top-level": (rule(bogus=1), "unknown top-level"),
    "missing id": (rule(id=""), "missing rule id"),
    "limit negative (400 live)": (rule(limit={"value": -1}), "limit.value"),
    "limit not an integer": (rule(limit={"value": 2.5}), "limit.value"),
    "limit missing": (rule(limit={}), "limit.value"),
    "empty filters (400 live)": (rule(filters={}, scoping={"per": []}),
                                 "no valid filters"),
    "scoping action w/o actions": (rule(f_actions=None, f_verbs=["PUT"],
                                        scoping={"per": ["action"]}),
                                   "requires a actions"),
    "scoping ip w/o ips": (rule(scoping={"per": ["ip"]}), "requires a ips"),
}


@pytest.mark.parametrize("name", ACCEPTED)
def test_accepts(name):
    assert problems(ACCEPTED[name]) == []


@pytest.mark.parametrize("name", REJECTED)
def test_rejects(name):
    r, needle = REJECTED[name]
    out = te.verify_rule(Thr(), r)
    assert not out["ok"]
    assert any(needle in p for p in out["problems"]), out["problems"]


def test_action_is_checked_against_the_live_list_not_a_constant():
    # same rule, different server: the verdict follows what the server offers
    r = rule(f_actions=["s3:PutObject"])
    assert problems(r, Thr(actions=["s3:PutObject", "*"])) == []
    assert any("not offered" in p
               for p in problems(r, Thr(actions=["s3:GetObject"])))


def test_unreachable_action_list_is_a_problem_not_a_pass():
    out = te.verify_rule(Thr(fail=True), rule())
    assert not out["ok"]
    assert any("could not be checked" in p for p in out["problems"])


def test_verbs_only_rule_needs_no_action_lookup():
    assert problems(rule(f_actions=None, f_verbs=["GET", "HEAD"]),
                    Thr(fail=True)) == []


def test_limit_zero_is_accepted_like_the_server_but_flagged():
    out = te.verify_rule(Thr(), rule(limit={"value": 0}))
    assert out["ok"] and any("not tested" in w for w in out["warnings"])
