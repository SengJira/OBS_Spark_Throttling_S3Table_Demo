#!/usr/bin/env python3
"""Phase 2 - idempotent ObjectScale prep.

Creates: namespace `poc`, IAM users + access keys (poc-admin, tenant-noisy,
tenant-critical, tables-rw, tables-ro), attaches managed policies, creates S3
buckets (poc-spark-data, poc-throttle) and the S3 Tables table bucket
(poc-lakehouse). Writes keys to /root/.poc/credentials (0600) and updates .env.

Usage: OBS_ROOT_PASSWORD=xxx python3 scripts/prep_obs.py
(root password is read from env only — never written to files)
"""
import json, os, re, sys, xml.etree.ElementTree as ET

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from poc import config
from poc.obs_admin import MgmtApi, Throttler
from poc import s3util

VPOOL = "urn:storageos:ReplicationGroupInfo:90e1845b-f2af-4257-853e-b73a6142cc8b:global"
CREDS_PATH = "/root/.poc/credentials"
ENV_PATH = config.ROOT / ".env"

IAM_USERS = ["poc-admin", "tenant-noisy", "tenant-critical", "tables-rw", "tables-ro"]
POLICY_ATTACH = {
    # tables users need both the s3tables control-plane policy AND an
    # S3 data-plane policy — OBS stores table data in real --table-s3 buckets
    # authorized through normal S3 permissions.
    "tables-rw": ["urn:ecs:iam:::policy/S3TablesFullAccess",
                  "urn:ecs:iam:::policy/ECSS3FullAccess"],
    "tables-ro": ["urn:ecs:iam:::policy/S3TablesReadOnlyAccess",
                  "urn:ecs:iam:::policy/ECSS3ReadOnlyAccess"],
    "poc-admin": ["urn:ecs:iam:::policy/ECSS3FullAccess"],
    "tenant-noisy": ["urn:ecs:iam:::policy/ECSS3FullAccess"],
    "tenant-critical": ["urn:ecs:iam:::policy/ECSS3FullAccess"],
}
ENV_KEY = {
    "poc-admin": ("POC_ADMIN_AK", "POC_ADMIN_SK"),
    "tenant-noisy": ("NOISY_AK", "NOISY_SK"),
    "tenant-critical": ("CRITICAL_AK", "CRITICAL_SK"),
    "tables-rw": ("TABLES_RW_AK", "TABLES_RW_SK"),
    "tables-ro": ("TABLES_RO_AK", "TABLES_RO_SK"),
}


def iam_users(api):
    r = api.iam("ListUsers", MaxItems=400)
    names = re.findall(r"<UserName>([^<]+)</UserName>", r.text)
    return names


def iam_call_ok(api, action, **params):
    r = api.iam(action, **params)
    if r.status_code != 200 or "<Error>" in r.text:
        code = re.search(r"<Code>([^<]+)</Code>", r.text)
        return False, code.group(1) if code else r.text[:200]
    return True, r.text


def ensure_namespace(api):
    r = api.get("/object/namespaces/namespace/" + config.OBS_NAMESPACE)
    if r.status_code == 200:
        print(f"namespace {config.OBS_NAMESPACE}: exists")
        return
    body = {"namespace": config.OBS_NAMESPACE,
            "default_data_services_vpool": VPOOL,
            "is_stale_allowed": False}
    r = api.post("/object/namespaces/namespace", body=body)
    if r.status_code in (200, 201):
        print(f"namespace {config.OBS_NAMESPACE}: created")
        return
    raise SystemExit(f"create namespace failed: {r.status_code} {r.text[:400]}")


def ensure_user(api, name):
    existing = iam_users(api)
    if name in existing:
        print(f"  iam user {name}: exists")
    else:
        ok, info = iam_call_ok(api, "CreateUser", UserName=name)
        if not ok:
            raise SystemExit(f"CreateUser {name} failed: {info}")
        print(f"  iam user {name}: created")
    # access key: reuse stored secret if we already have it, else mint a new key
    eak, esk = ENV_KEY[name]
    r = api.iam("ListAccessKeys", UserName=name)
    keys = re.findall(r"<AccessKeyId>([^<]+)</AccessKeyId>", r.text)
    if keys:
        old = config.load_credentials()
        if old.get(eak) == keys[0] and old.get(esk):
            print(f"  iam user {name}: reusing stored key {config.mask(keys[0])}")
            return keys[0], old[esk]
        # create a second key and return it, then clean up the old one
        r2 = api.iam("CreateAccessKey", UserName=name)
        ak2 = re.search(r"<AccessKeyId>([^<]+)</AccessKeyId>", r2.text)
        sk2 = re.search(r"<SecretAccessKey>([^<]+)</SecretAccessKey>", r2.text)
        if ak2 and sk2:
            api.iam("DeleteAccessKey", UserName=name, AccessKeyId=keys[0])
            print(f"  iam user {name}: rotated access key {config.mask(ak2.group(1))}")
            return ak2.group(1), sk2.group(1)
    r = api.iam("CreateAccessKey", UserName=name)
    ak = re.search(r"<AccessKeyId>([^<]+)</AccessKeyId>", r.text)
    sk = re.search(r"<SecretAccessKey>([^<]+)</SecretAccessKey>", r.text)
    if not (ak and sk):
        raise SystemExit(f"CreateAccessKey {name} failed: {r.text[:400]}")
    print(f"  iam user {name}: new access key {config.mask(ak.group(1))}")
    return ak.group(1), sk.group(1)


def attach_policy(api, user, arn):
    ok, info = iam_call_ok(api, "AttachUserPolicy", UserName=user, PolicyArn=arn)
    if ok:
        print(f"  attach {arn.rsplit('/',1)[-1]} -> {user}: ok")
    else:
        print(f"  attach {arn.rsplit('/',1)[-1]} -> {user}: {info}")


def ensure_bucket(api, name):
    cli = s3util.s3("admin")
    if s3util.bucket_exists(cli, name):
        print(f"  bucket {name}: exists")
        return
    try:
        cli.create_bucket(Bucket=name)
        print(f"  bucket {name}: created via S3")
    except Exception as e:
        raise SystemExit(f"create bucket {name} failed: {e}")


def write_credentials(keys):
    lines = ["# ObjectScale PoC IAM credentials - generated by prep_obs.py",
             "# DO NOT COMMIT. chmod 600."]
    for user, (ak, sk) in keys.items():
        eak, esk = ENV_KEY[user]
        lines += [f"{eak}={ak}", f"{esk}={sk}"]
    with open(CREDS_PATH, "w") as f:
        f.write("\n".join(lines) + "\n")
    os.chmod(CREDS_PATH, 0o600)
    print(f"wrote {CREDS_PATH}")


def update_env(keys):
    text = ENV_PATH.read_text()
    for user, (ak, sk) in keys.items():
        eak, esk = ENV_KEY[user]
        text = re.sub(rf"^{eak}=.*$", f"{eak}={ak}", text, flags=re.M)
        text = re.sub(rf"^{esk}=.*$", f"{esk}={sk}", text, flags=re.M)
    ENV_PATH.write_text(text)
    os.chmod(ENV_PATH, 0o600)
    print(f"updated {ENV_PATH}")


def main():
    api = MgmtApi()
    api.login()
    print("mgmt login ok; version check:")
    print("  ObjectScale:", api.version() or "?")

    print("== namespace ==")
    ensure_namespace(api)

    print("== IAM users + keys ==")
    keys = {}
    # reuse existing secret material from creds file where possible
    old = config.load_credentials()
    for u in IAM_USERS:
        ak, sk = ensure_user(api, u)
        eak, esk = ENV_KEY[u]
        if sk is None:
            ak, sk = old.get(eak, ak), old.get(esk, "")
        keys[u] = (ak, sk)

    print("== managed policy attach ==")
    for u, arns in POLICY_ATTACH.items():
        for arn in arns:
            attach_policy(api, u, arn)

    write_credentials(keys)
    update_env(keys)

    print("== S3 buckets ==")
    for b in (config.BUCKET_DATA, config.BUCKET_THROTTLE):
        ensure_bucket(api, b)
    # prefixes
    cli = s3util.s3("admin")
    for p in ("raw/", "curated/"):
        cli.put_object(Bucket=config.BUCKET_THROTTLE, Key=p, Body=b"")
    print("  prefixes raw/ curated/ ensured")

    print("== S3 Tables table bucket ==")
    try:
        st = s3util.s3tables("tables-rw")
        existing = st.list_table_buckets().get("tableBuckets", [])
        names = [t.get("name") for t in existing]
        if config.TABLE_BUCKET in names:
            print(f"  table bucket {config.TABLE_BUCKET}: exists")
        else:
            resp = st.create_table_bucket(name=config.TABLE_BUCKET)
            print(f"  table bucket {config.TABLE_BUCKET}: created -> {resp.get('arn', resp)}")
    except Exception as e:
        print(f"  WARNING s3tables create_table_bucket failed: {e}")
        print("  -> list as manual step for operator")

    print("== throttler backup ==")
    t = Throttler(api)
    snap = t.backup(config.ROOT / "reports" / "throttle_backup.json")
    print(f"  backed up {len(snap['config']['throttle']['rules'])} rules, "
          f"mode={snap['mode']}, code={snap['response_code']}")

    print("DONE")


if __name__ == "__main__":
    main()
