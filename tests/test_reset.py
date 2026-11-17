"""poc.reset with in-memory S3 / S3 Tables fakes: only PoC-owned data goes."""
import pathlib, sys

import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from poc import reset, s3util, config

TB = "poc-lakehouse"
OURS = "11111111-aaaa--table-s3"
THEIRS = "xyz--table-s3"


def err(code, op="Op"):
    return ClientError({"Error": {"Code": code},
                        "ResponseMetadata": {"HTTPStatusCode": 409}}, op)


class Pager:
    def __init__(self, fn):
        self.fn = fn

    def paginate(self, Bucket):
        yield from self.fn(Bucket)


class FakeS3:
    def __init__(self, buckets, versioned=(), emptying=()):
        self.buckets = {b: dict(keys) for b, keys in buckets.items()}
        self.versioned = set(versioned)
        self.emptying = set(emptying)
        self.delete_calls = []
        self.single_deletes = 0

    def list_buckets(self):
        return {"Buckets": [{"Name": b} for b in self.buckets]}

    def get_bucket_versioning(self, Bucket):
        return {"Status": "Enabled"} if Bucket in self.versioned else {}

    def get_paginator(self, op):
        def v2(bucket):
            if bucket not in self.buckets:
                raise err("NoSuchBucket")
            keys = sorted({k for k, _ in self.buckets[bucket]})
            for i in range(0, max(len(keys), 1), 700):
                yield {"Contents": [{"Key": k} for k in keys[i:i + 700]]}

        def versions(bucket):
            items = sorted(self.buckets[bucket])
            yield {"Versions": [{"Key": k, "VersionId": v} for k, v in items
                                if not v.startswith("dm")],
                   "DeleteMarkers": [{"Key": k, "VersionId": v}
                                     for k, v in items if v.startswith("dm")]}
        return Pager(v2 if op == "list_objects_v2" else versions)

    def delete_objects(self, Bucket, Delete):
        if Bucket in self.emptying:
            raise err("EmptyBucketInProgress")
        objs = Delete["Objects"]
        assert len(objs) <= 1000
        self.delete_calls.append((Bucket, len(objs)))
        for o in objs:
            if "VersionId" in o:
                self.buckets[Bucket].pop((o["Key"], o["VersionId"]), None)
            else:
                for kv in [kv for kv in self.buckets[Bucket] if kv[0] == o["Key"]]:
                    self.buckets[Bucket].pop(kv)
        return {}

    def delete_object(self, **kw):
        self.single_deletes += 1

    def delete_bucket(self, Bucket):
        if Bucket in self.emptying:
            raise err("EmptyBucketInProgress")
        if Bucket not in self.buckets:
            raise err("NoSuchBucket")
        assert not self.buckets[Bucket], "bucket not empty"
        del self.buckets[Bucket]

    def create_bucket(self, Bucket):
        self.buckets.setdefault(Bucket, {})

    def put_object(self, Bucket, Key, Body=b""):
        self.buckets[Bucket][(Key, "null")] = Body


class FakeTables:
    def __init__(self, tables):
        self.tables = dict(tables)          # {name: data bucket}
        self.bucket_exists = True
        self.namespaces = ["pocns"]

    def list_namespaces(self, tableBucketARN):
        return {"namespaces": [{"namespace": [n]} for n in self.namespaces]}

    def list_tables(self, tableBucketARN, namespace):
        return {"tables": [{"name": t} for t in self.tables]}

    def get_table(self, tableBucketARN, namespace, name):
        bucket = self.tables[name]
        return {"warehouseLocation": f"s3://{bucket}",
                "tableARN": f"{tableBucketARN}/table/"
                            f"{bucket[:-len('--table-s3')]}"}

    def delete_table(self, tableBucketARN, namespace, name):
        del self.tables[name]

    def delete_namespace(self, tableBucketARN, namespace):
        self.namespaces.remove(namespace)

    def delete_table_bucket(self, tableBucketARN):
        self.bucket_exists = False

    def create_table_bucket(self, name):
        self.bucket_exists = True

    def create_namespace(self, tableBucketARN, namespace):
        self.namespaces.append(namespace[0])


def objs(*keys):
    return {(k, "null"): b"" for k in keys}


@pytest.fixture
def lab(tmp_path, monkeypatch):
    s3 = FakeS3({
        config.OBS_BUCKET: objs("demo-1/a", "demo-1/b", "raw/", "prod/keep"),
        config.OBS_BUCKET_THROTTLE: objs("demo-2/x", "customer/keep"),
        OURS: objs("data/f1", "metadata/m1"),
        THEIRS: objs("data/other-team"),
    })
    st = FakeTables({"orders_ice": OURS})
    monkeypatch.setattr(s3util, "s3", lambda *a, **k: s3)
    monkeypatch.setattr(s3util, "s3tables", lambda *a, **k: st)
    monkeypatch.setattr(reset, "LEDGER", tmp_path / "ledger.json")
    monkeypatch.setattr(reset, "restore_throttler", lambda backup=None: "restored")
    monkeypatch.setattr(reset, "stage_rules", lambda: "staged")
    monkeypatch.setattr(reset.time, "sleep", lambda s: None)
    return s3, st


def test_unrelated_table_s3_bucket_is_left_alone(lab):
    s3, st = lab
    out = reset.full_reset(wait_s=0)
    assert THEIRS in s3.buckets and s3.buckets[THEIRS]      # untouched
    assert OURS not in s3.buckets                           # ours removed
    assert out["unattributed_left_alone"] == [THEIRS]
    assert out["data_buckets_deleted"] == {OURS: "deleted"}
    assert out["tables_removed"] == ["pocns.orders_ice"]


def test_only_demo_prefixed_objects_removed_from_ordinary_buckets(lab):
    s3, _ = lab
    reset.full_reset(wait_s=0)
    data = {k for k, _ in s3.buckets[config.OBS_BUCKET]}
    assert "prod/keep" in data and not any(k.startswith("demo-") for k in data)
    assert ("customer/keep", "null") in s3.buckets[config.OBS_BUCKET_THROTTLE]


def test_dry_run_changes_nothing_and_lists_the_plan(lab):
    s3, st = lab
    before = {b: dict(k) for b, k in s3.buckets.items()}
    plan = reset.full_reset(dry_run=True)
    assert s3.buckets == before and st.tables == {"orders_ice": OURS}
    assert st.bucket_exists and not s3.delete_calls
    assert plan["dry_run"] and plan["objects_removed"] == {
        config.OBS_BUCKET: 2, config.OBS_BUCKET_THROTTLE: 1}
    assert plan["data_buckets_deleted"] == {OURS: "would delete"}
    assert plan["unattributed_left_alone"] == [THEIRS]
    assert plan["tables_removed"] == ["pocns.orders_ice"]


def test_deletes_are_batched_not_per_object(lab):
    s3, _ = lab
    s3.buckets[config.OBS_BUCKET].update(objs(*[f"demo-9/k{i}" for i in range(2500)]))
    reset.clean_ordinary_buckets()
    sizes = [n for b, n in s3.delete_calls if b == config.OBS_BUCKET]
    assert sum(sizes) == 2502 and max(sizes) <= 1000 and len(sizes) == 3
    assert s3.single_deletes == 0


def test_versioned_bucket_removes_all_versions_and_delete_markers(lab):
    s3, _ = lab
    s3.versioned.add(config.OBS_BUCKET)
    s3.buckets[config.OBS_BUCKET] = {
        ("demo-1/a", "v1"): b"", ("demo-1/a", "v2"): b"",
        ("demo-1/a", "dm3"): b"", ("prod/keep", "v1"): b""}
    reset.clean_ordinary_buckets()
    assert s3.buckets[config.OBS_BUCKET] == {("prod/keep", "v1"): b""}


def test_pending_server_side_empty_is_reported_and_retried_next_reset(lab):
    s3, st = lab
    s3.emptying.add(OURS)
    out = reset.full_reset(wait_s=0)
    assert out["data_buckets_deleted"][OURS] == reset.PENDING
    assert OURS in s3.buckets
    s3.emptying.clear()                 # table is gone now; only the ledger
    out = reset.full_reset(wait_s=0)    # still knows the bucket is ours
    assert out["data_buckets_deleted"] == {OURS: "deleted"}
    assert THEIRS in s3.buckets


def test_location_not_matching_table_id_is_not_deleted(lab):
    s3, st = lab
    st.get_table = lambda tableBucketARN, namespace, name: {
        "warehouseLocation": f"s3://{THEIRS}",
        "tableARN": f"{tableBucketARN}/table/11111111-aaaa"}
    out = reset.full_reset(wait_s=0)
    assert THEIRS in s3.buckets and OURS in s3.buckets
    assert any("does not match" in n for n in out["notes"])
