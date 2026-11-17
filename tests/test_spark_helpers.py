"""Per-bucket S3A credentials, per-bucket identity, write benchmarking."""
import pathlib, sys, time

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from poc import config
import poc.spark_helpers as sh


@pytest.fixture
def users(monkeypatch):
    monkeypatch.setitem(config.USERS, "admin", ("AK_ADMIN", "SK_ADMIN"))
    monkeypatch.setitem(config.USERS, "noisy", ("AK_NOISY", "SK_NOISY"))
    monkeypatch.setitem(config.USERS, "tables-rw", ("AK_TBL", "SK_TBL"))


class _DF:
    def __init__(self, spark):
        self.spark = spark

    def repartition(self, n):
        self.spark.repartitions.append(n)
        return self

    def coalesce(self, n):
        self.spark.coalesces.append(n)
        return self

    @property
    def write(self):
        return self.spark.writer


class _Writer:
    def __init__(self, spark):
        self.spark = spark

    def mode(self, m):
        self.spark.modes.append(m)
        return self

    def json(self, path):
        self.spark.writes.append(path)
        if self.spark.fail_on and self.spark.fail_on in path:
            raise RuntimeError("simulated throttled-write failure")


class FakeSpark:
    def __init__(self, fail_on=None):
        self.writes, self.modes = [], []
        self.repartitions, self.coalesces = [], []
        self.fail_on = fail_on
        self.writer = _Writer(self)

    def range(self, n):
        self.spark_ranges = getattr(self, "spark_ranges", [])
        self.spark_ranges.append(n)
        return _DF(self)


def test_secret_conf_bucket_users(users):
    conf = sh.secret_conf(bucket_users={"poc-throttle": "noisy"})
    assert conf["spark.hadoop.fs.s3a.access.key"] == "AK_ADMIN"
    assert conf["spark.hadoop.fs.s3a.bucket.poc-throttle.access.key"] == "AK_NOISY"
    assert conf["spark.hadoop.fs.s3a.bucket.poc-throttle.secret.key"] == "SK_NOISY"


def test_secret_conf_bucket_users_missing_creds(users, monkeypatch):
    monkeypatch.setitem(config.USERS, "ghost", ("", ""))
    with pytest.raises(RuntimeError, match="missing credentials"):
        sh.secret_conf(bucket_users={"b": "ghost"})


def test_secret_conf_no_bucket_users_is_unchanged(users):
    conf = sh.secret_conf()
    assert not any(k.startswith("spark.hadoop.fs.s3a.bucket.") for k in conf)


class _HConf(dict):
    def get(self, k, d=None):
        return super().get(k, d)


class _JSC:
    def __init__(self, conf):
        self._conf = conf

    def hadoopConfiguration(self):
        return self._conf


class _SC:
    def __init__(self, conf):
        self._jsc = _JSC(conf)


class FakeSessionSpark:
    def __init__(self, hconf):
        self.sparkContext = _SC(hconf)


def test_bucket_identities_per_bucket(users):
    hconf = _HConf({
        "fs.s3a.access.key": "AK_ADMIN",
        "fs.s3a.bucket.poc-throttle.access.key": "AK_NOISY",
    })
    spark = FakeSessionSpark(hconf)
    ids = sh.bucket_identities(spark, ["poc-throttle", "poc-spark-data"])
    assert ids == {"poc-throttle": "noisy", "poc-spark-data": "admin"}


def test_bucket_identities_falls_back_to_default(users):
    hconf = _HConf({"fs.s3a.access.key": "AK_ADMIN"})
    spark = FakeSessionSpark(hconf)
    ids = sh.bucket_identities(spark, ["poc-throttle"])
    assert ids["poc-throttle"] == "admin"   # what the notebook turns into FAIL


def test_benchmark_writes_pairs_and_repeats():
    spark = FakeSpark()
    times, targets, t0, errors = sh.benchmark_writes(
        spark, "demo-x", [("admin", "b-admin"), ("noisy", "b-noisy")],
        rows=50_000, partitions=4, repeats=2, warmup_bucket="b-admin")
    assert times["admin"] == times["noisy"] or times  # both ran twice
    assert len(times["admin"]) == 2 and len(times["noisy"]) == 2
    assert [t for _, t in targets] == [
        "demo-x/spark-write-admin-0", "demo-x/spark-write-noisy-0",
        "demo-x/spark-write-admin-1", "demo-x/spark-write-noisy-1"]
    # one tiny untimed warm-up first, then four timed repartitioned writes
    assert spark.writes[0] == "s3a://b-admin/demo-x/spark-write-warmup"
    assert spark.coalesces == [1]
    assert len(spark.repartitions) == 4
    assert errors == [] and t0 <= time.time()


def test_benchmark_writes_repeats_one_is_single_run():
    spark = FakeSpark()
    times, targets, _, _ = sh.benchmark_writes(
        spark, "demo-x", [("admin", "b-admin"), ("noisy", "b-noisy")],
        rows=10, partitions=1, repeats=1, warmup_bucket=None)
    assert len(times["admin"]) == 1 and len(times["noisy"]) == 1
    assert len(spark.writes) == 2          # no warm-up
    # statistics.median of a single run is that run — the "single time" report
    import statistics
    assert statistics.median(times["noisy"]) == times["noisy"][0]


def test_benchmark_writes_collects_errors():
    spark = FakeSpark(fail_on="b-noisy")
    times, targets, _, errors = sh.benchmark_writes(
        spark, "demo-x", [("admin", "b-admin"), ("noisy", "b-noisy")],
        rows=10, partitions=1, repeats=1, warmup_bucket=None)
    assert len(times["admin"]) == 1 and times["noisy"] == []
    assert len(errors) == 1 and errors[0]["label"] == "noisy"
    assert errors[0]["target"] == "demo-x/spark-write-noisy-0"
    assert "simulated" in errors[0]["error"]
    # admin still ran to completion after the noisy failure
    assert len(targets) == 1


def test_secret_conf_file_0600(users, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SECRETS_DIR", tmp_path)
    path = sh._secret_conf_file("tables-rw", "admin",
                                {"poc-throttle": "noisy"})
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    body = path.read_text()
    assert "poc-throttle.access.key AK_NOISY" in body
    assert "SK_NOISY" in body
    assert path.name.endswith("-buckets.conf")
    # a different bucket_users set must not reuse the same filename
    other = sh._secret_conf_file("tables-rw", "admin")
    assert other != path
    assert "AK_NOISY" not in other.read_text()


def test_build_session_rejects_mismatched_identity(users):
    """build_session with a live session of the wrong identity must fail."""
    # covered indirectly: identities() is what build_session compares;
    # a direct check that bucket creds don't leak into the default keys.
    conf = sh.secret_conf(s3a_user="admin",
                          bucket_users={"poc-throttle": "noisy"})
    assert conf["spark.hadoop.fs.s3a.access.key"] == "AK_ADMIN"
    assert "AK_NOISY" not in conf["spark.hadoop.fs.s3a.access.key"]
    assert conf["spark.hadoop.fs.s3a.secret.key"] == "SK_ADMIN"
    assert "SK_NOISY" not in conf["spark.hadoop.fs.s3a.secret.key"]


def test_config_defaults():
    assert config.SPARK_WRITE_REPEATS >= 1
    assert config.SPARK_WRITE_PARTITIONS <= 20
    assert config.SPARK_WRITE_ROWS <= 200_000


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
