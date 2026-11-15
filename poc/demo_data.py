"""Demo dataset: synthetic sales orders -> partitioned Parquet on ObjectScale.

Rows are generated on the Spark side (spark.range + expressions seeded only by
the row id), so nothing is materialised on the driver and the dataset is
byte-for-byte the same on every run: same ids, customers, amounts and — with a
fixed base date — the same partitions.
"""
import datetime

from . import config, results

N_CUSTOMERS = 50
STATUSES = ["NEW", "PAID", "SHIPPED", "CANCELLED"]
DAYS = 30
# Measured on the lab cluster (snappy Parquet, partitioned by order_date):
# see reports/environment.md "Demo dataset sizing".
ROWS_PER_GB = config.DEMO_ROWS_PER_GB


def n_rows_for_scale(scale_gb=None):
    """Row count for a DEMO_SCALE_GB value (>= 1000 rows)."""
    scale = config.DEMO_SCALE_GB if scale_gb is None else float(scale_gb)
    return max(1000, int(scale * ROWS_PER_GB))


def base_date():
    return datetime.date.fromisoformat(config.DEMO_BASE_DATE)


def orders_df(spark, n_rows=None, days=DAYS):
    """Deterministic synthetic orders, `days` daily partitions from the fixed
    base date. Columns: order_id, customer, status, amount, qty, order_date."""
    from pyspark.sql import functions as F
    n = n_rows_for_scale() if n_rows is None else int(n_rows)
    rid = F.col("id")

    def h(salt):                      # deterministic non-negative hash of the id
        return F.pmod(F.xxhash64(rid, F.lit(salt)), F.lit(2 ** 31))

    statuses = F.array(*[F.lit(s) for s in STATUSES])
    return spark.range(n).select(
        F.concat(F.lit("o-"), rid.cast("string")).alias("order_id"),
        F.format_string("cust-%03d", (h(1) % N_CUSTOMERS + 1).cast("int")).alias("customer"),
        F.element_at(statuses, (h(2) % len(STATUSES) + 1).cast("int")).alias("status"),
        F.round(F.lit(5.0) + (h(3) % 199500) / 100.0, 2).alias("amount"),
        (h(4) % 50 + 1).cast("int").alias("qty"),
        F.date_add(F.lit(base_date().isoformat()).cast("date"),
                   (rid % days).cast("int")).cast("string").alias("order_date"),
    )


def data_path(sub="raw/orders"):
    return f"s3a://{config.BUCKET_DATA}/{results.run_id()}/{sub}"


def list_objects(bucket, prefix, limit=20):
    """First `limit` objects under a prefix + (total objects, total bytes)."""
    from . import s3util
    cli = s3util.s3("admin")
    out, total, size = [], 0, 0
    pag = cli.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            total += 1; size += o["Size"]
            if len(out) < limit:
                out.append({"Key": o["Key"], "Size": o["Size"]})
    return out, {"objects": total, "bytes": size}
