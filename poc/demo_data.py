"""Demo dataset: synthetic sales orders -> partitioned Parquet on ObjectScale."""
import random, datetime
from . import config, results

CUSTOMERS = [f"cust-{i:03d}" for i in range(1, 51)]
STATUSES = ["NEW", "PAID", "SHIPPED", "CANCELLED"]


def orders_df(spark, n_rows=200_000, days=30):
    """Synthetic orders DataFrame partitioned by date."""
    rows_per_day = max(100, n_rows // days)
    base = datetime.date.today() - datetime.timedelta(days=days)
    rnd = random.Random(42)
    data = []
    for d in range(days):
        for i in range(rows_per_day):
            data.append((f"o-{d}-{i}", rnd.choice(CUSTOMERS),
                         rnd.choice(STATUSES), round(rnd.uniform(5, 2000), 2),
                         rnd.randint(1, 50), str(base + datetime.timedelta(days=d))))
    return spark.createDataFrame(
        data, ["order_id", "customer", "status", "amount", "qty", "order_date"])


def data_path(sub="raw/orders"):
    return f"s3a://{config.BUCKET_DATA}/{results.run_id()}/{sub}"


def list_objects(bucket, prefix, limit=20):
    from . import s3util
    cli = s3util.s3("admin")
    out = []
    pag = cli.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            out.append({"Key": o["Key"], "Size": o["Size"]})
            if len(out) >= limit:
                return out, page.get("IsTruncated")
    return out, False
