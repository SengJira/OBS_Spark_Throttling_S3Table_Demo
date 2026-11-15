"""Throughput benchmark: multipart upload at several concurrency levels.

Each level uploads `total_mb` via a single multipart upload whose parts are
pushed by a thread pool of size `concurrency`. Shows aggregate S3 write
throughput vs parallelism — the metric customers care about for ingest.
"""
import os, time
from concurrent.futures import ThreadPoolExecutor
from . import s3util


def _put_part(cli, bucket, key, upload_id, part_no, body):
    r = cli.upload_part(Bucket=bucket, Key=key, PartNumber=part_no,
                        UploadId=upload_id, Body=body)
    return {"PartNumber": part_no, "ETag": r["ETag"]}


def multipart_sweep(bucket, prefix, levels=(1, 2, 4, 8, 16),
                    total_mb=256, part_mb=16, user="admin"):
    """One multipart upload per level. Returns [{'concurrency', 'mb',
    'seconds', 'mbs'}]. Aborts cleanly on failure."""
    # one client shared by all part threads: size its connection pool for
    # the widest level, or the sweep measures botocore's 10-connection pool
    cli = s3util.s3(user, max_pool_connections=max(levels) + 2)
    part = os.urandom(part_mb * 1024 * 1024)   # one buffer, reused by every level
    n_parts = max(1, total_mb // part_mb)
    rows = []
    for n in levels:
        key = f"{prefix}/sweep-{n}way.bin"
        mup = cli.create_multipart_upload(Bucket=bucket, Key=key)
        try:
            t0 = time.monotonic()
            with ThreadPoolExecutor(max_workers=n) as ex:
                parts = list(ex.map(
                    lambda i: _put_part(cli, bucket, key, mup["UploadId"], i + 1, part),
                    range(n_parts)))
            dt = time.monotonic() - t0
            cli.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=mup["UploadId"],
                MultipartUpload={"Parts": sorted(parts, key=lambda p: p["PartNumber"])})
            mb = n_parts * part_mb
            rows.append({"concurrency": n, "mb": mb,
                         "seconds": round(dt, 2), "mbs": round(mb / dt, 1)})
        except Exception:
            try:
                cli.abort_multipart_upload(Bucket=bucket, Key=key,
                                           UploadId=mup["UploadId"])
            except Exception:
                pass
            raise
    return rows
