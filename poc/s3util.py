"""boto3 client factory for ObjectScale S3 / S3 Tables."""
import boto3
from botocore.config import Config as BotoConfig
from . import config


DEFAULT_MAX_ATTEMPTS = 8


def s3(user="admin", endpoint=None, timeout_s=(3, 10),
       max_attempts=DEFAULT_MAX_ATTEMPTS):
    """S3 client for a PoC user. `max_attempts` is the total number of tries
    (botocore standard mode); 1 disables retries so every server response —
    including 429/503 throttle rejections — reaches the caller raw."""
    ak, sk = config.USERS[user]
    return boto3.client(
        "s3",
        endpoint_url=endpoint or config.OBS_S3_ENDPOINT,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        region_name=config.OBS_REGION,
        config=BotoConfig(s3={"addressing_style": "path"},
                          request_checksum_calculation="when_required",
                          response_checksum_validation="when_required",
                          connect_timeout=timeout_s[0],
                          read_timeout=timeout_s[1],
                          retries={"max_attempts": max_attempts,
                                   "mode": "standard"}),
        verify="/etc/ssl/certs/ca-certificates.crt",
    )


def s3tables(user="tables-rw"):
    ak, sk = config.USERS[user]
    cli = boto3.client(
        "s3tables",
        endpoint_url=config.OBS_TABLES_ENDPOINT,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        region_name=config.OBS_REGION,
        config=BotoConfig(signature_version="v4",
                          retries={"max_attempts": 5}),
    )
    # OBS tablesvc resolves the caller's namespace from x-emc-namespace
    def _add_ns(**kw):
        kw["params"]["headers"]["x-emc-namespace"] = config.OBS_NAMESPACE
    cli.meta.events.register("before-call.s3tables", _add_ns)
    return cli


def bucket_exists(cli, name):
    try:
        cli.head_bucket(Bucket=name)
        return True
    except Exception:
        return False
