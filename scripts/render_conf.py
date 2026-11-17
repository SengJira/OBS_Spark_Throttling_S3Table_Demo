#!/usr/bin/env python3
"""Render /opt/spark/conf/spark-defaults.conf from conf/spark-defaults.conf.j2.

Values come from the process environment first, then the credentials file,
then .env — the same precedence as poc.config.get(). The output is created
with mode 0600 from the first byte. A placeholder that looks like a secret
(…_AK, …_SK, …PASSWORD, …SECRET, …TOKEN) must have a value unless
--allow-empty is given; any other unresolved placeholder is always an error.
"""
import argparse, os, pathlib, re, sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from poc import config                                  # noqa: E402
from poc.spark_helpers import write_private             # noqa: E402

TPL = ROOT / "conf" / "spark-defaults.conf.j2"
OUT = pathlib.Path(os.environ.get("SPARK_DEFAULTS_OUT",
                                  "/opt/spark/conf/spark-defaults.conf"))
SECRET_RE = re.compile(r"(_AK|_SK|PASSWORD|SECRET|TOKEN)$")
PLACEHOLDER = re.compile(r"\{\{(\w+)\}\}")
DERIVED = {"ICEBERG_WAREHOUSE": config.table_bucket_arn}


def value(key):
    """Process env > credentials file > .env (poc.config.get), plus values
    derived from the config module (ARN, defaults)."""
    v = config.get(key)
    if v in (None, "") and key in DERIVED:
        v = DERIVED[key]()
    if v in (None, "") and hasattr(config, key):
        v = getattr(config, key)
    return "" if v is None else str(v)


def render(template, allow_empty=False):
    keys = sorted(set(PLACEHOLDER.findall(template)))
    vals = {k: value(k) for k in keys}
    empty = [k for k in keys if not vals[k]]
    empty_secret = [k for k in empty if SECRET_RE.search(k)]
    empty_plain = [k for k in empty if k not in empty_secret]
    if empty_plain:
        raise SystemExit(f"ERROR: no value for {empty_plain} "
                         f"(set them in the environment or {config.ENV_FILE})")
    if empty_secret and not allow_empty:
        raise SystemExit(f"ERROR: secret keys are empty: {empty_secret} — run "
                         f"scripts/prep_obs.py first, or pass --allow-empty "
                         f"to render a bootstrap file without them")
    return PLACEHOLDER.sub(lambda m: vals[m.group(1)], template), empty_secret


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--allow-empty", action="store_true",
                    help="render even if secret placeholders have no value")
    ap.add_argument("--out", default=str(OUT))
    args = ap.parse_args(argv)
    text, empty = render(TPL.read_text(), args.allow_empty)
    out = write_private(args.out, text)
    try:
        import pwd, grp
        os.chown(out, pwd.getpwnam("spark").pw_uid, grp.getgrnam("spark").gr_gid)
    except (KeyError, PermissionError):
        pass
    print(f"wrote {out} (0600" + (f"; EMPTY secrets: {empty}" if empty else "") + ")")


if __name__ == "__main__":
    sys.exit(main())
