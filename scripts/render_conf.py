#!/usr/bin/env python3
"""Render /opt/spark/conf/spark-defaults.conf from conf/spark-defaults.conf.j2 + .env.
Rendered file is chmod 0600 (contains injected secrets)."""
import os, re, sys, pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
ENV = ROOT / ".env"
TPL = ROOT / "conf" / "spark-defaults.conf.j2"
OUT = pathlib.Path("/opt/spark/conf/spark-defaults.conf")


def load_env(path):
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def main():
    env = {**os.environ, **load_env(ENV)}
    # OBS REST catalog needs the warehouse ARN percent-encoded as the {prefix}
    import urllib.parse
    env.setdefault("ICEBERG_PREFIX",
                   urllib.parse.quote(env.get("ICEBERG_WAREHOUSE", ""), safe=""))
    tpl = TPL.read_text()
    missing = set(re.findall(r"\{\{(\w+)\}\}", tpl)) - set(env)
    # empty secrets are fine at bootstrap; warn but still render
    def sub(m):
        v = env.get(m.group(1), "")
        return v
    OUT.write_text(re.sub(r"\{\{(\w+)\}\}", sub, tpl))
    os.chmod(OUT, 0o600)
    try:
        import pwd, grp
        os.chown(OUT, pwd.getpwnam("spark").pw_uid, grp.getgrnam("spark").gr_gid)
    except KeyError:
        pass
    print(f"wrote {OUT} (missing keys: {sorted(missing) or 'none'})")


if __name__ == "__main__":
    sys.exit(main())
