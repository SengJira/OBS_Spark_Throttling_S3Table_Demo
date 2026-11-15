"""render_conf: env precedence, empty-secret refusal, 0600 from creation (#19)."""
import importlib.util, os, pathlib, stat, sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from poc import config, spark_helpers

spec = importlib.util.spec_from_file_location(
    "render_conf", ROOT / "scripts" / "render_conf.py")
render_conf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(render_conf)


def test_process_env_beats_dotenv(monkeypatch):
    monkeypatch.setitem(config.ENV, "SPARK_MASTER", "spark://from-dotenv:7077")
    monkeypatch.setenv("SPARK_MASTER", "spark://from-env:7077")
    text, _ = render_conf.render("m {{SPARK_MASTER}}\n")
    assert text == "m spark://from-env:7077\n"


def test_empty_secret_is_refused_unless_allowed(monkeypatch):
    monkeypatch.delenv("DEMO_X_SK", raising=False)
    with pytest.raises(SystemExit, match="secret keys are empty"):
        render_conf.render("k {{DEMO_X_SK}}\n")
    text, empty = render_conf.render("k {{DEMO_X_SK}}\n", allow_empty=True)
    assert text == "k \n" and empty == ["DEMO_X_SK"]


def test_unresolved_plain_placeholder_is_always_an_error(monkeypatch):
    monkeypatch.delenv("DEMO_NOT_SET", raising=False)
    with pytest.raises(SystemExit, match="no value"):
        render_conf.render("k {{DEMO_NOT_SET}}\n", allow_empty=True)


def test_repo_template_renders_and_holds_no_secret_placeholders():
    tpl = (ROOT / "conf" / "spark-defaults.conf.j2").read_text()
    keys = set(render_conf.PLACEHOLDER.findall(tpl))
    assert not [k for k in keys if render_conf.SECRET_RE.search(k)]
    text, _ = render_conf.render(tpl)
    assert "{{" not in text and "spark.master" in text


def test_write_private_is_0600_even_over_a_world_readable_file(tmp_path):
    f = tmp_path / "secret.conf"
    f.write_text("old"); os.chmod(f, 0o644)
    old_umask = os.umask(0)            # a permissive umask must not matter
    try:
        spark_helpers.write_private(f, "new\n")
        g = spark_helpers.write_private(tmp_path / "fresh.conf", "x\n")
    finally:
        os.umask(old_umask)
    assert stat.S_IMODE(f.stat().st_mode) == 0o600 and f.read_text() == "new\n"
    assert stat.S_IMODE(g.stat().st_mode) == 0o600
