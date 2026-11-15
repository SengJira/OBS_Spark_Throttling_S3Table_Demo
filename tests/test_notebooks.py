"""Generated notebooks: lint-clean, output-free, sequentially numbered (#23)."""
import pathlib, re, subprocess, sys

import nbformat
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import gen_notebooks


@pytest.fixture(scope="module")
def nbs(tmp_path_factory):
    out = gen_notebooks.generate(tmp_path_factory.mktemp("nb"))
    return {p.name: nbformat.read(p, as_version=4) for p in sorted(out.glob("*.ipynb"))}


def test_all_six_notebooks_generated(nbs):
    assert sorted(nbs) == sorted(gen_notebooks.NOTEBOOKS)


def test_committed_notebooks_match_the_generator(nbs):
    for name, nb in nbs.items():
        committed = nbformat.read(ROOT / "notebooks" / name, as_version=4)
        assert [c.source for c in committed.cells] == [c.source for c in nb.cells], \
            f"{name} is stale — run scripts/gen_notebooks.py"


def test_notebooks_carry_no_outputs(nbs):
    for name in nbs:
        nb = nbformat.read(ROOT / "notebooks" / name, as_version=4)
        for c in nb.cells:
            if c.cell_type == "code":
                assert not c.outputs and c.execution_count is None, name


def test_code_is_pyflakes_clean(nbs, tmp_path):
    for name, nb in nbs.items():
        src = "\n\n".join(c.source for c in nb.cells if c.cell_type == "code")
        f = tmp_path / (name.replace(".ipynb", "").replace("-", "_") + ".py")
        f.write_text(src + "\n")
        r = subprocess.run([sys.executable, "-m", "pyflakes", str(f)],
                           capture_output=True, text=True)
        assert r.returncode == 0, f"{name}:\n{r.stdout}{r.stderr}"


def test_steps_and_phases_are_numbered_sequentially(nbs):
    for name, nb in nbs.items():
        heads = [c.source.splitlines()[0] for c in nb.cells
                 if c.cell_type == "markdown" and c.source.startswith("## ")]
        steps = [int(m.group(1)) for h in heads if (m := re.match(r"## Step (\d+)", h))]
        assert steps == list(range(1, len(steps) + 1)), f"{name}: steps {steps}"
        phases = [int(m.group(1)) for h in heads if (m := re.search(r"Phase (\d+)", h))]
        assert phases == list(range(1, len(phases) + 1)), f"{name}: phases {phases}"


def test_no_result_is_a_hardcoded_pass(nbs):
    # results.write(..., True, ...) would record PASS regardless of evidence
    for name, nb in nbs.items():
        for c in nb.cells:
            if c.cell_type == "code":
                assert not re.search(r'results\.write\([^)]*?,\s*True\s*[,)]', c.source, re.S), \
                    f"{name}: hard-coded PASS in\n{c.source[:200]}"
