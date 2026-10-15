import pathlib
import subprocess

import pytest

from sbn_parsl.templates import (
    CMD_TEMPLATE_CONTAINER,
    CMD_TEMPLATE_SPACK,
    LOCAL_BUFFER_INIT,
    LOCAL_BUFFER_SAVE,
)


def _bash(script):
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True)


def _save(rootdir):
    return "set -e\n" + LOCAL_BUFFER_SAVE.format(rootdir=rootdir)


@pytest.mark.parametrize("ending", ["true", "exit 3"])
def test_buffer_is_created_and_always_removed(ending):
    result = _bash(
        LOCAL_BUFFER_INIT + 'test -d "$SBN_LOCAL_BUFFER" && echo "$SBN_LOCAL_BUFFER"\n' + ending
    )
    buffer = pathlib.Path(result.stdout.strip())
    assert str(buffer).startswith("/tmp/sbn_local_buffer.")
    assert not buffer.exists()


def _filled_buffer(tmp_path):
    buffer = tmp_path / "buffer"
    (buffer / "keep" / "larcv" / "000000").mkdir(parents=True)
    (buffer / "keep" / "larcv" / "000000" / "larcv_x.root").write_text("larcv")
    (buffer / "discard").mkdir()
    (buffer / "discard" / "hists_x.root").write_text("calib")
    return buffer


def test_keep_copied_to_output_on_success(tmp_path):
    buffer, out = _filled_buffer(tmp_path), tmp_path / "out"
    out.mkdir()
    result = _bash(f"export SBN_LOCAL_BUFFER={buffer}\nexit_code=0\n" + _save(out))
    assert result.returncode == 0, result.stderr
    assert (out / "larcv" / "000000" / "larcv_x.root").read_text() == "larcv"
    assert not (out / "hists_x.root").exists()
    assert not (out / "discard").exists()


def test_nothing_copied_on_failure(tmp_path):
    buffer, out = _filled_buffer(tmp_path), tmp_path / "out"
    out.mkdir()
    _bash(f"export SBN_LOCAL_BUFFER={buffer}\nexit_code=1\n" + _save(out))
    assert list(out.iterdir()) == []


def test_empty_buffer_is_a_no_op(tmp_path):
    (tmp_path / "buffer").mkdir()
    result = _bash(f"export SBN_LOCAL_BUFFER={tmp_path / 'buffer'}\n" + _save(tmp_path / "out"))
    assert result.returncode == 0, result.stderr


def test_failed_copy_fails_the_task(tmp_path):
    buffer = _filled_buffer(tmp_path)
    not_a_dir = tmp_path / "file"
    not_a_dir.write_text("")
    result = _bash(f"export SBN_LOCAL_BUFFER={buffer}\n" + _save(not_a_dir))
    assert result.returncode != 0


@pytest.mark.parametrize("template, payload", [
    (CMD_TEMPLATE_CONTAINER, "singularity run"),
    (CMD_TEMPLATE_SPACK, "eval $lar_cmd"),
])
def test_templates_buffer_around_payload(template, payload):
    init = template.index("SBN_LOCAL_BUFFER=$(mktemp")
    save = template.index('cp -a "$SBN_LOCAL_BUFFER/keep/."')
    assert init < template.index(payload) < save


def test_old_workdir_mirror_is_gone():
    """The /tmp/<workdir> mirror could copy, then delete, all of /tmp."""
    assert "/tmp/{workdir}" not in CMD_TEMPLATE_CONTAINER
