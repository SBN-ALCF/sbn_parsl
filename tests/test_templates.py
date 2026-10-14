import pathlib
import subprocess
import uuid

import pytest

from sbn_parsl.templates import CMD_TEMPLATE_CONTAINER

# cp and rm are stubbed as shell functions, so this never touches /tmp even if
# the guard is broken; the test only sees what would have been run
STUBS = 'cp() { echo "CP $*"; }\nrm() { echo "RM $*"; }\n'


def _mirror_snippet(workdir, rootdir):
    """The /tmp mirror block at the end of CMD_TEMPLATE_CONTAINER, rendered."""
    start = CMD_TEMPLATE_CONTAINER.index('case "{workdir}"')
    end = CMD_TEMPLATE_CONTAINER.index("esac", start) + len("esac")
    return CMD_TEMPLATE_CONTAINER[start:end].format(workdir=workdir, rootdir=rootdir)


def _run(workdir, rootdir="/out"):
    result = subprocess.run(
        ["bash", "-c", STUBS + _mirror_snippet(workdir, rootdir)],
        capture_output=True, text=True, check=True,
    )
    return result.stdout


@pytest.mark.parametrize("workdir", [".", "", "/", "relative/dir"])
def test_mirror_skips_non_absolute_workdir(workdir):
    """A '.' workdir used to copy, then delete, all of /tmp."""
    assert _run(workdir) == ""


def test_mirror_skips_missing_tmp_dir():
    """SBND never writes /tmp/<workdir>; used to log a failed cp per task."""
    assert _run(f"/sbn_parsl_test_{uuid.uuid4().hex}") == ""


def test_mirror_copies_existing_tmp_dir():
    workdir = f"/sbn_parsl_test_{uuid.uuid4().hex}"
    mirror = pathlib.Path("/tmp" + workdir)
    mirror.mkdir()
    try:
        out = _run(workdir, rootdir="/out")
    finally:
        mirror.rmdir()
    # the template joins as /tmp/{workdir}, so an absolute workdir gives "//"
    assert out.splitlines() == [
        f"CP -a /tmp/{workdir}/. /out/",
        f"RM -rf /tmp/{workdir}",
    ]
