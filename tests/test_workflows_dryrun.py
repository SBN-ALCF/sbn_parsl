import pytest
import subprocess
import pathlib

# Mapping of workflow scripts to their test settings
# If a workflow isn't here, we'll try to find a matching one or skip
WORKFLOW_TEST_MAP = {
    "sbnd_mc.py": "settings/sbnd/sbnd_mc.toml",
    "sbnd_data.py": "settings/sbnd/sbnd_data.toml",
    "sbnd_mc_detsys_vars.py": "settings/sbnd/settings_caf_detsys_vars.toml",
    "sbnd_mc_dentvar.py": "settings/sbnd/sbnd_mc_dentvar.toml",
    "icarus_mc.py": "settings/icarus/settings_mc_icarus_container.toml",
    "icarus_data.py": "settings/icarus/settings_data_icarus_container.toml",
}


@pytest.mark.parametrize("workflow_script", WORKFLOW_TEST_MAP.keys())
def test_workflow_dryrun(workflow_script):
    repo_root = pathlib.Path(__file__).parent.parent
    script_path = repo_root / "workflows" / workflow_script
    settings_path = repo_root / WORKFLOW_TEST_MAP[workflow_script]

    if not script_path.exists():
        pytest.skip(f"Workflow script {workflow_script} not found")
    if not settings_path.exists():
        pytest.skip(f"Settings file {settings_path} not found")

    output_dir = repo_root / "test_dryrun_outputs" / workflow_script.replace(".py", "")

    cmd = [
        "uv",
        "run",
        "python",
        str(script_path),
        str(settings_path),
        "-o",
        str(output_dir),
        "--dry-run",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, (
        f"Dry-run failed for {workflow_script}\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    )
    assert "DRY RUN" in result.stdout or "DRY RUN" in result.stderr
    assert "Done" in result.stdout
