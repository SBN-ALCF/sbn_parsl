import pytest
import pathlib
import tomllib
from unittest.mock import MagicMock, patch

from sbn_parsl.config import Config
from sbn_parsl.app import entry_point, parse_arguments


@pytest.fixture
def mock_wfe():
    return MagicMock()


@pytest.fixture
def tmp_path_extended(tmp_path):
    """Setup a standard directory structure for testing."""
    settings_dir = tmp_path / "settings" / "sbnd"
    settings_dir.mkdir(parents=True)

    sites_dir = tmp_path / "settings" / "sites"
    sites_dir.mkdir(parents=True)

    # Create a mock site config
    site_toml = sites_dir / "polaris.toml"
    site_toml.write_text("""
[site]
name = "polaris"
cpus_per_node = 32
cores_per_worker = 1
max_futures = 100

[app.larsoft]
container_path = "/path/to/container"
larsoft_top = "/path/to/larsoft"
""")

    # Create a mock workflow config
    wf_toml = settings_dir / "sbnd_mc.toml"
    wf_toml.write_text("""
[app.larsoft]
experiment = "sbnd"
version = "v1"
qual = "e26:prof"

[workflow]
subruns_per_caf = 20

[workflow.fcls]
gen = "gen.fcl"
""")

    return tmp_path


def test_config_load_and_save(tmp_path_extended):
    wf_path = tmp_path_extended / "settings" / "sbnd" / "sbnd_mc.toml"

    real_path = pathlib.Path

    def mock_path_side_effect(*args, **kwargs):
        if args and isinstance(args[0], str) and args[0].endswith("config.py"):
            return real_path(tmp_path_extended / "sbn_parsl" / "config.py")
        return real_path(*args, **kwargs)

    with patch("sbn_parsl.config.pathlib.Path", side_effect=mock_path_side_effect):
        cfg = Config.load(
            wf_path, site_name="polaris", run_overrides={"output": "/tmp/out"}
        )

        assert cfg.larsoft.version == "v1"
        assert cfg.larsoft.container_path == "/path/to/container"
        assert cfg.larsoft.larsoft_top == "/path/to/larsoft"
        assert cfg.site.max_futures == 100
        assert cfg.workflow.fcls == {"gen": "gen.fcl"}

        # Test science hash
        h1 = cfg.get_science_hash()
        cfg.larsoft.version = "v2"
        h2 = cfg.get_science_hash()
        assert h1 != h2

        # Test save
        save_path = tmp_path_extended / "saved_config.toml"
        cfg.site.scheduler_options = (
            "#SBATCH -A neutrinoGPU\n#SBATCH -q debug\n#SBATCH -n 4"
        )
        cfg.site.launcher_options = 'container "sbn"'
        cfg.workflow.extra["nested_dict"] = {
            "test_key": "some\nnewline",
            "another": 'quote"',
        }
        cfg.workflow.extra["nested_list"] = ["item 1\nwith newline", "item 2"]
        cfg.save(save_path)
        assert save_path.exists()

        # Load back (direct raw TOML check)
        with open(save_path, "rb") as f:
            saved_data = tomllib.load(f)
        assert saved_data["app"]["larsoft"]["version"] == "v2"
        assert (
            saved_data["site"]["scheduler_options"]
            == "#SBATCH -A neutrinoGPU\n#SBATCH -q debug\n#SBATCH -n 4"
        )
        assert saved_data["site"]["launcher_options"] == 'container "sbn"'
        # The extra dictionary must be flattened directly under 'workflow'
        assert saved_data["workflow"]["nested_dict"]["test_key"] == "some\nnewline"
        assert saved_data["workflow"]["nested_dict"]["another"] == 'quote"'
        assert saved_data["workflow"]["nested_list"][0] == "item 1\nwith newline"

        # Load back via Config.load to verify identical science hash stability
        cfg_loaded = Config.load(
            save_path, site_name="polaris", run_overrides={"output": "/tmp/out"}
        )
        assert cfg_loaded.get_science_hash() == cfg.get_science_hash()
        assert cfg_loaded.workflow.extra["nested_dict"]["test_key"] == "some\nnewline"
        assert cfg_loaded.workflow.extra["nested_list"][0] == "item 1\nwith newline"


def test_cli_science_mismatch_prevents_run(tmp_path_extended, mock_wfe):
    tmp_path_extended / "settings" / "sbnd" / "sbnd_mc.toml"
    output_dir = tmp_path_extended / "output"
    output_dir.mkdir()

    # Mock the Config.load to simulate mismatch logic in app.py
    with patch("sbn_parsl.app.Config.load") as mock_load:
        cfg = MagicMock()
        cfg.run.output = str(output_dir)
        cfg.run.runinfo = None
        cfg.run.daos = False
        cfg.site.name = "polaris"
        cfg.get_science_hash.return_value = "hash1"

        mock_load.return_value = cfg

        # Mock runinfo dir existing with a DIFFERENT config
        runinfo_dir = output_dir / "runinfo"
        runinfo_dir.mkdir()
        config_toml = runinfo_dir / "config.toml"
        config_toml.write_text("dummy")

        # Mock existing_cfg load
        existing_cfg = MagicMock()
        existing_cfg.get_science_hash.return_value = "hash2"  # Mismatch!

        # Second call to load is for existing_cfg
        mock_load.side_effect = [cfg, existing_cfg]

        with patch("sys.stdout", new=MagicMock()) as mock_out:
            args = parse_arguments(["prog", "fake.toml", "-o", str(output_dir)])
            entry_point(args, mock_wfe)

            # Should print FATAL and NOT call mock_wfe.execute
            mock_wfe.execute.assert_not_called()
            stdout_str = "".join(call.args[0] for call in mock_out.write.call_args_list)
            assert "FATAL: Science identity mismatch" in stdout_str


def test_cli_force_bypasses_mismatch(tmp_path_extended, mock_wfe):
    output_dir = tmp_path_extended / "output"
    output_dir.mkdir()

    with patch("sbn_parsl.app.Config.load") as mock_load:
        cfg = MagicMock()
        cfg.run.output = str(output_dir)
        cfg.run.runinfo = None
        cfg.run.daos = False
        cfg.site.name = "polaris"
        cfg.get_science_hash.return_value = "hash1"

        mock_load.return_value = cfg

        runinfo_dir = output_dir / "runinfo"
        runinfo_dir.mkdir()
        (runinfo_dir / "config.toml").write_text("dummy")

        existing_cfg = MagicMock()
        existing_cfg.get_science_hash.return_value = "hash2"

        mock_load.side_effect = [cfg, existing_cfg]

        with (
            patch("sbn_parsl.app.parsl.load"),
            patch("sbn_parsl.app.parsl.clear"),
            patch("sbn_parsl.app.create_parsl_config"),
        ):
            # Call with --force
            args = parse_arguments(
                ["prog", "fake.toml", "-o", str(output_dir), "--force"]
            )
            entry_point(args, mock_wfe)

            # WFE is executed despite mismatch
            mock_wfe.return_value.execute.assert_called()


def test_cli_mandatory_output(mock_wfe):
    with pytest.raises(SystemExit):
        with patch("sys.stderr", new=MagicMock()):
            parse_arguments(["prog", "fake.toml"])


def test_config_legacy_fcls_fails(tmp_path_extended):
    wf_path = tmp_path_extended / "settings" / "sbnd" / "sbnd_mc.toml"
    # Overwrite the mock TOML to use a legacy top-level [fcls] block
    wf_path.write_text("""
[app.larsoft]
experiment = "sbnd"
version = "v1"
qual = "e26:prof"

[workflow]
subruns_per_caf = 20

[fcls]
gen = "gen.fcl"
""")

    real_path = pathlib.Path

    def mock_path_side_effect(*args, **kwargs):
        if args and isinstance(args[0], str) and args[0].endswith("config.py"):
            return real_path(tmp_path_extended / "sbn_parsl" / "config.py")
        return real_path(*args, **kwargs)

    with patch("sbn_parsl.config.pathlib.Path", side_effect=mock_path_side_effect):
        with pytest.raises(
            ValueError,
            match="Top-level \\[fcl\\] or \\[fcls\\] blocks are no longer supported",
        ):
            Config.load(
                wf_path, site_name="polaris", run_overrides={"output": "/tmp/out"}
            )


def test_config_singular_fcl_fails(tmp_path_extended):
    wf_path = tmp_path_extended / "settings" / "sbnd" / "sbnd_mc.toml"
    wf_path.write_text("""
[app.larsoft]
experiment = "sbnd"
version = "v1"
qual = "e26:prof"

[workflow]
subruns_per_caf = 20

[workflow.fcl]
gen = "gen.fcl"
""")

    real_path = pathlib.Path

    def mock_path_side_effect(*args, **kwargs):
        if args and isinstance(args[0], str) and args[0].endswith("config.py"):
            return real_path(tmp_path_extended / "sbn_parsl" / "config.py")
        return real_path(*args, **kwargs)

    with patch("sbn_parsl.config.pathlib.Path", side_effect=mock_path_side_effect):
        with pytest.raises(
            ValueError,
            match="The singular \\[workflow.fcl\\] heading is no longer supported",
        ):
            Config.load(
                wf_path, site_name="polaris", run_overrides={"output": "/tmp/out"}
            )


def test_config_flat_site_fails(tmp_path_extended):
    site_path = tmp_path_extended / "settings" / "sites" / "polaris.toml"
    site_path.write_text("""
name = "polaris"
max_futures = 100
container_path = "/path/to/container"
larsoft_top = "/path/to/larsoft"
""")
    wf_path = tmp_path_extended / "settings" / "sbnd" / "sbnd_mc.toml"

    real_path = pathlib.Path

    def mock_path_side_effect(*args, **kwargs):
        if args and isinstance(args[0], str) and args[0].endswith("config.py"):
            return real_path(tmp_path_extended / "sbn_parsl" / "config.py")
        return real_path(*args, **kwargs)

    with patch("sbn_parsl.config.pathlib.Path", side_effect=mock_path_side_effect):
        with pytest.raises(
            ValueError,
            match="missing required '\\[site\\]' and '\\[app.larsoft\\]' structured headers",
        ):
            Config.load(
                wf_path, site_name="polaris", run_overrides={"output": "/tmp/out"}
            )


def test_config_dynamic_app_loading(tmp_path_extended):
    wf_path = tmp_path_extended / "settings" / "sbnd" / "sbnd_mc.toml"
    site_path = tmp_path_extended / "settings" / "sites" / "polaris.toml"

    # Overwrite site config to include site-level apps
    site_path.write_text("""
[site]
name = "polaris"
cpus_per_node = 32
cores_per_worker = 1
max_futures = 100

[app.larsoft]
container_path = "/path/to/container"
larsoft_top = "/path/to/larsoft"

[app.spine]
model_path = "/path/to/site/spine.pt"
batch_size = 32
device = "cuda"

[app.unused_app]
some_key = "unused_value"
""")

    # Overwrite workflow config to activate app.spine and override settings
    wf_path.write_text("""
[app.larsoft]
experiment = "sbnd"
version = "v1"
qual = "e26:prof"

[workflow]
subruns_per_caf = 20

[workflow.fcls]
gen = "gen.fcl"

[app.spine]
batch_size = 64
device = "cpu"
""")

    real_path = pathlib.Path

    def mock_path_side_effect(*args, **kwargs):
        if args and isinstance(args[0], str) and args[0].endswith("config.py"):
            return real_path(tmp_path_extended / "sbn_parsl" / "config.py")
        return real_path(*args, **kwargs)

    with patch("sbn_parsl.config.pathlib.Path", side_effect=mock_path_side_effect):
        cfg = Config.load(
            wf_path, site_name="polaris", run_overrides={"output": "/tmp/out"}
        )

        # Verify aggregation and overrides
        assert cfg._active_dynamic_apps == ["larsoft", "spine"]
        assert cfg.spine.model_path == "/path/to/site/spine.pt"
        assert cfg.spine.batch_size == 64
        assert cfg.spine.device == "cpu"

        # Verify unused_app is NOT exposed
        assert not hasattr(cfg, "unused_app")


def test_config_dynamic_app_hash(tmp_path_extended):
    wf_path = tmp_path_extended / "settings" / "sbnd" / "sbnd_mc.toml"
    site_path = tmp_path_extended / "settings" / "sites" / "polaris.toml"

    # Setup site config
    site_path.write_text("""
[site]
name = "polaris"
cpus_per_node = 32
cores_per_worker = 1
max_futures = 100

[app.larsoft]
container_path = "/path/to/container"
larsoft_top = "/path/to/larsoft"

[app.spine]
model_path = "/path/to/site/spine.pt"
batch_size = 32

[app.unused_app]
some_key = "unused_value"
""")

    # Setup workflow config
    wf_path.write_text("""
[app.larsoft]
experiment = "sbnd"
version = "v1"
qual = "e26:prof"

[workflow]
subruns_per_caf = 20

[workflow.fcls]
gen = "gen.fcl"

[app.spine]
batch_size = 64
""")

    real_path = pathlib.Path

    def mock_path_side_effect(*args, **kwargs):
        if args and isinstance(args[0], str) and args[0].endswith("config.py"):
            return real_path(tmp_path_extended / "sbn_parsl" / "config.py")
        return real_path(*args, **kwargs)

    with patch("sbn_parsl.config.pathlib.Path", side_effect=mock_path_side_effect):
        cfg = Config.load(
            wf_path, site_name="polaris", run_overrides={"output": "/tmp/out"}
        )

        # Save base science hash
        h1 = cfg.get_science_hash()

        # Modify active app parameter -> science hash MUST change
        cfg.spine.batch_size = 128
        h2 = cfg.get_science_hash()
        assert h1 != h2

        # Reset and check that unused site app parameter change has NO effect
        cfg_ref = Config.load(
            wf_path, site_name="polaris", run_overrides={"output": "/tmp/out"}
        )
        h_ref1 = cfg_ref.get_science_hash()

        # Now change the unused app in the site TOML file, reload, and verify hash is identical
        site_path.write_text("""
[site]
name = "polaris"
cpus_per_node = 32
cores_per_worker = 1
max_futures = 100

[app.larsoft]
container_path = "/path/to/container"
larsoft_top = "/path/to/larsoft"

[app.spine]
model_path = "/path/to/site/spine.pt"
batch_size = 32

[app.unused_app]
some_key = "CHANGED_value"
""")
        cfg_ref2 = Config.load(
            wf_path, site_name="polaris", run_overrides={"output": "/tmp/out"}
        )
        h_ref2 = cfg_ref2.get_science_hash()
        assert h_ref1 == h_ref2


def test_config_dynamic_app_save(tmp_path_extended):
    wf_path = tmp_path_extended / "settings" / "sbnd" / "sbnd_mc.toml"
    site_path = tmp_path_extended / "settings" / "sites" / "polaris.toml"

    site_path.write_text("""
[site]
name = "polaris"
cpus_per_node = 32
cores_per_worker = 1
max_futures = 100

[app.larsoft]
container_path = "/path/to/container"
larsoft_top = "/path/to/larsoft"

[app.spine]
model_path = "/path/to/site/spine.pt"
""")

    wf_path.write_text("""
[app.larsoft]
experiment = "sbnd"
version = "v1"
qual = "e26:prof"

[workflow]
subruns_per_caf = 20

[workflow.fcls]
gen = "gen.fcl"

[app.spine]
batch_size = 64
""")

    real_path = pathlib.Path

    def mock_path_side_effect(*args, **kwargs):
        if args and isinstance(args[0], str) and args[0].endswith("config.py"):
            return real_path(tmp_path_extended / "sbn_parsl" / "config.py")
        return real_path(*args, **kwargs)

    with patch("sbn_parsl.config.pathlib.Path", side_effect=mock_path_side_effect):
        cfg = Config.load(
            wf_path, site_name="polaris", run_overrides={"output": "/tmp/out"}
        )

        save_path = tmp_path_extended / "saved_app_config.toml"
        cfg.save(save_path)

        assert save_path.exists()

        with open(save_path, "rb") as f:
            saved_data = tomllib.load(f)

        assert "app" in saved_data
        assert "spine" in saved_data["app"]
        assert saved_data["app"]["spine"]["model_path"] == "/path/to/site/spine.pt"
        assert saved_data["app"]["spine"]["batch_size"] == 64

        # Load back via Config.load and check stability
        cfg_loaded = Config.load(
            save_path, site_name="polaris", run_overrides={"output": "/tmp/out"}
        )
        assert cfg_loaded.get_science_hash() == cfg.get_science_hash()
        assert cfg_loaded.spine.model_path == "/path/to/site/spine.pt"
        assert cfg_loaded.spine.batch_size == 64


def test_local_qsub_submission_creates_correct_script(tmp_path_extended, mock_wfe):
    wf_path = tmp_path_extended / "settings" / "sbnd" / "sbnd_mc.toml"
    output_dir = tmp_path_extended / "output"
    output_dir.mkdir()

    real_path = pathlib.Path

    def mock_path_side_effect(*args, **kwargs):
        if args and isinstance(args[0], str) and args[0].endswith("config.py"):
            return real_path(tmp_path_extended / "sbn_parsl" / "config.py")
        return real_path(*args, **kwargs)

    with (
        patch("sbn_parsl.config.pathlib.Path", side_effect=mock_path_side_effect),
        patch("sbn_parsl.app.subprocess.run") as mock_run,
        patch("sbn_parsl.app.detect_active_env", return_value="/my/fake/venv"),
        patch("os.environ", {}),
    ):
        args = parse_arguments(
            [
                "prog",
                str(wf_path),
                "-o",
                str(output_dir),
                "--local",
                "--site",
                "polaris",
            ]
        )
        entry_point(args, mock_wfe)

        # Verify subprocess.run was called for qsub
        assert mock_run.call_count == 1
        qsub_cmd = mock_run.call_args[0][0]
        assert qsub_cmd[0] == "qsub"
        script_path = qsub_cmd[-1]

        # Read the generated script
        with open(script_path, "r") as f:
            script_content = f.read()

        # Verify environment activation and escaped PATH are present
        assert "source /my/fake/venv/bin/activate" in script_content
        assert "export PATH=/opt/cray/pals/1.4/bin:${PATH}" in script_content
        assert "${{PATH}}" not in script_content

