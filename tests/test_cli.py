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
name = "polaris"
cpus_per_node = 32
cores_per_worker = 1
container_path = "/path/to/container"
larsoft_top = "/path/to/larsoft"
max_futures = 100
""")

    # Create a mock workflow config
    wf_toml = settings_dir / "sbnd_mc.toml"
    wf_toml.write_text("""
[larsoft]
experiment = "sbnd"
version = "v1"
qual = "e26:prof"

[workflow]
subruns_per_caf = 20

[fcls]
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
        assert cfg.site.max_futures == 100

        # Test science hash
        h1 = cfg.get_science_hash()
        cfg.larsoft.version = "v2"
        h2 = cfg.get_science_hash()
        assert h1 != h2

        # Test save
        save_path = tmp_path_extended / "saved_config.toml"
        cfg.site.scheduler_options = "#SBATCH -A neutrinoGPU\n#SBATCH -q debug\n#SBATCH -n 4"
        cfg.site.launcher_options = 'container "sbn"'
        cfg.workflow.extra["nested_dict"] = {"test_key": "some\nnewline", "another": "quote\""}
        cfg.workflow.extra["nested_list"] = ["item 1\nwith newline", "item 2"]
        cfg.save(save_path)
        assert save_path.exists()

        # Load back (direct raw TOML check)
        with open(save_path, "rb") as f:
            saved_data = tomllib.load(f)
        assert saved_data["larsoft"]["version"] == "v2"
        assert saved_data["site"]["scheduler_options"] == "#SBATCH -A neutrinoGPU\n#SBATCH -q debug\n#SBATCH -n 4"
        assert saved_data["site"]["launcher_options"] == 'container "sbn"'
        # The extra dictionary must be flattened directly under 'workflow'
        assert saved_data["workflow"]["nested_dict"]["test_key"] == "some\nnewline"
        assert saved_data["workflow"]["nested_dict"]["another"] == "quote\""
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
