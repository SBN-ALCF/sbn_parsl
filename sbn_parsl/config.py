from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import tomllib
import pathlib
import socket
import json
import hashlib


@dataclass
class SiteConfig:
    """
    Static site configuration and defaults.
    Defines hardware-specific parameters such as core counts, queue options,
    and paths to the container and larsoft installations.
    """

    name: str
    cpus_per_node: int
    cores_per_worker: int
    container_path: str
    larsoft_top: str
    max_futures: int = 60000

    # Static patterns
    scheduler_options: str = ""
    launcher_options: str = ""
    cpu_affinity: str = "none"
    available_accelerators: Any = None
    pbs_filesystems: str = "home"

    # Environment
    worker_init: str = ""
    worker_venv_name: str = "sbn"
    simulation_inputs: str = "/lus/flare/projects/neutrinoGPU/simulation_inputs_striped"
    metadata_exe: Optional[str] = None

    @classmethod
    def from_toml(cls, path: pathlib.Path) -> "SiteConfig":
        """Loads a SiteConfig from a TOML file."""
        with open(path, "rb") as f:
            data = tomllib.load(f)
        return cls(**data)


@dataclass
class JobConfig:
    """
    Dynamic job submission parameters.
    These parameters typically override the site defaults via the CLI and
    dictate queue, allocation, walltime, and sizing properties.
    """

    allocation: str = "neutrinoGPU"
    queue: str = "debug"
    nodes_per_block: int = 1
    walltime: str = "01:00:00"
    max_workers_per_node: Optional[int] = None
    retries: int = 5
    strategy: str = "none"
    daos_pool: Optional[str] = None
    daos_cont: Optional[str] = None


@dataclass
class LArSoftConfig:
    """
    Configuration options specific to the LArSoft execution environment.
    Defines the experiment name, software version, FCL parameters,
    and optional overlays or flux file paths.
    """

    experiment: str
    version: str
    qual: str
    software: str = "sbndcode"
    nevts: int = -1
    nskip: int = 0
    lar_args: str = ""
    flux_path: str = "fluxFiles/bnb/G4BNB/v1.1.1/fhc/a"
    flux_files: str = "NuBeam_production_BooNE_50m_I174000A_*.dk2nu.root"
    flux_type: str = "dk2nu"
    overlays: List[str] = field(default_factory=list)
    tarballs: Dict[str, str] = field(default_factory=dict)


@dataclass
class WorkflowConfig:
    """
    Top-level workflow configuration logic.
    Holds FCL stage definitions and logic-specific parameters like keep fractions.
    """

    subruns_per_caf: int = 20
    full_keep_fraction: float = 1.0
    fcls: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetadataConfig:
    """
    Metadata injection configuration parameters.
    """

    exe: Optional[str] = None
    mdprojectversion: Optional[str] = None


@dataclass
class RunConfig:
    """
    General execution properties for the current workflow run.
    Contains output directory locations and success requirements.
    """

    nsubruns: int = 1
    runinfo: Optional[str] = None
    daos: bool = False
    output: str = ""  # Set dynamically from CLI
    require_success: bool = True
    file_list: Optional[str] = None
    files_per_subrun: Optional[int] = None


@dataclass
class Config:
    """
    Root configuration object containing all structured settings.
    This class handles the parsing and merging of TOML configuration files
    along with dynamic CLI overrides.
    """

    site: SiteConfig
    job: JobConfig
    larsoft: LArSoftConfig
    workflow: WorkflowConfig
    run: RunConfig
    metadata: MetadataConfig = field(default_factory=MetadataConfig)

    def get_science_hash(self) -> str:
        """
        Returns a hash representing the 'Science Identity' of this run.
        Excludes transient job settings, site infrastructure, and nsubruns.
        """
        # We include larsoft, workflow, and metadata
        identity = {
            "larsoft": self.larsoft.__dict__,
            "workflow": self.workflow.__dict__,
            "metadata": self.metadata.__dict__,
        }

        # Convert to a stable JSON string for hashing
        def _stable_repr(obj):
            if isinstance(obj, dict):
                return {k: _stable_repr(v) for k, v in sorted(obj.items())}
            if isinstance(obj, (list, tuple)):
                return [_stable_repr(x) for x in obj]
            return obj

        stable_id = json.dumps(_stable_repr(identity), sort_keys=True)
        return hashlib.sha256(stable_id.encode()).hexdigest()[:16]

    def save(self, path: pathlib.Path):
        """Save the current configuration to a TOML file."""
        data = self.to_dict()

        lines = []
        for section, content in sorted(data.items()):
            lines.append(f"[{section}]")
            for k, v in sorted(content.items()):
                if isinstance(v, str):
                    lines.append(f'{k} = "{v}"')
                elif isinstance(v, bool):
                    lines.append(f"{k} = {str(v).lower()}")
                elif v is None:
                    continue
                elif isinstance(v, dict):
                    items = []
                    for dk, dv in v.items():
                        if isinstance(dv, str):
                            items.append(f'{dk} = "{dv}"')
                        elif isinstance(dv, bool):
                            items.append(f"{dk} = {str(dv).lower()}")
                        else:
                            items.append(f"{dk} = {dv}")
                    lines.append(f"{k} = {{ {', '.join(items)} }}")
                elif isinstance(v, list):
                    items = []
                    for item in v:
                        if isinstance(item, str):
                            items.append(f'"{item}"')
                        elif isinstance(item, bool):
                            items.append(str(item).lower())
                        else:
                            items.append(str(item))
                    lines.append(f"{k} = [{', '.join(items)}]")
                else:
                    lines.append(f"{k} = {v}")
            lines.append("")

        with open(path, "w") as f:
            f.write("\n".join(lines))

    @classmethod
    def load(
        cls,
        workflow_path: pathlib.Path,
        site_name: Optional[str] = None,
        job_overrides: Dict[str, Any] = None,
        run_overrides: Dict[str, Any] = None,
    ) -> "Config":

        # 1. Detect site
        if site_name is None:
            hostname = socket.gethostname()
            if "polaris" in hostname or hostname.startswith("x3"):
                site_name = "polaris"
            elif "aurora" in hostname or hostname.startswith("x4"):
                site_name = "aurora"
            else:
                site_name = "local"

        # 2. Load site config
        repo_root = pathlib.Path(__file__).parent.parent
        site_path = repo_root / "settings" / "sites" / f"{site_name}.toml"
        if not site_path.exists():
            raise FileNotFoundError(f"Site config not found: {site_path}")

        with open(site_path, "rb") as f:
            site_data = tomllib.load(f)
        site_cfg = SiteConfig(**site_data)

        # 3. Load workflow TOML
        with open(workflow_path, "rb") as f:
            wf_data = tomllib.load(f)

        larsoft_cfg = LArSoftConfig(**wf_data.get("larsoft", {}))
        workflow_cfg = WorkflowConfig(**wf_data.get("workflow", {}))
        if "fcls" in wf_data:
            workflow_cfg.fcls.update(wf_data["fcls"])

        run_data = wf_data.get("run", {})
        if run_overrides:
            run_data.update({k: v for k, v in run_overrides.items() if v is not None})
        run_cfg = RunConfig(**run_data)

        metadata_cfg = MetadataConfig(**wf_data.get("metadata", {}))
        if metadata_cfg.exe is None:
            metadata_cfg.exe = site_cfg.metadata_exe

        # 4. Job Config merging (Workflow TOML defaults -> CLI Overrides)
        job_data = wf_data.get("job", {})
        if job_overrides:
            job_data.update({k: v for k, v in job_overrides.items() if v is not None})

        # Ensure required fields for JobConfig are present or provided
        job_cfg = JobConfig(**job_data)

        return cls(
            site=site_cfg,
            job=job_cfg,
            larsoft=larsoft_cfg,
            workflow=workflow_cfg,
            run=run_cfg,
            metadata=metadata_cfg,
        )

    def to_dict(self) -> Dict:
        """Deep conversion to dict for legacy compatibility."""

        def _asdict(obj):
            if hasattr(obj, "__dict__"):
                return {k: _asdict(v) for k, v in obj.__dict__.items()}
            elif isinstance(obj, (list, tuple)):
                return [_asdict(i) for i in obj]
            elif isinstance(obj, dict):
                return {k: _asdict(v) for k, v in obj.items()}
            else:
                return obj

        return _asdict(self)
