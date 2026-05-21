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

    # Machine / Site Parameters
    name: str
    cpus_per_node: int
    cores_per_worker: int
    max_futures: int = 60000
    scheduler_options: str = ""
    launcher_options: str = ""
    cpu_affinity: str = "none"
    available_accelerators: Any = None
    pbs_filesystems: str = "home"
    worker_init: str = ""
    worker_venv_name: str = "sbn"

    # LArSoft / Software Parameters
    metadata_exe: Optional[str] = None


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
    container_path: str = ""
    larsoft_top: str = ""
    simulation_inputs: str = "/lus/flare/projects/neutrinoGPU/simulation_inputs_striped"


@dataclass
class WorkflowConfig:
    """
    Top-level workflow configuration logic.
    Holds FCL stage definitions and logic-specific parameters like keep fractions.
    """

    subruns_per_caf: int = 20
    full_keep_fraction: float = 1.0
    fcls: Dict[str, str] = field(default_factory=dict)

    # Extra fields for workflow-specific parameters
    extra: Dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, name):
        if name in self.extra:
            return self.extra[name]
        raise AttributeError(f"'WorkflowConfig' object has no attribute '{name}'")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkflowConfig":
        """Load from dict, separating known fields from extras."""
        known_fields = {name for name in cls.__dataclass_fields__ if name != "extra"}
        kwargs = {k: v for k, v in data.items() if k in known_fields}
        extra = {k: v for k, v in data.items() if k not in known_fields}
        return cls(**kwargs, extra=extra)


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

    output: str  # Set dynamically from CLI
    nsubruns: int = 1
    runinfo: Optional[str] = None
    daos: bool = False
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

        def _serialize(val) -> str:
            if isinstance(val, str):
                return json.dumps(val)
            elif isinstance(val, bool):
                return str(val).lower()
            elif isinstance(val, dict):
                parts = []
                for dk, dv in val.items():
                    if dv is not None:
                        parts.append(f"{dk} = {_serialize(dv)}")
                return f"{{ {', '.join(parts)} }}"
            elif isinstance(val, (list, tuple)):
                parts = []
                for item in val:
                    if item is not None:
                        parts.append(_serialize(item))
                return f"[{', '.join(parts)}]"
            else:
                return str(val)

        lines = []
        for section, content in sorted(data.items()):
            lines.append(f"[{section}]")
            for k, v in sorted(content.items()):
                if v is None:
                    continue
                lines.append(f"{k} = {_serialize(v)}")
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

        # Merge [site] and [larsoft] sections from site config
        # We also support a flat structure for backward compatibility during transition
        merged_site_data = {}
        if "site" in site_data or "larsoft" in site_data:
            merged_site_data.update(site_data.get("site", {}))
            merged_site_data.update(site_data.get("larsoft", {}))
        else:
            merged_site_data = site_data

        # Only pass valid fields to SiteConfig
        site_fields = {f.name for f in SiteConfig.__dataclass_fields__.values()}
        filtered_site_data = {k: v for k, v in merged_site_data.items() if k in site_fields}
        site_cfg = SiteConfig(**filtered_site_data)

        # 3. Load workflow TOML
        with open(workflow_path, "rb") as f:
            wf_data = tomllib.load(f)

        # Get defaults from site config's [larsoft] section
        site_larsoft = site_data.get("larsoft", {}) if "larsoft" in site_data else site_data

        # Merge site-specific larsoft defaults with workflow's larsoft config
        larsoft_data = {}
        for key in ["container_path", "larsoft_top", "simulation_inputs"]:
            if key in site_larsoft:
                larsoft_data[key] = site_larsoft[key]

        # Now update with whatever is in workflow larsoft config
        larsoft_data.update(wf_data.get("larsoft", {}))

        larsoft_cfg = LArSoftConfig(**larsoft_data)

        # Load workflow config, merging top-level fcls if present
        wf_raw_data = wf_data.get("workflow", {})
        if "fcls" in wf_data:
            if "fcls" not in wf_raw_data:
                wf_raw_data["fcls"] = {}
            wf_raw_data["fcls"].update(wf_data["fcls"])

        workflow_cfg = WorkflowConfig.from_dict(wf_raw_data)

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
            if isinstance(obj, WorkflowConfig):
                # Flatten the 'extra' dict into the main dictionary
                res = {}
                for k, v in obj.__dict__.items():
                    if k == "extra":
                        if isinstance(v, dict):
                            for ek, ev in v.items():
                                res[ek] = _asdict(ev)
                    else:
                        res[k] = _asdict(v)
                return res
            elif hasattr(obj, "__dict__"):
                return {k: _asdict(v) for k, v in obj.__dict__.items()}
            elif isinstance(obj, (list, tuple)):
                return [_asdict(i) for i in obj]
            elif isinstance(obj, dict):
                return {k: _asdict(v) for k, v in obj.items()}
            else:
                return obj

        return _asdict(self)
