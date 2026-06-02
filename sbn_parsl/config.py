from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import tomllib
import pathlib
import socket
import json
import hashlib


class AppConfigNamespace:
    """Dynamically wraps configuration dictionaries into dot-accessible attributes."""

    def __init__(self, data: Dict[str, Any]):
        for k, v in data.items():
            if isinstance(v, dict):
                setattr(self, k, AppConfigNamespace(v))
            else:
                setattr(self, k, v)

    def to_dict(self) -> Dict[str, Any]:
        res = {}
        for k, v in self.__dict__.items():
            if isinstance(v, AppConfigNamespace):
                res[k] = v.to_dict()
            else:
                res[k] = v
        return res


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
    worker_init: Any = ""
    virtual_env: Optional[str] = None


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
    initialize_logging: bool = False


@dataclass
class MetadataConfig:
    """
    Metadata injection configuration parameters.
    """

    exe: Optional[str] = None
    mdprojectversion: Optional[str] = None


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
    env_file: str = ""
    container_path: str = ""
    larsoft_top: str = ""
    simulation_inputs: str = "/lus/flare/projects/neutrinoGPU/simulation_inputs_striped"
    metadata: MetadataConfig = field(default_factory=MetadataConfig)


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
        data = dict(data)
        if "fcl" in data:
            raise ValueError(
                "The singular [workflow.fcl] heading is no longer supported. Please use the plural [workflow.fcls] heading instead."
            )

        known_fields = {name for name in cls.__dataclass_fields__ if name != "extra"}
        kwargs = {k: v for k, v in data.items() if k in known_fields}
        extra = {k: v for k, v in data.items() if k not in known_fields}
        return cls(**kwargs, extra=extra)


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
    workflow: WorkflowConfig
    run: RunConfig
    larsoft: Optional[LArSoftConfig] = None

    def get_science_hash(self) -> str:
        """
        Returns a hash representing the 'Science Identity' of this run.
        Excludes transient job settings, site infrastructure, and nsubruns.
        """
        # We include larsoft, workflow, and metadata
        # Exclude metadata from larsoft dict to preserve stable science hash identity representation
        identity = {
            "workflow": self.workflow.__dict__,
        }

        # Add active dynamic apps
        active_apps = getattr(self, "_active_dynamic_apps", [])
        if active_apps:
            apps_identity = {}
            for app_name in sorted(active_apps):
                if app_name == "larsoft":
                    continue
                apps_identity[app_name] = getattr(self, app_name).to_dict()
            if apps_identity:
                identity["apps"] = apps_identity

        if self.larsoft is not None:
            identity["larsoft"] = {
                k: v for k, v in self.larsoft.__dict__.items() if k != "metadata"
            }
            identity["metadata"] = self.larsoft.metadata.__dict__

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
        if "metadata" in data:
            del data["metadata"]

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
        sections_to_write = [
            "site",
            "job",
            "larsoft",
            "larsoft.metadata",
            "workflow",
            "workflow.fcls",
            "run",
        ]
        active_apps = getattr(self, "_active_dynamic_apps", [])
        for app_name in sorted(active_apps):
            sections_to_write.append(f"app.{app_name}")

        for sec in sections_to_write:
            sec_parts = sec.split(".")
            sec_data = data
            for part in sec_parts:
                if isinstance(sec_data, dict):
                    sec_data = sec_data.get(part, None)
                else:
                    sec_data = None

            if not sec_data:
                continue

            lines.append(f"[{sec}]")
            for k, v in sorted(sec_data.items()):
                if v is None:
                    continue
                if sec == "larsoft" and k == "metadata":
                    continue
                if sec == "workflow" and k == "fcls":
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

        # Merge [site] and [app.larsoft] sections from site config
        # We require structured [site] and/or [app.larsoft] sections
        has_site = "site" in site_data
        has_app_larsoft = (
            "app" in site_data
            and isinstance(site_data["app"], dict)
            and "larsoft" in site_data["app"]
        )
        if not has_site and not has_app_larsoft:
            raise ValueError(
                f"Site configuration file '{site_path.name}' is missing required '[site]' and '[app.larsoft]' structured headers."
            )

        merged_site_data = {}
        merged_site_data.update(site_data.get("site", {}))
        if has_app_larsoft:
            merged_site_data.update(site_data["app"]["larsoft"])

        # Only pass valid fields to SiteConfig
        site_fields = {f.name for f in SiteConfig.__dataclass_fields__.values()}
        filtered_site_data = {
            k: v for k, v in merged_site_data.items() if k in site_fields
        }
        site_cfg = SiteConfig(**filtered_site_data)

        # 3. Load workflow TOML
        with open(workflow_path, "rb") as f:
            wf_data = tomllib.load(f)

        if "fcls" in wf_data or "fcl" in wf_data:
            raise ValueError(
                "Top-level [fcl] or [fcls] blocks are no longer supported. Please use the nested [workflow.fcls] block."
            )

        # Extract larsoft configurations from [app.larsoft]
        site_app_section = site_data.get("app", {})
        wf_app_section = wf_data.get("app", {})

        larsoft_cfg = None
        if "larsoft" in wf_app_section and isinstance(wf_app_section["larsoft"], dict):
            site_larsoft = site_app_section.get("larsoft", {})
            wf_larsoft = wf_app_section.get("larsoft", {})

            larsoft_data = {}
            for key in ["container_path", "larsoft_top", "simulation_inputs"]:
                if key in site_larsoft:
                    larsoft_data[key] = site_larsoft[key]

            # Extract nested metadata defaults and overrides
            site_metadata = site_larsoft.get("metadata", {})
            wf_metadata = wf_larsoft.get("metadata", {})

            merged_metadata = {}
            # Apply site-level metadata defaults
            merged_metadata.update(site_metadata)
            # Apply workflow-level overrides
            merged_metadata.update(wf_metadata)

            metadata_cfg = MetadataConfig(**merged_metadata)

            # Now update with whatever is in workflow larsoft config
            filtered_wf_larsoft = {
                k: v for k, v in wf_larsoft.items() if k != "metadata"
            }
            larsoft_data.update(filtered_wf_larsoft)
            larsoft_data["metadata"] = metadata_cfg

            larsoft_cfg = LArSoftConfig(**larsoft_data)

        # Load workflow config
        wf_raw_data = wf_data.get("workflow", {})

        workflow_cfg = WorkflowConfig.from_dict(wf_raw_data)

        run_data = wf_data.get("run", {})
        if run_overrides:
            run_data.update({k: v for k, v in run_overrides.items() if v is not None})
        run_cfg = RunConfig(**run_data)

        # 4. Job Config merging (Workflow TOML defaults -> CLI Overrides)
        job_data = wf_data.get("job", {})
        if job_overrides:
            job_data.update({k: v for k, v in job_overrides.items() if v is not None})

        # Ensure required fields for JobConfig are present or provided
        job_cfg = JobConfig(**job_data)

        cfg = cls(
            site=site_cfg,
            job=job_cfg,
            workflow=workflow_cfg,
            run=run_cfg,
        )

        # 5. Dynamic App Config parsing and merging
        active_apps = []
        if larsoft_cfg is not None:
            setattr(cfg, "larsoft", larsoft_cfg)
            active_apps.append("larsoft")

        for app_name, wf_app_data in wf_app_section.items():
            if app_name == "larsoft":
                continue
            if isinstance(wf_app_data, dict):
                merged_app_data = {}
                if app_name in site_app_section and isinstance(
                    site_app_section[app_name], dict
                ):
                    merged_app_data.update(site_app_section[app_name])
                merged_app_data.update(wf_app_data)

                app_cfg = AppConfigNamespace(merged_app_data)
                setattr(cfg, app_name, app_cfg)
                active_apps.append(app_name)

        setattr(cfg, "_active_dynamic_apps", active_apps)
        return cfg

    def to_dict(self) -> Dict:
        """Deep conversion to dict for legacy compatibility."""

        def _asdict(obj):
            if isinstance(obj, AppConfigNamespace):
                return obj.to_dict()
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
                return {
                    k: _asdict(v)
                    for k, v in obj.__dict__.items()
                    if not k.startswith("_")
                }
            elif isinstance(obj, (list, tuple)):
                return [_asdict(i) for i in obj]
            elif isinstance(obj, dict):
                return {k: _asdict(v) for k, v in obj.items()}
            else:
                return obj

        raw_dict = _asdict(self)
        if "larsoft" in raw_dict and raw_dict["larsoft"] is None:
            del raw_dict["larsoft"]
        active_apps = getattr(self, "_active_dynamic_apps", [])
        if active_apps:
            raw_dict["app"] = {}
            for app_name in active_apps:
                if app_name in raw_dict:
                    raw_dict["app"][app_name] = raw_dict.pop(app_name)
        return raw_dict
