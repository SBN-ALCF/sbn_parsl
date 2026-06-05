#!/usr/bin/env python3
# Class for generating metadata directives for fcl files

import os
import shutil
import pathlib
import subprocess
from typing import Dict, Optional, Any


POMS_EXE = "sbndpoms_metadata_injector.sh"


class MetadataGenerator:
    """
    Handles the generation of JSON metadata for LArSoft output files.
    This metadata is used for ingestion into the SAM (Sequential Access via Metadata)
    system at FNAL.
    """

    defaults = {
        "inputfclname": "dummy.fcl",
        "mdfclname": "dummy.fcl",
        "mdprojectname": "dummy",
        "mdprojectstage": "gen",
        "mdprojectversion": "v09_78_04",
        "mdprojectsoftware": "sbndcode",
        "mdproductionname": "MCP2023Blike",
        "mdproductiontype": "polaris",
        "mdappversion": "",
        "mdfiletype": "mc",
        "mdappfamily": "art",
        "mdruntype": "physics",
        "mdgroupname": "sbnd",
        "tfilemdjsonname": "",
        "cafname": "caf",
    }

    def __init__(
        self, md_config: Any, fclnames: Optional[Dict] = None, defer_check: bool = False
    ):
        """
        Initializes the generator.

        Args:
            md_config: Either a Config object or a dictionary of metadata settings.
            fclnames: Mapping of StageType names to FHiCL files.
            defer_check: Included for backwards compatibility with workflow scripts.
        """
        self.md_config = md_config
        self.fclnames = fclnames or {}

        # 1. Start with class defaults
        self.metadata = MetadataGenerator.defaults.copy()

        # Determine defaults based on experiment/software/version if available
        larsoft_version = None
        larsoft_software = None
        larsoft_experiment = None

        config_settings = {}

        if hasattr(md_config, "larsoft") and hasattr(md_config.larsoft, "metadata"):
            # It's a Config object
            larsoft = md_config.larsoft
            larsoft_version = larsoft.version
            larsoft_software = getattr(larsoft, "software", None)
            larsoft_experiment = larsoft.experiment

            md_obj = larsoft.metadata
            import dataclasses
            for f in dataclasses.fields(md_obj):
                val = getattr(md_obj, f.name)
                if val is not None:
                    config_settings[f.name] = val
            if hasattr(md_obj, "extra_fields") and isinstance(md_obj.extra_fields, dict):
                config_settings.update(md_obj.extra_fields)
        elif isinstance(md_config, dict):
            # It's a dict
            config_settings = md_config.copy()
            if "experiment" in config_settings:
                larsoft_experiment = config_settings.pop("experiment")
            if "software" in config_settings:
                larsoft_software = config_settings.pop("software")
            if "version" in config_settings:
                larsoft_version = config_settings.pop("version")
        else:
            # It's some other object (e.g. MetadataConfig or MockMD)
            md_obj = md_config
            import dataclasses
            if dataclasses.is_dataclass(md_obj):
                for f in dataclasses.fields(md_obj):
                    val = getattr(md_obj, f.name)
                    if val is not None:
                        config_settings[f.name] = val
                if hasattr(md_obj, "extra_fields") and isinstance(md_obj.extra_fields, dict):
                    config_settings.update(md_obj.extra_fields)
            else:
                # Fallback attributes
                for key in MetadataGenerator.defaults:
                    if hasattr(md_obj, key):
                        val = getattr(md_obj, key)
                        if val is not None:
                            config_settings[key] = val
                if hasattr(md_obj, "exe"):
                    config_settings["exe"] = getattr(md_obj, "exe")
                if hasattr(md_obj, "mdprojectversion"):
                    config_settings["mdprojectversion"] = getattr(md_obj, "mdprojectversion")

        # Resolve exe
        exe = config_settings.pop("exe", POMS_EXE)
        if exe is None:
            exe = POMS_EXE

        # Check path for exe
        path = shutil.which(exe)
        if path is None and not defer_check:
            raise RuntimeError(f"Could not find {exe} in $PATH.")
        self.exe = exe

        # Apply experiment/software defaults to self.metadata if present
        if larsoft_experiment:
            self.metadata["mdgroupname"] = larsoft_experiment
        if larsoft_software:
            self.metadata["mdprojectsoftware"] = larsoft_software
        if larsoft_version:
            self.metadata["mdprojectversion"] = larsoft_version

        # Update metadata with config-specified options
        self.metadata.update(config_settings)

        # Ensure project name is set based on fclnames
        if self.fclnames:
            # copy to resolve absolute paths
            resolved_fclnames = {}
            for stage, fcl in self.fclnames.items():
                resolved_fclnames[stage] = pathlib.Path(fcl).name
            self.fclnames = resolved_fclnames

            # project name is the first fcl in the chain
            first_fcl = list(self.fclnames.values())[0]
            self.metadata["mdprojectname"] = first_fcl.replace(".fcl", "")

        # currently no difference between app and project versions
        if self.metadata.get("mdprojectversion"):
            self.metadata["mdappversion"] = self.metadata["mdprojectversion"]

    @property
    def md(self):
        # Return a compat object with exe and mdprojectversion attributes
        from dataclasses import dataclass
        @dataclass
        class MDCompat:
            exe: str
            mdprojectversion: str
        return MDCompat(exe=self.exe, mdprojectversion=self.metadata.get("mdprojectversion"))

    @property
    def experiment(self):
        return self.metadata.get("mdgroupname", "unknown")

    def metadata_stage(self, stagename):
        """Return metadata for a specific stage"""
        this_metadata = self.metadata.copy()
        this_metadata["inputfclname"] = self.fclnames[stagename]
        this_metadata["mdfclname"] = self.fclnames[stagename]
        this_metadata["mdprojectstage"] = stagename
        if stagename != "caf":
            if "cafname" in this_metadata:
                del this_metadata["cafname"]

        return this_metadata

    def metadata_fcl(self, fclname):
        """Return metadata for a specific fcl"""
        # reverse dict lookup
        stagename = list(self.fclnames.keys())[
            list(self.fclnames.values()).index(fclname)
        ]
        return self.metadata_stage(stagename)

    def run(self, filename, fcl="", stage="", check_exists=True):
        parts = self.run_cmd_parts(filename, fcl, stage, check_exists)
        subprocess.run(parts)

    def run_cmd(self, filename, fcl="", stage="", check_exists=True):
        parts = self.run_cmd_parts(filename, fcl, stage, check_exists)
        return " ".join(parts)

    def run_cmd_parts(self, filename, fcl="", stage="", check_exists=True):
        """Generate command to run POMS utility with metadata"""
        if fcl == "" and stage == "":
            raise RuntimeError(
                "run method requires either a fcl or stage key word argument"
            )

        if stage != "":
            fcl = self.fclnames[stage]

        if fcl:
            fcl = os.path.basename(fcl)

        if fcl not in self.fclnames.values():
            raise ValueError(
                f"Attempt to run metadata generation with {fcl} which has no corresponding stage."
            )

        fclfilepath = pathlib.Path(fcl)
        # Note: if check_exists is True, we check the local path. But in dry_run / components context,
        # it is often run with check_exists=False or defer_check because fcl is not locally present yet
        if check_exists and not fclfilepath.is_file():
            raise ValueError(f"{fcl} does not exist.")

        m = self.metadata_fcl(fcl)
        if "tfilemdjsonname" in m:
            m["tfilemdjsonname"] = filename

        args = []
        for key, value in m.items():
            args.append(f"--{key}")
            args.append(f"{value}")
        args.insert(0, self.exe)
        return args


if __name__ == "__main__":
    # test
    settings = {}
    fcls = {"gen": "myfile.fcl"}
    m = MetadataGenerator(settings, fcls, defer_check=True)
    print(m.metadata_stage("gen"))
    print(m.metadata_fcl("myfile.fcl"))
    print(m.run_cmd("myfile.json", "myfile.fcl", check_exists=False))
