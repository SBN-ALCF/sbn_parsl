#!/usr/bin/env python3
# Class for generating metadata directives for fcl files

import os
from typing import Dict, Optional, Any


POMS_EXE = "sbndpoms_metadata_injector.sh"


class MetadataGenerator:
    """
    Handles the generation of JSON metadata for LArSoft output files.
    This metadata is used for ingestion into the SAM (Sequential Access via Metadata)
    system at FNAL.
    """

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

        # Determine if we have a full Config or just the metadata part
        if hasattr(md_config, "metadata"):
            self.md = md_config.metadata
            self.experiment = md_config.larsoft.experiment
        elif isinstance(md_config, dict):
            # Fallback for when just the metadata dict is passed
            from dataclasses import dataclass

            @dataclass
            class MockMD:
                exe: Optional[str] = None
                mdprojectversion: Optional[str] = None

            self.md = MockMD(
                exe=md_config.get("exe"),
                mdprojectversion=md_config.get("mdprojectversion"),
            )
            self.experiment = md_config.get("experiment", "unknown")
        else:
            self.md = md_config
            self.experiment = "unknown"

    def run_cmd(self, out_json, fcl, check_exists=True):
        """
        Generates a bash command to run the metadata injector script.
        """
        if self.md.exe is None:
            return ""

        if check_exists and not os.path.isfile(self.md.exe):
            print(f"Warning: Metadata injector not found at {self.md.exe}")
            return ""

        items = [
            self.md.exe,
            f"--json {out_json}",
            f"--fcl {fcl}",
            f"--project {self.experiment}",
            f"--version {self.md.mdprojectversion}",
        ]

        return " ".join(items)


if __name__ == "__main__":
    # test
    settings = {}
    fcls = {"gen": "myfile.fcl"}
    m = MetadataGenerator(settings, fcls)
    print(m.metadata_stage("gen"))
    print(m.metadata_fcl("myfile.fcl"))
    print(m.run_cmd("myfile.fcl", check_exists=False))
