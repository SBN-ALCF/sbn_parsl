#!/usr/bin/env python3
# Class for generating metadata directives for fcl files

import os
import shutil
import pathlib
import subprocess
from typing import Dict, Optional

from sbn_parsl.config import Config


POMS_EXE = "sbndpoms_metadata_injector.sh"


class MetadataGenerator:
    """
    Handles the generation of JSON metadata for LArSoft output files.
    This metadata is used for ingestion into the SAM (Sequential Access via Metadata) 
    system at FNAL.
    """

    def __init__(self, cfg: Config, fclnames: Optional[Dict] = None):
        """Initializes the generator with project-specific settings."""
        self.cfg = cfg
        self.md = cfg.metadata
        self.fclnames = fclnames or {}

    def run_cmd(self, out_json, fcl, check_exists=True):
        """
        Generates a bash command to run the metadata injector script.

        Args:
            out_json: Filename for the resulting metadata JSON.
            fcl: The FHiCL file used to produce the root file.
            check_exists: If True, only generates the command if the injector exists.
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
            f"--project {self.cfg.larsoft.experiment}",
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
