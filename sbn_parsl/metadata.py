#!/usr/bin/env python3
# Class for generating metadata directives for fcl files

import shutil
import pathlib
import subprocess
from typing import List, Dict

POMS_EXE = "sbndpoms_metadata_injector.sh"

class MetadataGenerator:
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

    def __init__(self, settings_: Dict, fclnames: Dict, exe=POMS_EXE, defer_check=False):
        # allow constructor to defer check for exe. This allows us to generate
        # metadata commands without modifying our local PATH, but we have to make
        # sure to modify PATH by the time the command actually runs

        # copy since we will del, caller may re-use settings
        settings = settings_.copy()
        try:
            exe = settings['exe']
            del settings['exe']
        except KeyError:
            print(f'Warning: No metadata executable specified in settings. Using "{exe}"')
            pass

        path = shutil.which(exe)
        if path is None and not defer_check:
            raise RuntimeError(f"Could not find {exe} in $PATH.")
        self.exe = exe

        if len(fclnames) == 0:
            raise ValueError("Need to provide at least one stagename and corresponding fcl filename")

        # copy to resolve absolute paths
        self.fclnames = fclnames.copy()
        for stage, fcl in self.fclnames.items():
            self.fclnames[stage] = pathlib.Path(fcl).name

        self.metadata = MetadataGenerator.defaults.copy()
        self.metadata.update(settings)

        # project name is the first fcl in the chain
        self.metadata["mdprojectname"] = list(fclnames.values())[0].replace('.fcl', '')

        # currently no difference between app and project versions
        self.metadata["mdappversion"] = self.metadata["mdprojectversion"]

    def metadata_stage(self, stagename):
        """ Return metadata for a specific stage """
        this_metadata = self.metadata.copy()
        this_metadata["inputfclname"] = self.fclnames[stagename]
        this_metadata["mdfclname"] = self.fclnames[stagename]
        this_metadata["mdprojectstage"] = stagename
        if stagename != "caf":
            del this_metadata["cafname"]

        return this_metadata

    def metadata_fcl(self, fclname):
        """ Return metadata for a specific fcl """
        # reverse dict lookup
        stagename = list(self.fclnames.keys())[
            list(self.fclnames.values()).index(fclname)
        ]
        return self.metadata_stage(stagename)

    def run(self, filename, fcl='', stage='', check_exists=True):
        parts = self.run_cmd_parts(filename, fcl, stage, check_exists)
        subprocess.run(parts)

    def run_cmd(self, filename, fcl='', stage='', check_exists=True):
        parts = self.run_cmd_parts(filename, fcl, stage, check_exists)
        return ' '.join(parts)

    def run_cmd_parts(self, filename, fcl='', stage='', check_exists=True):
        """ Generate command to run POMS utility with metadata """
        if fcl == '' and stage == '':
            raise RuntimeError("run method requires either a fcl or stage key word argument")

        if stage != '':
            fcl = self.fclnames[stage]

        if fcl not in self.fclnames.values():
            raise ValueError(f"Attempt to run metadata generation with {fcl} which has no corresponding stage.")

        fclfilepath = pathlib.Path(fcl)
        if check_exists and not fclfilepath.is_file():
            raise ValueError(f"{fcl} does not exist.")

        m = self.metadata_fcl(fcl)
        if "tfilemdjsonname" in m:
            m["tfilemdjsonname"] = filename

        args = []
        for key, value in m.items():
            args.append(f'--{key}')
            args.append(f'{value}')
        args.insert(0, self.exe)
        return args


if __name__ == '__main__':
    # test
    settings = {}
    fcls = {'gen': 'myfile.fcl'}
    m = MetadataGenerator(settings, fcls)
    print(m.metadata_stage('gen'))
    print(m.metadata_fcl('myfile.fcl'))
    print(m.run_cmd('myfile.fcl', check_exists=False))
