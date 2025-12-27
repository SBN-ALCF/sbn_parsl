#!/usr/bin/env python

# This workflow generates full MC events, creates a CAF file from the events,
# and does so for different detsim variations.

import sys
import json
import pathlib
import functools
import itertools
from typing import Dict, List

from sbn_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor, \
        DefaultStageTypes
from sbn_parsl.metadata import MetadataGenerator
from sbn_parsl.templates import CMD_TEMPLATE_SPACK, CMD_TEMPLATE_CONTAINER
from sbn_parsl.components import mc_runfunc_sbnd
from sbn_parsl.app import entry_point


class Reco2FromGenExecutor(WorkflowExecutor):
    """Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.detsim_path = pathlib.Path(settings['workflow']['detsim_path'])
        self.subruns_per_caf = settings['workflow']['subruns_per_caf']
        self.default_fcls = self.fcls['cv']
        self.variations = [key for key in self.fcls.keys() if key != 'cv']

        # fill in the default fcls for each variation
        for var in self.variations:
            for key, val in self.default_fcls.items():
                if key in self.fcls[var]:
                    continue
                self.fcls[var][key] = val

        self.meta = {
            key: MetadataGenerator(
                settings['metadata'], fcls, defer_check=True) \
            for key, fcls in self.fcls.items()
        }

        # the default (CV) stage order
        self.stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4, DefaultStageTypes.DETSIM, \
                            DefaultStageTypes.RECO1, DefaultStageTypes.RECO2, DefaultStageTypes.CAF]
        # this is the stage order for adding new variations on existing detsim files
        # self.stage_order = [DefaultStageTypes.DETSIM, DefaultStageTypes.RECO1, DefaultStageTypes.RECO2, DefaultStageTypes.CAF]

        # when we construct the scrub stage, this will let the workflow know to
        # treat a detsim stage as the parent of scrub, instead of a fixed input file
        self.scrub_stage_order = [DefaultStageTypes.DETSIM, DefaultStageTypes.SCRUB]

        # the stage order for the variations
        self.var_stage_order = [DefaultStageTypes.SCRUB, DefaultStageTypes.DETSIM, \
                                DefaultStageTypes.RECO1, DefaultStageTypes.RECO2, DefaultStageTypes.CAF]

        common_args = {
                'template': CMD_TEMPLATE_CONTAINER,
                'executor': self,
        }
        self.runfuncs = {
                key: functools.partial(mc_runfunc_sbnd, meta=self.meta[key], label=key, **common_args) \
                        for key in self.fcls.keys()
        }
        self.runfuncs_no_meta = {
                key: functools.partial(mc_runfunc_sbnd, meta=None, label=key, **common_args) \
                        for key in self.fcls.keys()
        }

    def file_generator(self):
        path_generators = [self.detsim_path.rglob('detsim*.root')]
        generator = itertools.chain(*path_generators)
        for f in generator:
            yield f

    def setup_single_workflow(self, iteration: int, input_files=None, last_file=None):
        var_dirs = {}
        var_runfuncs = {}
        for var in self.variations:
            var_dirs[var] = self.output_dir / var

        workflow = Workflow(self.stage_order, default_fcls=self.default_fcls)
        # CAF for each variation
        var_caf_stages = {}
        for var in self.variations:
            svar = Stage(DefaultStageTypes.CAF, stage_order=self.var_stage_order)
            svar.run_dir = get_caf_dir(var_dirs[var], iteration)
            svar.runfunc = self.runfuncs[var]
            var_caf_stages[var] = svar
            workflow.add_final_stage(svar)

        for i in range(self.subruns_per_caf):
            inst = iteration * self.subruns_per_caf + i

            for var, svar in var_caf_stages.items():
                svar_reco2 = Stage(DefaultStageTypes.RECO2, stage_order=self.var_stage_order)
                svar_reco2.runfunc = self.runfuncs_no_meta[var]
                svar_reco2.run_dir = get_subrun_dir(var_dirs[var], inst)

                svar_reco1 = Stage(DefaultStageTypes.RECO1, stage_order=self.var_stage_order)

                svar_detsim = Stage(DefaultStageTypes.DETSIM, stage_order=self.var_stage_order)
                svar_detsim.runfunc = self.runfuncs_no_meta[var]
                svar_scrub = Stage(DefaultStageTypes.SCRUB, stage_order=self.var_stage_order)
                svar_scrub.runfunc = self.runfuncs_no_meta[var]
                svar_scrub.add_input_file(input_files[i])

                svar.add_parents(svar_reco2, self.fcls[var])
                svar_reco2.add_parents(svar_reco1, self.fcls[var])
                svar_reco1.add_parents(svar_detsim, self.fcls[var])
                svar_detsim.add_parents(svar_scrub, self.fcls[var])

                # combine scrub with detsim
                # combine detsim with reco1
                # combine reco2 with caf
                svar_reco2.combine = True
                svar_scrub.combine = True
                svar_detsim.combine = True

        return workflow


    def _setup_single_workflow(self, iteration: int, input_files=None, last_file=None):
        cv_dir = self.output_dir / 'cv'

        var_dirs = {}
        var_runfuncs = {}
        for var in self.variations:
            var_dirs[var] = self.output_dir / var

        workflow = Workflow(self.stage_order, default_fcls=self.default_fcls)
        s = Stage(DefaultStageTypes.CAF)
        s.run_dir = get_caf_dir(cv_dir, iteration)
        s.runfunc = self.runfuncs['cv']
        workflow.add_final_stage(s)

        # CAF for each variation
        var_caf_stages = {}
        for var in self.variations:
            svar = Stage(DefaultStageTypes.CAF, stage_order=self.var_stage_order)
            svar.run_dir = get_caf_dir(var_dirs[var], iteration)
            svar.runfunc = self.runfuncs[var]
            var_caf_stages[var] = svar
            workflow.add_final_stage(svar)

        for i in range(self.subruns_per_caf):
            inst = iteration * self.subruns_per_caf + i
            # create reco2 file from MC, only need to specify the last stage
            # since there are no inputs
            scv_reco2 = Stage(DefaultStageTypes.RECO2)
            scv_reco2.run_dir = get_subrun_dir(cv_dir, inst)

            scv_reco1 = Stage(DefaultStageTypes.RECO1)
            scv_detsim = Stage(DefaultStageTypes.DETSIM)

            scv_scrub = Stage(DefaultStageTypes.SCRUB, stage_order=self.scrub_stage_order)
            scv_scrub.run_dir = get_subrun_dir(cv_dir, inst)
            scv_scrub.runfunc = self.runfuncs['cv']

            s.add_parents(scv_reco2, workflow.default_fcls)
            scv_reco2.add_parents(scv_reco1, workflow.default_fcls)
            scv_reco1.add_parents(scv_detsim, workflow.default_fcls)
            scv_scrub.add_parents(scv_detsim, workflow.default_fcls)
            # scv_scrub.add_input_file(input_files[i])

            for var, svar in var_caf_stages.items():
                svar_reco2 = Stage(DefaultStageTypes.RECO2, stage_order=self.var_stage_order)
                svar_reco2.fcl = self.fcls[var]['reco2']
                svar_reco2.run_dir = get_subrun_dir(var_dirs[var], inst)

                svar_reco1 = Stage(DefaultStageTypes.RECO1, stage_order=self.var_stage_order)
                svar_reco1.fcl = self.fcls[var]['reco1']

                svar_detsim = Stage(DefaultStageTypes.DETSIM, stage_order=self.var_stage_order)
                svar_detsim.fcl = self.fcls[var]['detsim']

                svar_g4 = Stage(DefaultStageTypes.G4, stage_order=self.var_stage_order)
                svar_g4.fcl = self.fcls[var]['g4']

                svar.add_parents(svar_reco2, self.fcls[var])
                svar_reco2.add_parents(svar_reco1, self.fcls[var])
                svar_reco1.add_parents(svar_detsim, self.fcls[var])
                svar_detsim.add_parents(scv_scrub, self.fcls[var])
                # svar_g4.add_parents(scv_scrub, self.fcls[var])

        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{(subrun//1000):06d}" / f"{(subrun//100):06d}" / f"subrun_{subrun:06d}"

def get_caf_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/caf/XXXXXX"""
    return prefix / f"{(subrun//1000):06d}" / 'caf' / f"subrun_{subrun:06d}"

if __name__ == '__main__':
    entry_point(sys.argv, Reco2FromGenExecutor)
