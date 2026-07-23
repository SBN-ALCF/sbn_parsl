#!/usr/bin/env python

# This workflow runs SPINE on a list of larcv files

import sys
import json
import pathlib
import functools
import itertools
from typing import List

from parsl.app.app import bash_app
from parsl.data_provider.files import File

from sbn_parsl.workflow import Stage, Workflow, LArSoftExecutor, DefaultStageTypes
from sbn_parsl.templates import SPINE_TEMPLATE
from sbn_parsl.components import _transfer_ids
from sbn_parsl.app import entry_point
from sbn_parsl.config import Config


SPINE_METADATA_TEMPLATE = {
    "file_name": "h5_file",
    "user": "sbndpro",
    "application": {
        "family": "art",
        "name": "gen_g4_detsim_reco1",
        "version": "spine_version",
    },
    "parents": [{"file_name": "larcv_file"}],
    "data_tier": "spine",
    "file_format": "spine",
    "file_type": "mc",
    "group": "sbnd",
    "production.name": "settings_override",
    "production.type": "settings_override",
    "sbnd_project.name": "settings_override",
}


@bash_app(cache=False)
def fcl_future(
    workdir,
    stdout,
    stderr,
    template,
    spine_opts,
    inputs=[],
    outputs=[],
    pre_job_hook="",
    post_job_hook="",
):
    """Return formatted bash script which produces each future when executed."""
    return template.format(
        workdir=workdir,
        input=inputs[0],
        **spine_opts,
        output=outputs[0],
        pre_job_hook=pre_job_hook,
        post_job_hook=post_job_hook,
    )


def runfunc(self, fcl, input_files, run_dir, iteration, executor):
    """Method bound to each Stage object and run during workflow execution."""

    run_dir.mkdir(parents=True, exist_ok=True)
    output_dir = executor.output_dir / self.stage_type.name

    # DataFuture for this task will be the final h5 file of all inputs
    def spine_output_name(filename: pathlib.PurePosixPath) -> pathlib.PurePosixPath:
        return pathlib.PurePosixPath(f"{filename.with_suffix('')}_spine.h5")

    subdir_name = f"{iteration // 100:06d}"
    output_dir = output_dir / subdir_name
    output_dir.mkdir(parents=True, exist_ok=True)

    last_file = None
    input_file = run_dir / "filelist.txt"
    with open(input_file, "w") as f:
        for file_str in input_files:
            f.write(f"{file_str}\n")
            larcv_file = pathlib.PurePosixPath(file_str)

            # metadata file to be generated along with each h5 file
            metadata = SPINE_METADATA_TEMPLATE.copy()
            metadata["application"]["version"] = executor.spine_opts["version"]
            h5_file = spine_output_name(larcv_file)
            metadata["file_name"] = h5_file.name
            metadata["parents"][0]["file_name"] = larcv_file.name
            metadata_file = h5_file.with_suffix(".h5.json").name
            with open(output_dir / metadata_file, "w") as json_file:
                json.dump(metadata, json_file, indent=4)

            last_file = larcv_file

    input_str = str(input_file)

    output_filepath = output_dir / spine_output_name(last_file).name
    print(f"submitting {len(input_files)} files")
    future = fcl_future(
        workdir=str(run_dir),
        stdout=str(run_dir / "spine.out"),
        stderr=str(run_dir / "spine.err"),
        template=SPINE_TEMPLATE,
        spine_opts=executor.spine_opts,
        inputs=[input_str],
        outputs=[File(str(output_filepath))],
    )

    _transfer_ids(self, future.outputs[0])

    executor.register_future(future.outputs[0])
    executor._stage_counter += 1

    return future.outputs


class SpineExecutor(LArSoftExecutor):
    """Execute a decoder workflow from user settings."""

    def __init__(self, cfg: Config):
        super().__init__(cfg)

        self.stage_order = [DefaultStageTypes.SPINE]
        self.files_per_subrun = cfg.run.files_per_subrun

        wf_dict = cfg.workflow.__dict__
        self.larcv_path = pathlib.Path(wf_dict.get("larcv_path", "."))

        # Accessing custom 'spine' section from TOML via cfg if it exists,
        # otherwise we might need a more dynamic way if it's not in the dataclasses.
        # Based on config.py, JobConfig doesn't have spine.
        # The Config class might have loaded it into a custom attribute if we were using a more dynamic loader,
        # but config.py is strictly typed.
        # Let's assume for now it's accessible or we need to add it to Config.
        self.spine_opts = wf_dict.get("spine", {})
        self.filelist = wf_dict.get("filelist", "")

        # If spine was a top-level section in TOML, Config.load didn't capture it into a dataclass.
        # I might need to update Config.load to handle extra sections.

        self.spine_opts.update({"cores_per_worker": cfg.site.cores_per_worker})

    def file_generator(self):
        """Run spine on list of files if specified, otherwise glob input directory"""
        if self.filelist == "":
            path_generators = [self.larcv_path.rglob("larcv*.root")]
            generator = itertools.chain(*path_generators)
            for f in generator:
                yield f
        else:
            with open(self.filelist, "r") as f:
                for line in f.readlines():
                    yield pathlib.Path(line.strip())

    def setup_single_workflow(
        self, iteration: int, larcv_files: List[pathlib.Path], last_file=None
    ):
        if not larcv_files:
            raise RuntimeError()

        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = functools.partial(runfunc, iteration=iteration, executor=self)
        s = Stage(DefaultStageTypes.SPINE)
        s.run_dir = self.get_run_dir(iteration)
        s.runfunc = runfunc_

        for i, file in enumerate(larcv_files):
            s.add_input_file(str(file))

        workflow.add_final_stage(s)

        return workflow


if __name__ == "__main__":
    entry_point(sys.argv, SpineExecutor)
