import os
import subprocess
import argparse
import pathlib
import json
from datetime import datetime

import parsl

from sbn_parsl.workflow import WorkflowExecutor
from sbn_parsl.utils import create_default_useropts, create_parsl_config
from sbn_parsl.dfk_hacks import apply_hacks


def entry_point(argv, wfe_class):
    """Provide common options & setup for sbn_parsl workflows."""
    parser = argparse.ArgumentParser(prog='sbn_parsl')
    parser.add_argument('settings', help='Path to JSON settings file') 
    parser.add_argument('-o', '--output-dir', help='Directory for outputs')
    parser.add_argument('-l', '--local', action='store_true', help='Use local provider instead of PBS for running within an existing node reservation. NOTE: Reduces worker node count by 1 for multi-node jobs') 
    parser.add_argument('-c', '--cycle', help='Cycle workflow submission such that this number of workflows must finish completely before more are submitted. The optimal choice is usually the total number of workers (nodes * CPUs/node) divided by the number of tasks started by a single workflow')
    parser.set_defaults(local=False)
    args = parser.parse_args()

    user_opts = create_default_useropts()

    # update user_opts from command line options & settings file
    # settings dictionary is also passed to WorkflowExecutor below
    with open(args.settings, 'r') as f:
        settings = json.load(f)

    if args.output_dir is not None:
        settings['run']['output'] = args.output_dir

    runinfo_dir = pathlib.Path(settings['run']['output']) / 'runinfo'
    user_opts['run_dir'] = str(runinfo_dir)
    runinfo_dir.mkdir(parents=True, exist_ok=True)

    cycle = -1
    if args.cycle is not None:
        cycle = int(args.cycle)

    user_opts.update(settings['queue'])

    # if we are local, submit via qsub unless we are already on a compute node
    if args.local:
        if os.environ.get('PBS_NODEFILE') is None:
            # qsub & return
            cmd_dir = runinfo_dir / 'cmd'
            job_name = f'parsl.{pathlib.Path(argv[0]).stem}.{datetime.now().strftime("%Y%m%d%H%M%S")}'

            hostfile_cmd = ''
            if user_opts['nodes_per_block'] > 1:
                hostfile_cmd = HOSTFILE_CMD_EXCLUDE_SELF
            
            submit_script_dir = cmd_dir / 'submit_scripts'
            submit_script_dir.mkdir(parents=True, exist_ok=True)

            template = LOCAL_TEMPLATE.format(
                job_name=job_name,
                workflow=argv[0],
                out_dir=settings['run']['output'],
                cmd_dir=str(cmd_dir),
                settings=args.settings,
                stdout=str(submit_script_dir / f'{job_name}.stdout'),
                stderr=str(submit_script_dir / f'{job_name}.stderr'),
                walltime=user_opts['walltime'],
                nodes_per_block=user_opts['nodes_per_block'],
                hostfile_cmd=hostfile_cmd
            )

            script = cmd_dir / job_name
            with open(script, 'w') as f:
                f.write(template)

            subprocess.run([
                'qsub', '-A', user_opts['allocation'],
                '-q', user_opts['queue'], str(script)
            ])
            return
        else:
            # subtract 1 from node count if node count > 1 so that the driver program gets its own node
            if user_opts['nodes_per_block'] > 1:
                user_opts['nodes_per_block'] -= 1


    parsl_config = create_parsl_config(user_opts, local=args.local)
    print(parsl_config)
    parsl.clear()

    with parsl.load(parsl_config) as dfk:
        apply_hacks(dfk)
        wfe = wfe_class(settings)
        wfe.execute(cycle)


# when submitting with local provider, we call qsub directly on this script
LOCAL_TEMPLATE=f'''#!/bin/bash

#PBS -S /bin/bash
#PBS -N {{job_name}}
#PBS -m n
#PBS -l walltime={{walltime}}
#PBS -l select={{nodes_per_block}}
#PBS -o {{stdout}}
#PBS -e {{stderr}}
#PBS -l filesystems=home:flare

OUTDIR={{cmd_dir}}
mkdir -p $OUTDIR
cd $OUTDIR

# exclude this node (where the driver program runs) from the available nodes
{{hostfile_cmd}}

JOBNAME={{job_name}}
cat << EOL > cmd_$JOBNAME.sh
export TMPDIR=/tmp/
module load frameworks
source ~/.venv/sbn/bin/activate
export PATH=/opt/cray/pals/1.4/bin:\${{{{PATH}}}}

python {{workflow}} {{settings}} --local -o {{out_dir}}
EOL
chmod u+x cmd_$JOBNAME.sh

# line buffer to get logs faster
./cmd_$JOBNAME.sh > {{cmd_dir}}/sbn_parsl.log

[[ "1" == "1" ]] && echo "All done"
'''

HOSTFILE_CMD_EXCLUDE_SELF = r'''sort -u $PBS_NODEFILE | grep -v $(hostname) > $OUTDIR/hostfile.noexec
export PBS_NODEFILE=$OUTDIR/hostfile.noexec
'''
