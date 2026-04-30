# neutrinoGPU scaling workflow

## Set up

It is recommended that you first set your project allocation in the input json file as described [below](#queue-settings).

```
git clone -b test/io_improvement https://github.com/SBN-ALCF/sbn_parsl/
module load frameworks

# make a virtual env on top of frameworks
python -m venv ~/.venv/sbn
source ~/.venv/sbn/bin/activate

# install sbn_parsl to virtual environment
cd sbn_parsl && pip install -e .
cd workflows/aurora_test

# Workflow driver program will submit jobs to pbs from the login node
# Optional: run in a screen session to keep driver program running
# First argument to script is a .json settings file. Use
# -o flag to write outputs to a directory you own on flare
screen -S sbnd
mkdir outputs
python sbnd_mc.py sbnd_mc_particlebomb_512nodes.json -o $PWD/outputs

# If using screen you can detach and reattach to session:
ctl-a d # detach
screen -r sbnd # reattach
```
### DAOS setup

If also using DAOS to write outputs, create a DAOS container within your project pool.

To setup a DAOS container `sbnd` in DAOS pool `gpu_hack`:
```
module use /soft/modulefiles
module load daos

daos container create --type=POSIX gpu_hack sbnd
daos cont list gpu_hack
daos container get-prop gpu_hack sbnd
```
See ALCF docs for more information:
https://docs.alcf.anl.gov/aurora/data-management/daos/daos-overview/?h=daos#pool

To mount the container on any node (uan or compute):
```
module use /soft/modulefiles
module load daos

# When working on uan node, include ${USER} in path, on compute nodes, not necessary
mkdir -p /tmp/${USER}/gpu_hack/sbnd
start-dfuse.sh -m /tmp/${USER}/gpu_hack/sbnd --pool gpu_hack --cont sbnd # To mount
mount | grep dfuse # to check mount
ls /tmp/${USER}/gpu_hack/sbnd # to see contents
```

To launch workflow and write outputs to DAOS, use this command:
```
python sbnd_mc.py sbnd_mc_particlebomb_512nodes.json --daos -o /tmp/gpu_hack/sbnd/$PWD/outputs -r $PWD/outputs
```
For this example, the `runinfo` directory with parsl logging will be written to `$PWD/outputs` on lustre and workers running `lar` will write to `/tmp/gpu_hack/sbnd/$PWD/outputs` on compute nodes.

Note that with how the DAOS container is mounted on compute nodes at scale with `launch-dfuse.sh`, the container will be at the path `/tmp/gpu_hack/sbnd` on the compute nodes.

## Outputs

Outputs are contained in the path passed with `-o` to the `sbnd_mc.py` driver script.  The path `<output_dir>/runinfo` contains logs pertaining to job submission and task execution from parsl.  Task logs from the `lar` application are contained in other subdirectories in the output directory.

### PBS scripts

PBS scripts are contained in a path that follows this pattern:
```
<output_dir>/runinfo/<run_num>/submit_scripts
```
The job stdout and stderr are also contained here.  Ping errors will appear in stderr files here.

### Nodes

The nodes used in jobs submitted by parsl are in files contained in this directory:
```
<output_dir>/runinfo/cmd
```
They have the form `parsl.htex.block-<num>.<identifier>.nodes`.

## Checking status

Monitoring processes on the compute nodes has been the best way idenftifed so far to see if a workflow is running into trouble.  Things are healthy when here are multiple instances of the application `lar` running on a node.  If no or few instances of `lar` are seen on nodes, this usually preceeds a ping error.  Ping errors will appear in the stderr of the pbs submission scripts.

## Modifying job settings

### Queue settings

Check the `queue` block in `sbnd_mc_particlebomb_512nodes.json`. These settings will
modify the Parsl config.

```
    "queue": {
        "queue": "prod",
        "walltime": "04:00:00",
        "nodes_per_block": 512,
        "cores_per_worker": 1,
        "min_blocks": 0,
        "max_blocks": 1,
        "init_blocks": 1,
        "cpus_per_node": 102,
        "strategy": "none",
        "allocation": "neutrinoGPU::ALCC_benchmark",
	"retries": 0,
	"init_cmd": "cp /lus/flare/projects/neutrinoGPU/scisoft/sbnd_data-01.41.00-noarch.tar.bz2 /tmp && tar -xf /tmp/sbnd_data-01.41.00-noarch.tar.bz2 -C /tmp"
    },
```

### Task settings

The `nsubruns` setting within the `run` block controls how many tasks are
submitted. To fully saturate the nodes with work, this should be set to at
least (number of nodes) x (number of workers per node). E.g.,

```
    "run": {
	...		
        "nsubruns": 52224,
	...
    },
```

Should match

```
    "queue": {
	...
        "nodes_per_block": 512,
        "cores_per_worker": 1,
        "cpus_per_node": 102,
	...
     }
```


### Misc. settings

The `max_futures` setting limits how many tasks Parsl will submit before
waiting for tasks to finish. This should be at least greater than the number of
subruns to ensure there are enough tasks for the workers, but very large
numbers (>100k) will cause the driver program to use several GBs of memory,
which may result in the program getting killed on the login nodes.

### Smaller test input file

The file `sbnd_mc_particlebomb_512nodes.json` contains settings for a 4 node test in debug-scaling.  It will not trigger problem behavior seen in larger tests.

## Modifying task code

### Modifying the Parsl bash app

The workflow progam submits tasks from a bash template that is populated based
on the settings file. To edit what's run, modify `CMD_TEMPLATE_CONTAINER`
within the `sbn_parsl/templates.py` file.

Note: Must be careful of braces. The template is an f-string with multiple
layers of subsitutions. Expressions in single braces ({}) are evaluated with
file-local variables, expressions within double braces ({{}}) are evaluated at
task submission time from the settings file. Expressions with four braces
({{{{}}}}) are evaluated by the bash interpreter when the task executes.

Note2: Must be careful of escapes. Any shell variables or expressions within
the `singularity run` heredoc should have the dollar sign escaped: "\$". This
is not necessary outside of the heredoc.

As an example, saving the environment to a variable could look like this:

```
CMD_TEMPLATE_CONTAINER = f'''
{JOB_PRE}
cd {{workdir}}
echo "Current directory: "
pwd
echo "Current files: "
ls
echo "Move fcl."
node_env=$(env) # <-----
# ... do something with $node_env

echo "Load singularity"
{CONTAINER_INIT}
set -e
singularity run $MNT_ARG {{container}} <<EOF
	container_env=\$(env) # <-----
	# ... do something with \$container_env
	...
```

### Modifying the LArSoft command

The template ultimately runs a larsoft command (`{{cmd}}` line in
`templates.py`).  This command is created within the `sbn_parsl/components.py`
`larsoft_runfunc` function. The function creates the command based on inputs
from previous tasks and the LArSoft settings. Different workflows create the
command with variations, so the `larsoft_runfunc` function accepts replacement
functions functions as arguments for the different command parts.

The function used by the driver program is a Python partial object with some of
the command code swapped for this particular workflow. See `mc_runfunc_sbnd`
within `components.py` for the exact functions used.

## Library files needed to run LArSoft

LArSoft is installed in our project directory at
`/lus/flare/projects/neutrinoGPU/scisoft/larsoft`. For using a different
installation, modify the `larsoft_top` line in the settings file.
