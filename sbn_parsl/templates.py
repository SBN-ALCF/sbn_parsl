#!/usr/bin/env python3

JOB_PRE = r'''
echo "START $(date +%s) $(hostname)" 
echo "Job starting!"
pwd
date
hostname
'''


JOB_POST = r'''
echo "Job finishing!"
date
echo "\nJob finished in: $(pwd)"
echo "Available files:"
ls
hostname
echo "END $(date +%s) $(hostname)" 
'''

# pick the CUDA device with the lowest memory utilization by assigning it to
# CUDA_VISIBLE_DEVICES environment variable
# note: ties broken randomly
NVIDIA_BEST_CUDA = r'''
nvidia-smi
BESTCUDA=$(python3 -c 'import gpustat;import numpy as np;stats=gpustat.GPUStatCollection.new_query();memory=np.array([gpu.memory_used for gpu in stats.gpus]);print(np.random.choice(np.flatnonzero(memory == memory.min())))')
export CUDA_VISIBLE_DEVICES=$BESTCUDA
echo GPU Selected
echo $CUDA_VISIBLE_DEVICES
'''

CONTAINER_INIT = r'''
host=$(hostname -d | sed 's/.*\.\(.*\)\.alcf\.anl\.gov/\1/g')
if [ $host == "polaris" ]; then
    export MNT_ARG="-B /lus/grand -B /lus/eagle"
    module use /soft/spack/gcc/0.6.1/install/modulefiles/Core
fi
module load apptainer
if [ $host == "aurora" ]; then
    export MNT_ARG="-B /lus/flare"
    module load fuse-overlayfs
fi
'''

PROXY=r'''
export http_proxy="http://proxy.alcf.anl.gov:3128"
export https_proxy="http://proxy.alcf.anl.gov:3128"
export ftp_proxy="http://proxy.alcf.anl.gov:3128"
'''


# define a function so the job can find the full path to fcl by name
FIND_FCL = r'''
echo "Defining fcl lookup function"
echo "FHICL_FILE_PATH=$FHICL_FILE_PATH"
function find_fcl() {{
    if [ "$1" != "${{1#/}}" ]; then
        # absolute path
        echo $1 
        return 0
    fi

    # relative path or filename -- look up in FHICL_FILE_PATH
    echo $(IFS=:; find $FHICL_FILE_PATH -name $(basename "${{1}}") | head -n 1)
}}
'''

FIND_FCL_CONTAINER = r'''
echo "Defining fcl lookup function"
echo "FHICL_FILE_PATH=\$FHICL_FILE_PATH"
function find_fcl() {{
    if [ "\$1" != "\${{1#/}}" ]; then
        # absolute path
        echo \$1 
        return 0
    fi

    # relative path or filename -- look up in FHICL_FILE_PATH
    echo \$(IFS=:; find \$FHICL_FILE_PATH -name \$(basename "\${{1}}") | head -n 1)
}}
'''


# this template additionally loads sbndata and expects "input" in the form of "-s file1 -s file2 ..."
SPINE_TEMPLATE = f'''
{JOB_PRE}
cd {{workdir}}
echo "Current directory: "
pwd
echo "Current files: "
ls

{{pre_job_hook}}
echo "Load singularity"
module use /soft/spack/gcc/0.6.1/install/modulefiles/Core
module load apptainer
set -e

# these lines replace hard-coded path in SPINE cfg files from github
# put the changes in a temporary copy
TMP_CFG={{workdir}}/tmp.cfg
cp {{config}} $TMP_CFG
sed -i "s|\(.*num_workers:\).*|\\1 {{cores_per_worker}}|g" $TMP_CFG
sed -i "s|\(.*weight_path:\).*|\\1 {{weights}}|g" $TMP_CFG
sed -i "s|\(.*cfg:\).*\(flashmatch.*\.cfg\).*|\\1 $(dirname {{config}})/\\2|g" $TMP_CFG

# use GPUs from environment, so remove this 
sed -i "s|\(.*gpus:\).*||g" $TMP_CFG
echo "Config: $TMP_CFG"

{NVIDIA_BEST_CUDA}

# litify: Run on all output files
LITIFY_LIST=$(dirname {{input}})/litify_list.txt
sed "s|.*/\(.*\)\.root$|\\1_spine\.h5|g" {{input}} > $LITIFY_LIST

singularity run -B /lus/eagle/ -B /lus/grand/ --nv {{container}} <<EOL
    echo "Running in: "
    pwd
    export CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES
    echo "CUDA_VISIBLE_DEVICES=\$CUDA_VISIBLE_DEVICES"
    if [ "{{opt0finder}}x" != "x" ]; then
        source {{opt0finder}}/configure.sh
        echo "using custom OpT0Finder"
        echo \$FMATCH_BASEDIR
        echo \$FMATCH_LIBDIR
    fi
    python {{exe}} -c $TMP_CFG -S {{input}} &&
    {{{{
        if [ "{{litify}}x" != "x" ]; then
            echo "running litify"
            python {{exe}} -c {{litify}} -S $LITIFY_LIST
        fi
    }}}}
EOL
echo "moving files"
if [ "{{litify}}x" != "x" ]; then
    echo "Using lite files"
    rename "_spine_spine.h5" "_spine.h5" *_spine_spine.h5
fi
mv *.h5 $(dirname {{output}}) || true
{{post_job_hook}}
{JOB_POST}
'''


# execute a custom command
CMD_TEMPLATE_SPACK = f'''
{JOB_PRE}
cd {{workdir}}
echo "Current directory: "
pwd
echo "Current files: "
ls
echo "Move fcl."

{FIND_FCL}
fhicl_from_env=$(find_fcl {{fhicl}})
if [ -f $fhicl_from_env ]; then
    cp $fhicl_from_env {{workdir}}/
else
    echo "Could not find fcl! Expect subsequent commands to fail."
fi
export LOCAL_FCL=$(basename {{fhicl}})

echo "fhicl_from_env=$fhicl_from_env"
echo "LOCAL_FCL=$LOCAL_FCL"

{{pre_job_hook}}
set -e
echo "Running in: "
pwd
echo "Sourcing products area"
#setup SBNDCODE:
echo "CUDA_VISIBLE_DEVICES=$CUDA_VISIBLE_DEVICES"
export EXPERIMENT={{experiment}}
echo "Products setup!"
# get the fcls
set -e
# Add an optional input file:
export lar_cmd="{{cmd}}"
echo $lar_cmd
echo "About to run larsoft"
eval $lar_cmd
set +e

echo "moving files"
echo "mv *.json $(dirname {{output}}) || true"
mv *.json $(dirname {{output}}) || true

echo "mv  *hist*root "$(dirname {{output}})/hists_$(basename {{output}})" || true"
mv  *hist*root "$(dirname {{output}})/hists_$(basename {{output}})" || true

echo "mv *.root $(dirname {{output}}) || true"
mv *.root $(dirname {{output}}) || true

{{post_job_hook}}
{JOB_POST}
'''


CMD_TEMPLATE_CONTAINER = f'''
{JOB_PRE}
cd {{workdir}}
echo "Current directory: "
pwd
echo "Current files: "
ls
echo "Move fcl."

echo "Load singularity"
{CONTAINER_INIT}
set -e
singularity run $MNT_ARG {{container}} <<EOF
    echo "Running in: "
    pwd
    echo "Sourcing products area"
    {PROXY}
    export EXPERIMENT={{experiment}}
    source {{larsoft_top}}/setup
    setup {{software}} {{version}} -q {{qual}}
    export PATH=/lus/flare/projects/neutrinoGPU/scisoft/larsoft/gcc/v12_1_0/Linux64bit+3.10-2.17/libexec/gcc/x86_64-pc-linux-gnu/12.1.0:\$PATH
    echo "Products setup!"
    # get the fcls
    {FIND_FCL_CONTAINER}
    fhicl_from_env=\$(find_fcl {{fhicl}})
    if [ -f \$fhicl_from_env ]; then
        cp \$fhicl_from_env {{workdir}}/
    else
        echo "Could not find fcl! Expect subsequent commands to fail."
    fi
    export LOCAL_FCL=\$(basename {{fhicl}})
    {{pre_job_hook}}

    echo "fhicl_from_env=\$fhicl_from_env"
    echo "LOCAL_FCL=\$LOCAL_FCL"

    set -e
    # Add an optional input file:
    export lar_cmd="{{cmd}}"
    echo \$lar_cmd
    echo "About to run larsoft"
    eval \$lar_cmd
    set +e
EOF

echo "mv *.root $(dirname {{output}}) || true"
mv *.root $(dirname {{output}}) || true

echo "mv *.json $(dirname {{output}}) || true"
mv *.json $(dirname {{output}}) || true

{{post_job_hook}}
{JOB_POST}
'''
