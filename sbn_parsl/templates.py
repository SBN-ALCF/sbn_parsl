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
    export MNT_ARG="-B /lus/flare/projects/neutrinoGPU/larsoft_hpc/envs -B /lus/flare/projects/neutrinoGPU/larsoft_hpc/tools -B /lus/flare/projects/neutrinoGPU/simulation_inputs_striped -B /lus/flare/projects/neutrinoGPU/icarus -B /lus/flare/projects/neutrinoGPU/twester"
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

REPLACE_PKG_WITH_TMP = r'''
replace_pkg_env_vars() {{
  # Accepts one or more lower-case package names as arguments
  # e.g., replace_pkg_env_vars sbnd_data sbndcode
  for pkg in "\$@"; do
    upper_pkg=\$(echo "\$pkg" | tr '[:lower:]' '[:upper:]')
    old_pkg_var="\${{upper_pkg}}_DIR"
    new_pkg_var="/tmp/\$pkg/\${{upper_pkg}}_VERSION"

    # Get the current value of PKG_DIR and PKG_VERSION
    old_pkg_val="\${{!old_pkg_var}}"
    pkg_version_var="\${{upper_pkg}}_VERSION"
    pkg_version="\${{!pkg_version_var}}"
    new_pkg_val="/tmp/\$pkg/\$pkg_version"

    # Apply replacement to all environment variables
    while IFS='=' read -r name value; do
      # Replace all occurrences of old_pkg_val with new_pkg_val
      new_value="\${{value//\$old_pkg_val/\$new_pkg_val}}"
      [ "\$new_value" != "\$value" ] && export "\$name=\$new_value"
    done < <(env)
  done
}}
'''


# this template additionally loads sbndata and expects "input" in the form of "-s file1 -s file2 ..."
SPINE_TEMPLATE = rf'''
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
CMD_TEMPLATE_SPACK = rf'''
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


CMD_TEMPLATE_CONTAINER = rf'''
{JOB_PRE}
cd {{workdir}}
echo "Current directory: "
pwd
echo "Current files: "
ls
echo "Move fcl."

echo "CONTAINTER $(date +%s) $(hostname)"
echo "Load singularity"
{CONTAINER_INIT}
set -e

# spread out container starts
# sometimes containe can fail to mount with squashfuse_ll
sleep $(( $RANDOM % 30 ))
MAX_RETRIES=3
attempt=0

while [ $attempt -lt $MAX_RETRIES ]; do
    singularity run {{overlay_str}} $MNT_ARG {{container}} <<EOF
        echo "ENV \$(date +%s) \$(hostname)"
        echo "Running in: "
        pwd
        echo "Sourcing products area"
        {PROXY}
        export EXPERIMENT={{experiment}}
        if [ "{{env_file}}x" != "x" ]; then
            echo "Sourcing environment from {{env_file}}"
            source {{env_file}}
        else
            echo "Sourcing environment from {{larsoft_top}}"
            source {{larsoft_top}}/setup && setup {{software}} {{version}} -q {{qual}};
        fi
        export PATH=/lus/flare/projects/neutrinoGPU/scisoft/larsoft/gcc/v12_1_0/Linux64bit+3.10-2.17/libexec/gcc/x86_64-pc-linux-gnu/12.1.0:\$PATH

        {REPLACE_PKG_WITH_TMP}
        replace_pkg_env_vars sbndcode sbnd_data icaruscode icarus_data

        echo "FHICL \$(date +%s) \$(hostname)"
        # get the fcls
        FCL_LIST=\$(sed -n -e 's/.*-c\ \(.*fcl\).*/\\1/p' <( echo "{{cmd}}" ))
        echo \${{{{FCL_LIST[@]}}}}
        {FIND_FCL_CONTAINER}
        for fcl in \${{{{FCL_LIST[@]}}}}; do
            if [ -f ./\$fcl ]; then
                echo "removing local fcl \$fcl"
                rm ./\$fcl
            fi
            fhicl_from_env=\$(find_fcl \$fcl)
            echo "fhicl_from_env=\$fhicl_from_env"
            if [ -f \$fhicl_from_env ]; then
                cp \$fhicl_from_env {{workdir}}/
            else
                echo "Could not find fcl \$fcl! Expect subsequent commands to fail."
            fi
        done


        echo "HOOK \$(date +%s) \$(hostname)"
        {{pre_job_hook}}

        set -e
        echo "About to run larsoft"
        echo "LAR \$(date +%s) \$(hostname)"
        {{cmd}}
        set +e
EOF
    exit_code=$?

    if [ $exit_code -eq 255 ]; then
        attempt=$((attempt + 1))

        sleep_time=$(( (2 ** attempt) * 5 + (RANDOM % 10) ))

        echo "Caught exit code 255. Retry $attempt/$MAX_RETRIES in $sleep_time s..."
        sleep $sleep_time
    else
        break
    fi
done

echo "CLEAN $(date +%s) $(hostname)"
echo "cp *.root $(dirname {{output}}) || true"
cp *.root $(dirname {{output}}) && rm -f *.root || true

echo "p *.json $(dirname {{output}}) || true"
cp *.json $(dirname {{output}}) && rm -f *.json || true

{{post_job_hook}}
{JOB_POST}
'''
