#!/bin/bash

#PBS -S /bin/bash
#PBS -N parsl_local_test
#PBS -m n
#PBS -l walltime=1:00:00
#PBS -l select=2
#PBS -o /lus/flare/projects/neutrinoGPU/twester/local_test/runinfo/cmd/submit_scripts/parsl_local_test.stdout
#PBS -e /lus/flare/projects/neutrinoGPU/twester/local_test/runinfo/cmd/submit_scripts/parsl_local_test.stderr
#PBS -l filesystems=home:flare

OUTDIR=/lus/flare/projects/neutrinoGPU/twester/local_test/runinfo/cmd
mkdir -p ${OUTDIR}
cd ${OUTDIR}

# exclude this node (where the driver program runs) from the available nodes
sort -u $PBS_NODEFILE | grep -v $(hostname) > ${OUTDIR}/hostfile.noexec

JOBNAME="local"
cat << EOL > cmd_$JOBNAME.sh
export TMPDIR=/tmp/
module load frameworks
source ~/.venv/sbn/bin/activate
export PATH=/opt/cray/pals/1.4/bin:\${PATH}
python ~/sbn_parsl/workflows/icarus_mc.py ~/sbn_parsl/settings/icarus/settings_mc_icarus_container_stage1_caf.json --local -o /lus/flare/projects/neutrinoGPU/twester/local_test --hostfile=${OUTDIR}/hostfile.noexec
EOL
chmod u+x cmd_$JOBNAME.sh

./cmd_$JOBNAME.sh > /lus/flare/projects/neutrinoGPU/twester/local_test/runinfo/cmd/sbn_parsl.log

[[ "1" == "1" ]] && echo "All done"
