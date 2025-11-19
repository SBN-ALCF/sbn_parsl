#!/bin/bash

#
# Get system information including running lar jobs as JSON output
#
# How to run: ssh into a node, then run to get the instantaneous statistics.
# To create a log, use the jq command in a loop to add the previous log with
# the current output, e.g.,
# `rm -f ~/log.json; touch /tmp/log && watch -n 30 -x bash -c "cp ~/log.json /tmp/log; jq -s add <(./lar_mon.sh) /tmp/log > /tmp/log2 && mv /tmp/log2 ~/log.json"`

echo "{"

# echo "\"timestamp\": $(date +%s),"
echo \"$(date +%s)\": {

# get lar PIDs & fcl name being run
echo "\"lar\":{"
ps -ww -C lar -o rss,%cpu=,pid=,args= | grep -v "defunct" \
    | sed -r 's/(.*)lar\s+-c(.*\.fcl).*/\1 \2/g' | grep -e 'fcl$' \
    | awk '{printf("\"%d\":{\"fcl\": \"%s\", \"cpu\": %.1f, \"rss\": %.1f}\n", $3, $4, $2, $1)}' \
    | paste -sd ","
echo "},"

# cpu info 10 s interval
mpstat -P ALL -o JSON 10 1 | tr '\n' '\r' | sed 's/^.\(.*\)..$/\1/mg'

# memory
free -k | awk '{if($1=="Mem:"){printf(",\"mem\":{\"total\":%d,\"used\":%d,\"free\":%d}\n", $2, $3, $4)}}'

if [ 1 -eq 0 ]; then
# disk
echo ",\"disk\":"
jq '.sysstat.hosts[0].statistics[0].disk' <(iostat -o JSON)

# gpu info
echo ",\"gpu\": {"
nvidia-smi --query-gpu=pci.bus_id,name,utilization.gpu,memory.used,memory.total --format=csv,noheader \
    | sed 's/, /,/g' | awk -F"," \
    '{
        printf("\"%s\":{\"name\": \"%s\", \"gpu\": %.1f, \"mem\":%d, \"total_mem\": %d}\n", $1, $2, $3, $4, $5)
    }' | paste -sd,
echo "}"
fi

echo "}}"
