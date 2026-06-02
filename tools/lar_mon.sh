#!/bin/bash
# lar_mon.sh: Monitor CPU cores, memory, and GPUs, printing/appending JSON lines.

INTERVAL=0
OUTPUT=""

# Simple command-line argument parser
while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--interval)
            INTERVAL="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Query GPU stats (NVIDIA Polaris) - easily extensible to Intel GPU query tools in the future
get_gpu_stats() {
    if command -v nvidia-smi &> /dev/null; then
        nvidia-smi --query-gpu=pci.bus_id,name,utilization.gpu,memory.used,memory.total --format=csv,noheader 2>/dev/null \
            | sed 's/, /,/g' \
            | awk -F"," 'BEGIN {printf "{"} {if (NR>1) printf ","; printf "\"%s\":{\"name\":\"%s\",\"gpu\":%.1f,\"mem\":%d,\"total_mem\":%d}", $1, $2, $3, $4, $5} END {printf "}"}'
    else
        echo "{}"
    fi
}

run_once() {
    # 1. Gather lar processes info
    local lar_json
    lar_json=$(ps -ww -C lar -o rss,%cpu=,pid=,args= 2>/dev/null | grep -v "defunct" \
        | sed -E 's/(.*)lar\s+-c(.*\.fcl).*/\1 \2/g' | grep -e 'fcl$' \
        | awk 'BEGIN {printf "{"} {if (NR>1) printf ","; printf "\"%d\":{\"fcl\":\"%s\",\"cpu\":%.1f,\"rss\":%.1f}", $3, $4, $2, $1} END {printf "}"}')
    [ -z "$lar_json" ] && lar_json="{}"

    # 2. Gather per-core CPU usage (condensed to a list of busy percentages)
    local cpu_json
    if command -v mpstat &> /dev/null; then
        cpu_json=$(mpstat -P ALL -o JSON 1 1 2>/dev/null | jq -c '[.sysstat.hosts[0].statistics[0]."cpu-load"[] | select(.cpu != "all") | 100.0 - .idle]')
    fi
    [ -z "$cpu_json" ] && cpu_json="[]"

    # 3. Gather memory info
    local mem_json
    mem_json=$(free -k 2>/dev/null | awk '{if($1=="Mem:"){printf("{\"total\":%d,\"used\":%d,\"free\":%d}", $2, $3, $4)}}')
    [ -z "$mem_json" ] && mem_json="{}"

    # 4. Gather GPU info
    local gpu_json
    gpu_json=$(get_gpu_stats)

    # 5. Output compact combined JSON line
    jq -c -n \
       --argjson ts "$(date +%s)" \
       --argjson lar "$lar_json" \
       --argjson cpu "$cpu_json" \
       --argjson mem "$mem_json" \
       --argjson gpu "$gpu_json" \
       '{"timestamp": $ts, "lar": $lar, "cpu": $cpu, "mem": $mem, "gpu": $gpu}'
}

# Execution logic
if [ "$INTERVAL" -gt 0 ]; then
    while true; do
        if [ -n "$OUTPUT" ]; then
            run_once >> "$OUTPUT" 2>/dev/null
        else
            run_once
        fi
        sleep "$INTERVAL"
    done
else
    if [ -n "$OUTPUT" ]; then
        run_once >> "$OUTPUT" 2>/dev/null
    else
        run_once
    fi
fi
