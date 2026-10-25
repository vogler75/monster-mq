#!/bin/bash
# Start a 3-node local cluster in the background (stop it with ./stop_cluster.sh).
cd "$(dirname "$0")"
PYTHON="${PYTHON:-python3}"
mkdir -p log
"$PYTHON" ../make.py run -nokill -cluster -config configs/config-cluster-node-1.yaml > log/node1.out 2>&1 &
"$PYTHON" ../make.py run -nokill -cluster -config configs/config-cluster-node-2.yaml > log/node2.out 2>&1 &
"$PYTHON" ../make.py run -nokill -cluster -config configs/config-cluster-node-3.yaml > log/node3.out 2>&1 &
