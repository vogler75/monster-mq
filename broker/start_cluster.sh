mkdir -p log
./run.sh -cluster -config configs/config-cluster-node-1.yaml -log INFO > log/node1.out 2>&1 &
./run.sh -cluster -config configs/config-cluster-node-2.yaml -log INFO > log/node2.out 2>&1 &
./run.sh -cluster -config configs/config-cluster-node-3.yaml -log INFO > log/node3.out 2>&1 &

