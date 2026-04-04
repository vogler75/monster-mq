mkdir -p log
./run.sh -nokill -cluster -config configs/config-cluster-node-1.yaml > log/node1.out 2>&1 &
./run.sh -nokill -cluster -config configs/config-cluster-node-2.yaml > log/node2.out 2>&1 &
./run.sh -nokill -cluster -config configs/config-cluster-node-3.yaml > log/node3.out 2>&1 &
