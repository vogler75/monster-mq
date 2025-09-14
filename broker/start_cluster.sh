./run.sh -cluster -config config1.yaml -log FINE > node1.out 2>&1 &
./run.sh -cluster -config config2.yaml -log FINE > node2.out 2>&1 &
./run.sh -cluster -config config3.yaml -log FINE > node3.out 2>&1 &
./run.sh -cluster -config config4.yaml -log FINE > node4.out 2>&1 &

