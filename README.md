# Monster MQ

TODO General  
- Messages are sent multiple times if the client has multiple matching wildcard subscriptions. Same problem for retained messages. Subscribe should only subscribe if there is not yet another subscription which covers the new subscription.


TODO Clustering  
- Store Last-Will message in a ClusterWideMap and send it for all clients of a distributer node if the node dies.  

- Replica for distributer, if a distributor node dies, a replica should take over. Subscriptions of non-clean-session clients must continue to collect data. 


TODO Security  
- Authorization  
- Topic ACL  






