# Monster MQ

MonsterMQ is a MQTT broker built on Vert.X and Hazelcast with data persistence through PostgreSQL. 

> docker run -v ./config.yaml:/app/config.yaml rocworks/monstermq [-cluster]

```
Port: 1883
SSL: false
WS: false
TCP: true

SessionStoreType: POSTGRES # POSTGRES, CRATEDB
RetainedStoreType: POSTGRES # POSTGRES, CRATEDB

SparkplugMetricExpansion:
  Enabled: true

ArchiveGroups:
  - Name: "group1"
    Enabled: true
    TopicFilter: [ "test1/#" ]
    RetainedOnly: false
    LastValType: POSTGRES # NONE, MEMORY, HAZELCAST, POSTGRES, CRATEDB
    ArchiveType: POSTGRES # NONE, POSTGRES, CRATEDB
  - Name: "group2"
    Enabled: true
    TopicFilter: [ "test2/#" ]
    RetainedOnly: true
    LastValType: POSTGRES # NONE, MEMORY, HAZELCAST, POSTGRES, CRATEDB
    ArchiveType: POSTGRES # NONE, POSTGRES, CRATEDB

Postgres:
  Url: jdbc:postgresql://192.168.1.30:5432/postgres
  User: system
  Pass: manager

CrateDB:
  Url: jdbc:postgresql://192.168.1.31:5432/doc
  User: crate
  Pass: ""

```






