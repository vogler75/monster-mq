# Monster MQ

MonsterMQ is a MQTT broker built on Vert.X and Hazelcast with data persistence through PostgreSQL. 

> docker run -v ./config.yaml:/app/config.yaml rocworks/monstermq [-cluster] [-log INFO|FINE|FINER|FINEST|ALL]

```
Port: 1883
SSL: false
WS: false
TCP: true
MaxMessageSizeKb: 512

SessionStoreType: POSTGRES
RetainedStoreType: POSTGRES

SparkplugMetricExpansion:
  Enabled: true

ArchiveGroups:
  - Name: "group1"
    Enabled: true
    TopicFilter: [ "test1/#" ]
    RetainedOnly: false
    LastValType: POSTGRES
    ArchiveType: POSTGRES
  - Name: "group2"
    Enabled: true
    TopicFilter: [ "test2/#" ]
    RetainedOnly: false
    LastValType: POSTGRES
    ArchiveType: POSTGRES
  - Name: "video"
    Enabled: true
    TopicFilter: [ "video/#" ]
    RetainedOnly: false
    LastValType: NONE
    ArchiveType: KAFKA

Kafka:
  Servers: linux0:9092
  Bus: # Use Kafka as the message bus
    Enabled: false
    Topic: monster

Postgres:
  Url: jdbc:postgresql://192.168.1.30:5432/test
  User: system
  Pass: manager

CrateDB:
  Url: jdbc:postgresql://192.168.1.31:5432/test
  User: crate
  Pass: ""

```






