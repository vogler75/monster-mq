# Monster MQ

MonsterMQ is a MQTT broker built on Vert.X and Hazelcast with data persistence through PostgreSQL. 

## Docker Image

> docker run -v ./log:/app/log -v ./config.yaml:/app/config.yaml rocworks/monstermq [-cluster] [-log INFO|FINE|FINER|FINEST|ALL]

## Build Locally 

> cd broker  
> mvn package  
> java -classpath target/classes:target/dependencies/* at.rocworks.MainKt $@  

## Configuration 

Use the YAML schema "broker/yaml-json-schema.json" in your editor.

```
TCP: 1883 # TCP Port (0=Disabled)
WS: 1884 # Websockets Port (0=Disabled)
SSL: false
MaxMessageSizeKb: 512

SessionStoreType: POSTGRES  # POSTGRES, CRATEDB
RetainedStoreType: POSTGRES  # MEMORY, HAZELCAST, POSTGRES, CRATEDB

SparkplugMetricExpansion:
  Enabled: true # Expand SparkplugB messages from spBv1.0 to spBv1.0e topic

ArchiveGroups:
  - Name: "group1"
    Enabled: true
    TopicFilter: [ "test1/#" ] # list of topic filters 
    RetainedOnly: false # Store only retained messages or all
    LastValType: POSTGRES # NONE, MEMORY, HAZELCAST, POSTGRES, CRATEDB
    ArchiveType: POSTGRES # NONE, POSTGRES, CRATEDB, KAFKA

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






