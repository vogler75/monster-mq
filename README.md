# Monster MQ

MonsterMQ is a MQTT broker built on Vert.X and Hazelcast with data persistence through PostgreSQL. 
![Logo](Logo.png)

## Docker Image

> docker run -v ./log:/app/log -v ./config.yaml:/app/config.yaml rocworks/monstermq [-cluster] [-log INFO|FINE|FINER|FINEST|ALL]

## Docker Compose
```
services:
  timescale:
    image: timescale/timescaledb:latest-pg16
    container_name: timescale
    restart: unless-stopped
    ports:
      - "5432:5432"
    volumes:
      - /data/timescale:/var/lib/postgresql/data
    environment:
      POSTGRES_USER: system
      POSTGRES_PASSWORD: manager
  monstermq:
    image: rocworks/monstermq:latest
    container_name: monstermq
    restart: unless-stopped
    ports:
      - 1883:1883
      - 8883:8883
      - 9000:9000
      - 9001:9001
    volumes:
      - ./config.yaml:/app/config.yaml
      # optionally log to files with log rotation
      #- ./log:/app/log      
      #- ./logging-file.properties:/app/logging.properties # see broker/src/main/resources/*
    command: ["-log INFO"]
```

## Build Locally 

> cd broker  
> mvn package  
> java -classpath target/classes:target/dependencies/* at.rocworks.MainKt $@  

## Configuration 

Use the YAML schema "broker/yaml-json-schema.json" in your editor.

```
TCP: 1883 # TCP Port (0=Disabled)
TCPS: 1884 # TCP Port with TLS (0=Disabled)

WS: 9001 # Websockets Port (0=Disabled)
WSS: 9002 # Websockets Port with TLS (0=Disabled)

MaxMessageSizeKb: 512
QueuedMessagesEnabled: true # if false the QoS>0 messages will not be queued for persistant sessions

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

Kafka:
  Servers: linux0:9092
  Bus: # Use Kafka as the message bus
    Enabled: false
    Topic: monster

Postgres:
  Url: jdbc:postgresql://timescale:5432/postgres
  User: system
  Pass: manager

CrateDB:
  Url: jdbc:postgresql://cratedb:5432/monster
  User: crate
  Pass: ""

```






