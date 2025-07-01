# Monster MQ

MonsterMQ is a MQTT broker built on Vert.X and Hazelcast with data persistence through PostgreSQL or CrateDB or MongoDB. It also comes with a MCP (Model Context Protocol) Server for AI integration.
![Logo](Logo.png)

## Features

MonsterMQ can store unlimited amounts of retained messages. Also the message queue for message with QoS>0 and persistent offline sessions are unlimited because they are stored in PostgreSQL/CrateDB/MongoDB.

Clustering is supported through Hazelcast, which allows you to run multiple instances of the broker in a cluster. With that you can scale the broker horizontally and have a high availability setup, and also you can build hierarchical clusters with multiple levels of brokers, like having a top server and multiple server for each product line or factory. If clients are connected to the top server, they can subscribe to topics of the lower servers and receive messages from them. This allows you to build a distributed system with multiple levels of brokers.

## Limitations

Currently the broker does not support MQTT5 features like shared subscriptions, message expiry, etc. It is planned to add these features in the future.

Because MonsterMQ uses databases like PostgreSQL or CrateDB for storage, its performance is limited by the database and the scalability options provided by those databases.

Currenlty there are no ACL (Access Control Lists) implemented, so all clients can subscribe and publish to all topics. This is planned to be added in the future.

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
      - 5432:5432
    volumes:
      - ./db:/var/lib/postgresql/data
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
      - 3000:3000
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

  - Name: MCP # Must be named MCP to be used by the MCP Server
    Enabled: true
    TopicFilter: [ "Original/#" ]
    RetainedOnly: false
    LastValType: POSTGRES # Lst value is needed to read current values for MCP
    ArchiveType: POSTGRES # Optionally store the messages in the archive to be able to read them by MCP

Kafka:
  Servers: linux0:9092
  Bus: # Use Kafka as the message bus
    Enabled: false
    Topic: monster

Postgres:
  Url: jdbc:postgresql://timescale:5432/monster
  User: system
  Pass: manager

CrateDB:
  Url: jdbc:postgresql://cratedb:5432/monster
  User: crate
  Pass: ""

MongoDB:
  Url: mongodb://system:manager@mongodb:27017
  Database: monster  

```

# MCP Server

The MCP Server is a Model Context Protocol server that allows you to integrate AI models with MonsterMQ. The MCP Server needs a ArchiveGroup named "MCP", which is used to store the message which can be used by the MCP Server. If the archive group also has history enabled, the MCP Server can also read the history of messages. Currently only PostgreSQL is supported as a storage for the MCP Server.

The MCP Server is running on port 3000. 





