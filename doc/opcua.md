# OPC UA Integration

MonsterMQ ships two complementary OPC UA features:

- [OPC UA Client](opcua-client.md) – subscribes to external OPC UA servers and republishes values on MQTT topics. The guide covers GraphQL management, connection options, addressing, payload format, and certificate handling.
- [OPC UA Server](opcua-server.md) – exposes MQTT topics as OPC UA variables. The guide explains runtime architecture, GraphQL operations, address mappings, data conversion, and server-side certificates.

Refer to the dedicated documents to configure or operate either role. The legacy combined page was removed to keep the client and server stories maintainable and in sync with the implementation under `broker/src/main/kotlin/devices/opcua` and `broker/src/main/kotlin/devices/opcuaserver`.
