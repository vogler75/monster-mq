# MonsterMQ documentation

These guides describe the Kotlin/Vert.x broker in this repository. The Go edge
broker exposes a compatible subset where implemented; check the running broker's
schema and enabled features before using a full-broker example on an edge node.

## Start and operate

| Guide | Purpose |
|---|---|
| [Installation](installation.md) | Installers, source builds, Docker, and first connection checks. |
| [Configuration](configuration.md) | Common YAML settings and links to detailed configuration. |
| [Databases](databases.md) | Store compatibility, backend connections, metrics, and queues. |
| [Archiving](archiving.md) | Last-value/history groups, retention, buffering, and management. |
| [Clustering](clustering.md) | Hazelcast discovery, node assignment, and shared-storage requirements. |
| [Windows service](windows-service.md) | WinSW service installation and operation. |
| [Monitoring](monitoring.md) | MQTT bulk-processing metrics and broker monitoring entry points. |
| [Grafana](grafana.md) | Prometheus-compatible queries, datasource setup, and limitations. |

## Security and APIs

| Guide | Purpose |
|---|---|
| [Security](security.md) | TLS, authentication configuration, and interface-specific enforcement limits. |
| [Users](users.md) | Account lifecycle, passwords, login, and Anonymous access. |
| [ACLs](acl.md) | Topic permission evaluation, wildcard matching, and rule management. |
| [MQTT 5](mqtt5.md) | Implemented protocol features, client examples, and unsupported behavior. |
| [GraphQL](graphql.md) | HTTP/WebSocket endpoints, data queries, publishing, and subscriptions. |
| [Redfish Gateway](redfish-gateway.md) | Map MQTT telemetry to Redfish sensor resources and configure the gateway. |
| [REST API](rest-api.md) | HTTP reads/writes, line-protocol ingestion, and SSE subscriptions. |
| [MCP](mcp.md) | MCP connection setup, tool inventory, metadata, and access scope. |
| [GraphQL system logs](graphql-system-logs.md) | Bounded log capture, queries, and live filtering. |
| [MQTT logging migration](mqtt-logging.md) | Compatibility pointer explaining removal of MQTT log topics. |

## Connectors and federation

| Guide | Purpose |
|---|---|
| [Kafka](kafka.md) | Kafka message bus, bidirectional bridges, and compatible server. |
| [NATS](nats.md) | Native NATS listener, client bridge, topic mapping, and TLS limits. |
| [Zenoh](zenoh.md) | Live broker federation and current-value query behavior. |
| [OPC UA overview](opcua.md) | Stable entry point directing readers to the appropriate OPC UA role. |
| [OPC UA client](opcua-client.md) | External server subscriptions, reads/writes, and certificates. |
| [OPC UA server](opcua-server.md) | Expose MQTT topics as OPC UA variables and accept writes. |
| [WinCC OA](winccoa.md) | Managed WinCC OA client bridge and datapoint mappings. |
| [WinCC Unified](winccua.md) | Managed tag/alarm subscriptions through Unified GraphQL. |
| [Neo4j](neo4j.md) | MQTT graph storage, actual labels/properties, and operational metrics. |
| [Snowflake](snowflake.md) | JDBC logger setup, authentication, and field-to-column mapping. |

Additional connectors are described by the GraphQL schemas and the dashboard.
This directory is not an exhaustive inventory of every broker feature.

## Automation and AI

| Guide | Purpose |
|---|---|
| [Workflows](workflows.md) | User walkthrough for flow classes, instances, and scripts. |
| [Flow engine](flow-engine.md) | Runtime/developer reference complementing the walkthrough. |
| [AI agents](ai-agents.md) | Agent configuration, providers, tools, context, and A2A messaging. |
| [AI topic analysis](ai-topic-analysis.md) | Topic-browser AI analysis and prompt configuration. |

## Contribution agreement

[CONTRIBUTOR_LICENSE_AGREEMENT.pdf](CONTRIBUTOR_LICENSE_AGREEMENT.pdf) is retained
because [CONTRIBUTING.md](../CONTRIBUTING.md) requires it. It is contribution
paperwork rather than broker configuration documentation.

## Maintenance

The 31 original Markdown guides, the Redfish Gateway guide moved from `dev/`,
and the contributor agreement were reviewed on
2026-09-05 against the repository implementation and schemas. The MQTT logging
and OPC UA overview files remain as useful navigation/migration entry points;
obsolete duplicated instructions were consolidated into their linked guides.
The contributor agreement's repository identity, references, and rendered pages
were checked; its terms were left unchanged.

When behavior changes, update its owning guide above and check:

- YAML against [`yaml-json-schema.json`](../broker/yaml-json-schema.json) **and** the code that reads it. Schema comments and legacy skill examples can lag implementation.
- GraphQL operations against the full set of `broker/src/main/resources/schema-*.graphqls`; validate each operation rather than only checking field names.
- Command examples against current scripts, paths, listener ports, and authentication behavior.
- Links and fenced JSON/YAML/XML/Python examples for broken targets or syntax.

Examples were checked statically during this review. External database/device
connections, Windows service installation, and live protocol behavior were not
integration-tested. For developer architecture and implementation plans, use
[the developer documentation index](../dev/INDEX.md).
