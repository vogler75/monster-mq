# Developer and AI Coding Documentation

This directory contains documentation for developers and AI coding agents working on the MonsterMQ codebase. For operator/user documentation, see [the documentation index](../doc/README.md) instead, including the [Redfish Gateway guide](../doc/redfish-gateway.md).

Keep this index as the curated entry point for active and completed developer plans. Update it when files are added, moved between `plans/`, `todo/` and `done/`, or removed.

File names use lowercase kebab-case (`like-this.md`).

## Coding Guides

- [development.md](development.md) — Environment setup, build commands, test execution, contribution guidelines
- [ix.instructions.md](ix.instructions.md) — Siemens iX design system guidelines for dashboard development
- [plugin-architecture-implementation.md](plugin-architecture-implementation.md) — Planned plugin architecture (v2.0.0, planning phase)
- [wincc-unified-openpipe-reference.md](wincc-unified-openpipe-reference.md) — WinCC Unified Open Pipe reference

## Implementation Plans (`plans/`)

**Primary AI coding reference:**
- [plans/plan-device-integration.md](plans/plan-device-integration.md) — Step-by-step guide for adding new device types (Backend Kotlin verticles, GraphQL schema/resolvers, Frontend dashboard pages)

**Feature-specific plans:**
- [plans/plan-amazon-kinesis-client-issue-110.md](plans/plan-amazon-kinesis-client-issue-110.md) — Amazon Kinesis client integration
- [plans/plan-edge-topic-schema-governance.md](plans/plan-edge-topic-schema-governance.md) — Implementing topic schema governance in the Go edge broker
- [plans/plan-knowledge-graph.md](plans/plan-knowledge-graph.md) — Knowledge Graph & semantic topic subscriptions with Apache Jena (#167)
- [plans/plan-peerlink-interest-routing.md](plans/plan-peerlink-interest-routing.md) — PeerLink interest routing: consumers announce their subscription filters, the source forwards only matching publishes (sparse batches, per-record consumer masks); wire-compatible with the edge broker
- [plans/plan-peerlink-redundancy.md](plans/plan-peerlink-redundancy.md) — PeerLink replication (implemented) plus redundancy roles for the main broker without native WinCC OA connectivity: ACTIVE/STANDBY decided by a witness lease in Postgres/MongoDB (or static config), split-mode handling when the link is down, hot/cold standby for bridges and archive groups; the edge counterpart with native WinCC OA is in the edge repo

**Architecture analysis:**
- [plans/plan-graalvm-capabilities-analysis.md](plans/plan-graalvm-capabilities-analysis.md) — GraalVM capabilities analysis

## Todo (`todo/`)

- [todo/code-analysis.md](todo/code-analysis.md) — Full-codebase review (architecture, security holes, duplication, engineering practices, test coverage)

## Completed Plans (`done/`)

- [done/plan-agent-improvements-review.md](done/plan-agent-improvements-review.md) — Code review findings and follow-ups for Agent Improvements (#189–#194), resolved
- [done/plan-data-catalog.md](done/plan-data-catalog.md) — Data Catalog with storage, GraphQL, MCP and i3X integration
- [done/plan-i3x-spec.md](done/plan-i3x-spec.md) — i3X v1 manufacturing API specification
- [done/plan-mqtt5-implementation-plan-issue-86.md](done/plan-mqtt5-implementation-plan-issue-86.md) — MQTT v5 implementation plan
- [done/plan-multi-db-archive-connections.md](done/plan-multi-db-archive-connections.md) — Multi-database archive connections
- [done/plan-hmi-mqtt-sync.md](done/plan-hmi-mqtt-sync.md) — Native MQTT HMI file synchronization (#195)
- [done/plan-mqtt-client-mtls-issue-111.md](done/plan-mqtt-client-mtls-issue-111.md) — Mutual TLS for the MQTT-Client bridge (#111); edge part tracked in monster-mq-edge#20
- [done/plan-opcua-client-write-support-issue-95.md](done/plan-opcua-client-write-support-issue-95.md) — OPC UA client write operations
- [done/plan-script-engine-python.md](done/plan-script-engine-python.md) — Python script engine (GraalPy)
- [done/plan-redis-protocol-server.md](done/plan-redis-protocol-server.md) — Redis-compatible protocol server for topic access through archive group last-value stores
- [done/plan-topic-based-decision-making.md](done/plan-topic-based-decision-making.md) — Topic-based decision making with OpenRouter / Jev (#196)
- [done/plan-ui-mqtt5-update-plan-issue-86.md](done/plan-ui-mqtt5-update-plan-issue-86.md) — Dashboard UI updates for MQTT v5 features
- [done/plan-wincc-ua-openpipe-implementation.md](done/plan-wincc-ua-openpipe-implementation.md) — WinCC Unified Open Pipe implementation
- [done/plan-zenoh-integration.md](done/plan-zenoh-integration.md) — Zenoh federation transport
