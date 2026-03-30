# Developer and AI Coding Documentation

This directory contains documentation for developers and AI coding agents working on the MonsterMQ codebase. For operator/user documentation, see `doc/` instead.

## Coding Guides

- [development.md](development.md) — Environment setup, build commands, test execution, contribution guidelines
- [iX.instructions.md](iX.instructions.md) — Siemens iX design system guidelines for dashboard development
- [plugin-architecture-implementation.md](plugin-architecture-implementation.md) — Planned plugin architecture (v2.0.0, planning phase)

## Implementation Plans (`plans/`)

**Primary AI coding reference:**
- [plans/DEVICE_INTEGRATION.md](plans/DEVICE_INTEGRATION.md) — Step-by-step guide for adding new device types (Backend Kotlin verticles, GraphQL schema/resolvers, Frontend dashboard pages)

**Feature-specific plans:**
- [plans/FEATURE_FLAGS.md](plans/FEATURE_FLAGS.md) — Config-based feature flags for extension isolation
- [plans/MQTT5_IMPLEMENTATION_PLAN_ISSUE_86.md](plans/MQTT5_IMPLEMENTATION_PLAN_ISSUE_86.md) — MQTT v5 implementation plan
- [plans/UI_MQTT5_UPDATE_PLAN_ISSUE_86.md](plans/UI_MQTT5_UPDATE_PLAN_ISSUE_86.md) — Dashboard UI updates for MQTT v5 features
- [plans/OPCUA_CLIENT_WRITE_SUPPORT_ISSUE_95.md](plans/OPCUA_CLIENT_WRITE_SUPPORT_ISSUE_95.md) — OPC UA client write operations
- [plans/MQTT_CLIENT_MUTUAL_TLS_AWS_IOT_ISSUE_111.md](plans/MQTT_CLIENT_MUTUAL_TLS_AWS_IOT_ISSUE_111.md) — AWS IoT mutual TLS client support
- [plans/TOPIC_SCHEMA_GOVERNANCE_ISSUE_112.md](plans/TOPIC_SCHEMA_GOVERNANCE_ISSUE_112.md) — Topic schema governance and validation
- [plans/AMAZON_KINESIS_CLIENT_ISSUE_110.md](plans/AMAZON_KINESIS_CLIENT_ISSUE_110.md) — Amazon Kinesis client integration

**Performance optimization:**
- [plans/QUEUEDB_EMBEDDED_DATABASE.md](plans/QUEUEDB_EMBEDDED_DATABASE.md) — Embedded QueueDB with Hazelcast IMap clustering (sequence-based inbox model, reference counting, EntryProcessor)
- [plans/QUEUED_MESSAGES_OPTIMIZATION.md](plans/QUEUED_MESSAGES_OPTIMIZATION.md) — QoS > 0 queued messages PostgreSQL optimization (FOR UPDATE SKIP LOCKED, PGMQ-inspired redesign)

**Strategic architecture:**
- [plans/GRAALVM-CAPABILITIES-ANALYSIS.md](plans/GRAALVM-CAPABILITIES-ANALYSIS.md) — GraalVM capabilities analysis
- [plans/OPEN_INDUSTRIAL_CLAW.md](plans/OPEN_INDUSTRIAL_CLAW.md) — Open Industrial standards integration initiative
- [plans/OPEN_INDUSTRIAL_CLAW_PHASE1.md](plans/OPEN_INDUSTRIAL_CLAW_PHASE1.md) — Phase 1 implementation details
