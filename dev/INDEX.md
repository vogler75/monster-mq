# Developer and AI Coding Documentation

This directory contains documentation for developers and AI coding agents working on the MonsterMQ codebase. For operator/user documentation, see `doc/` instead.

Keep this index as the curated entry point for active and completed developer plans. Update it when files are added, moved between `plans/` and `done/`, or removed.

## Coding Guides

- [development.md](development.md) — Environment setup, build commands, test execution, contribution guidelines
- [iX.instructions.md](iX.instructions.md) — Siemens iX design system guidelines for dashboard development
- [plugin-architecture-implementation.md](plugin-architecture-implementation.md) — Planned plugin architecture (v2.0.0, planning phase)
- [WinCC_Unified_OpenPipe_Reference.md](WinCC_Unified_OpenPipe_Reference.md) — WinCC Unified Open Pipe reference

## Implementation Plans (`plans/`)

**Primary AI coding reference:**
- [plans/DEVICE_INTEGRATION.md](plans/DEVICE_INTEGRATION.md) — Step-by-step guide for adding new device types (Backend Kotlin verticles, GraphQL schema/resolvers, Frontend dashboard pages)

**Feature-specific plans:**
- [plans/AMAZON_KINESIS_CLIENT_ISSUE_110.md](plans/AMAZON_KINESIS_CLIENT_ISSUE_110.md) — Amazon Kinesis client integration
- [plans/EDGE_TOPIC_SCHEMA_GOVERNANCE.md](plans/EDGE_TOPIC_SCHEMA_GOVERNANCE.md) — Implementing topic schema governance in the Go edge broker

**Architecture analysis:**
- [plans/GRAALVM-CAPABILITIES-ANALYSIS.md](plans/GRAALVM-CAPABILITIES-ANALYSIS.md) — GraalVM capabilities analysis

## Completed Plans (`done/`)

- [done/I3X_SPEC.md](done/I3X_SPEC.md) — i3X v1 manufacturing API specification
- [done/MQTT5_IMPLEMENTATION_PLAN_ISSUE_86.md](done/MQTT5_IMPLEMENTATION_PLAN_ISSUE_86.md) — MQTT v5 implementation plan
- [done/MULTI_DB_ARCHIVE_CONNECTIONS.md](done/MULTI_DB_ARCHIVE_CONNECTIONS.md) — Multi-database archive connections
- [done/OPCUA_CLIENT_WRITE_SUPPORT_ISSUE_95.md](done/OPCUA_CLIENT_WRITE_SUPPORT_ISSUE_95.md) — OPC UA client write operations
- [done/PLAN-memory-metrics-store.md](done/PLAN-memory-metrics-store.md) — In-memory metrics store plan
- [done/PLAN-memory-session-queue-stores.md](done/PLAN-memory-session-queue-stores.md) — In-memory session and queue stores plan
- [done/UI_MQTT5_UPDATE_PLAN_ISSUE_86.md](done/UI_MQTT5_UPDATE_PLAN_ISSUE_86.md) — Dashboard UI updates for MQTT v5 features
- [done/WinCCUa_OpenPipe_Implementation.md](done/WinCCUa_OpenPipe_Implementation.md) — WinCC Unified Open Pipe implementation
