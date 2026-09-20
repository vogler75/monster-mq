# Add Topic-Based Decision Making with OpenRouter / Jev

GitHub Issue: [#196](https://github.com/vogler75/monster-mq/issues/196)

## Summary

Introduce a standalone **Decision Making** capability in MonsterMQ, independent of the existing Agent framework.

The goal is to support fast, structured decision models such as **Jev via OpenRouter**. Decisions are triggered by MQTT topics, enriched with current and historical topic context, evaluated by a configured decision provider, and published back to MQTT.

MonsterMQ Flows are explicitly **out of scope**.

## Goals

- Decision Making is independent of Agents.
- MQTT topics act as decision triggers.
- Current topic values can be included as context using the existing LastVal functionality.
- Historical topic data can be included using MonsterMQ's existing historical-data capabilities.
- Historical context supports configurable time ranges and existing aggregations such as averages.
- Decision results are published to configurable MQTT topics.
- OpenRouter is the initial provider, with Jev as the initial decision model.
- The architecture should remain provider/model agnostic.

## Conceptual Flow

```text
MQTT Trigger Topic
       |
       v
Decision Definition
       |
       +--> Current Context
       |      └── LastVal(topic)
       |
       +--> Historical Context
       |      └── History(topic, time range, aggregation)
       |
       v
Context Snapshot
       |
       v
DecisionProvider
       |
       +--> OpenRouter
              └── Jev
       |
       v
Structured Decision Result
       |
       v
MQTT Result Topic
```

## Decision Definition

A decision configuration should contain at least:

- Name / ID
- Enabled state
- Trigger topic
- Result/output topic
- Decision instructions / question
- Provider
- Model
- Current Context configuration
- Historical Context configuration
- Provider-specific settings where required

A trigger message may itself also become part of the model input.

## 1. Current Context

Current Context uses MonsterMQ's existing **LastVal** functionality.

The user selects one or more MQTT topics whose current values should be included whenever the decision is triggered.

Example:

```yaml
currentContext:
  - topic: factory/line1/state
  - topic: factory/line1/motor/temperature
  - topic: factory/line1/motor/current
  - topic: factory/line1/production/speed
```

At decision time, MonsterMQ resolves the current LastVal value for every configured topic and adds it to the decision context.

## 2. Historical Context

Historical Context uses MonsterMQ's existing historical topic-data functionality.

Each historical context entry should allow configuration of:

- Topic
- Time range/window
- Aggregation
- Any additional historical query options already supported by MonsterMQ

Example:

```yaml
historicalContext:
  - topic: factory/line1/motor/temperature
    range: 10m
    aggregation: avg

  - topic: factory/line1/motor/current
    range: 5m
    aggregation: avg
```

The intention is **not** to implement a second historical-data engine. Decision Making should reuse MonsterMQ's existing query and aggregation capabilities.

Depending on what MonsterMQ already supports, useful modes may include:

```text
RAW
AVG
MIN
MAX
SUM
COUNT
```

The exact supported set should follow the existing historical API rather than introducing Decision-specific aggregation semantics.

## Dashboard

Decision Making should get its own configuration area in the MonsterMQ dashboard.

Within a Decision configuration, context should be visibly separated into two sections.

### Current Context

Configure topics whose current values are read from LastVal.

Possible UI:

```text
Current Context
────────────────────────────────────────
Topic
[ factory/line1/state                 ]

Topic
[ factory/line1/production/speed      ]

                         [+ Add Topic]
```

### Historical Context

Configure historical topic queries independently.

Possible UI:

```text
Historical Context
────────────────────────────────────────
Topic
[ factory/line1/motor/temperature     ]

Time Range
[ 10 minutes                           ]

Aggregation
[ Average ▼                            ]

                         [+ Add Query]
```

This separation is important because current state and historical behaviour represent different kinds of model context.

## Example Use Case

A production line publishes:

```text
factory/line1/motor/temperature
factory/line1/motor/current
factory/line1/production/speed
factory/line1/state
```

A decision is triggered by:

```text
factory/line1/decision/evaluate
```

Its configured context could be:

**Current Context**

```text
factory/line1/state
factory/line1/production/speed
```

**Historical Context**

```text
AVG(factory/line1/motor/temperature, last 10m)
AVG(factory/line1/motor/current, last 5m)
```

MonsterMQ could construct a provider request conceptually equivalent to:

```json
{
  "trigger": {
    "topic": "factory/line1/decision/evaluate",
    "payload": {
      "reason": "periodic-check"
    }
  },
  "current": {
    "factory/line1/state": "RUNNING",
    "factory/line1/production/speed": 94
  },
  "history": {
    "factory/line1/motor/temperature": {
      "range": "10m",
      "aggregation": "avg",
      "value": 81.4
    },
    "factory/line1/motor/current": {
      "range": "5m",
      "aggregation": "avg",
      "value": 14.7
    }
  }
}
```

The configured decision question might be:

> Determine whether the machine should continue running, be inspected at the next planned stop, or require immediate inspection.

The provider returns a structured decision, which MonsterMQ publishes to the configured result topic.

Example:

```json
{
  "decision": "INSPECT_NEXT_STOP",
  "confidence": 0.87
}
```

Published to:

```text
factory/line1/decision/result
```

## Architecture

Introduce a provider-neutral abstraction rather than coupling MonsterMQ directly to Jev.

Conceptually:

```java
public interface DecisionProvider {
    DecisionResult decide(DecisionRequest request);
}
```

Initial implementation:

```text
DecisionProvider
    └── OpenRouterDecisionProvider
             └── Jev
```

This leaves room for future providers or local decision models without changing the MQTT/context architecture.

## OpenRouter

The first provider implementation should use OpenRouter.

Configuration should include items such as:

```yaml
provider: openrouter
model: <jev-model-id>
```

Authentication should use MonsterMQ's existing secret/configuration conventions rather than storing API keys directly inside individual Decision definitions.

## Execution

When a configured trigger topic receives a message:

1. Match the MQTT message against configured Decision triggers.
2. Resolve all Current Context entries through LastVal.
3. Execute all configured Historical Context queries.
4. Build an immutable context snapshot for this decision execution.
5. Add the triggering topic/payload.
6. Send the request to the configured `DecisionProvider`.
7. Validate/parse the structured result.
8. Publish the result to the configured MQTT result topic.
9. Record execution metadata/errors using MonsterMQ's existing observability mechanisms.

## Non-Goals

For the initial implementation:

- No dependency on MonsterMQ Flows.
- No requirement to run an Agent.
- No conversational/chat functionality.
- No duplication of LastVal storage.
- No duplication of historical storage or aggregation functionality.
- No direct actuation of industrial equipment by the model itself.

Decision Making should produce a decision on MQTT. Existing consumers can determine what action, if any, follows from that decision.

## Future Extensions

The provider-neutral design could later support:

- Multiple Decision Providers
- Local/on-premise decision models
- Multiple output schemas
- Confidence thresholds
- Fallback providers
- Decision auditing
- Rate limiting
- Context aliases / transformations
- Wildcard topic context
- Scheduled decision triggers
- Decision chaining through MQTT topics

## Acceptance Criteria

- A Decision can be created/configured independently of Agents.
- A Decision is triggered by a configured MQTT topic.
- Trigger payload can be included in the decision input.
- Current Context topics can be configured and resolved through LastVal.
- Historical Context queries can be configured separately.
- Historical queries support a configurable time range.
- Existing MonsterMQ aggregation functionality can be selected for historical context.
- Dashboard presents **Current Context** and **Historical Context** as separate configuration areas.
- OpenRouter is available as the initial Decision Provider.
- Jev can be selected/configured as the initial model.
- Decision results are published to a configurable MQTT topic.
- Provider integration is abstracted behind a reusable `DecisionProvider` interface.
- The implementation does not depend on deprecated MonsterMQ Flows.
