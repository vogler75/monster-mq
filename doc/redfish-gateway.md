# Redfish Gateway

This document provides a comprehensive overview of what **DMTF Redfish** is, how the **MonsterMQ Redfish Gateway** works, how MQTT telemetry is mapped to standard Redfish REST endpoints, and how to configure and use it with practical examples.

---

## 1. What is Redfish? (The Basics)

### 1.1 Overview
[Redfish®](https://www.dmtf.org/standards/redfish) is an open industry standard managed by the **Distributed Management Task Force (DMTF)** (DSP0266 specification). It defines a modern, secure, hypermedia-driven **RESTful API** formatted in **JSON** (following OData v4 conventions) for monitoring and managing compute systems, edge nodes, enclosures, data centers, and physical infrastructure.

Historically, hardware monitoring relied on legacy protocols like **IPMI** (Intelligent Platform Management Interface) or vendor-specific SNMP traps. Redfish replaces these with standard HTTP/HTTPS methods (`GET`, `POST`, etc.) returning predictable, schema-validated JSON.

### 1.2 Core Redfish Concepts & Hierarchy

Redfish organizes resources as a hypermedia tree rooted at `/redfish/v1`:

```text
/redfish/v1/                                     (ServiceRoot)
 ├── Chassis/                                    (Physical enclosures, racks, edge boxes)
 │    └── {ChassisID}/                           (e.g., "EdgeNode", "Rack-1")
 │         ├── Sensors/                          (Modern standalone sensor resources)
 │         │    └── {SensorID}                   (e.g., "cpu-temp", "fan-1", "power-supply")
 │         ├── Thermal                           (Legacy temperature readings & fans)
 │         └── Power                             (Legacy voltages & power control)
 ├── Systems/                                    (Compute instances / OS / host systems)
 │    └── {SystemID}/                            (e.g., "edge-node", "server-01")
 ├── Managers/                                   (Management controllers / BMC / daemon)
 │    └── {ManagerID}/                           (e.g., "monstermq-edge")
 ├── TelemetryService/                           (Metric reports & telemetry streams)
 │    └── MetricReports/{ReportID}
 └── EventService/                               (SSE / Webhook notifications & alerts)
```

| Redfish Resource | Purpose | Examples |
|:---|:---|:---|
| **`ServiceRoot`** (`/redfish/v1`) | API entry point, versioning, links to collections | Product UUID, Redfish version (1.18.0) |
| **`Chassis`** (`/redfish/v1/Chassis/{id}`) | Represents the physical enclosure, rack, or edge box | Dimensions, enclosure status, health rollup |
| **`Sensor`** (`.../Chassis/{id}/Sensors/{id}`) | Modern standalone sensor metric with status & thresholds | Temperature (`Cel`), Voltage (`V`), Power (`W`), Humidity (`%`), Pressure (`Pa`) |
| **`Thermal`** (`.../Chassis/{id}/Thermal`) | Aggregated temperature sensors and fans | Backwards-compatibility for older DCIM tools |
| **`Power`** (`.../Chassis/{id}/Power`) | Aggregated voltages, power supplies, wattages | Backwards-compatibility for older power monitors |
| **`System`** (`/redfish/v1/Systems/{id}`) | Logical compute entity / edge device info | CPU status, memory health, power state |
| **`Manager`** (`/redfish/v1/Managers/{id}`) | Controller management daemon | MonsterMQ service health, firmware version |
| **`TelemetryService`** | Aggregated telemetry & metric reports | Periodic multi-metric snapshots |

---

## 2. Why MonsterMQ has a Redfish Gateway

Industrial edge nodes, IoT sensors, and PLCs typically publish telemetry via **MQTT** topics (e.g. `sensors/rack1/temperature`, `factory/line1/power`).

Monitoring systems can consume Redfish resources directly or through a compatible collector/exporter. Compatibility depends on the resources that the particular client expects.

The **MonsterMQ Redfish Gateway** bridges this gap:

1. **Subscribes** to MQTT topics matching configured wildcard filters.
2. **Extracts & Normalizes** incoming payloads using the configured JSONPath mappings and array expansion. The `jsonSchema` field is a mapping configuration; the current mapper does not perform full JSON Schema validation.
3. **Calculates Health State** automatically against warning/caution/critical thresholds.
4. **Caches** normalized sensor states in a last-value store: the `Default` archive group's store when available, otherwise another deployed group's store, then an in-memory fallback.
5. **Serves standard DMTF Redfish REST APIs** under `/redfish/v1/...` to external monitoring tools.

```text
┌─────────────────────────┐
│ MQTT Devices / Sensors  │
└────────────┬────────────┘
             │  MQTT Publish (e.g. sensors/rack1/temp)
             ▼
┌────────────────────────────────────────────────────────┐
│ MonsterMQ Broker (Core)                                │
│                                                        │
│   ┌────────────────────────────────────────────────┐   │
│   │ Redfish Ingestion Engine                       │   │
│   │  - JSON Schema / JSONPath extraction           │   │
│   │  - Array expansion (arrayPath: $.readings[*])  │   │
│   │  - Threshold & health evaluation               │   │
│   └───────────────────────┬────────────────────────┘   │
│                           │                            │
│                           │ Writes normalized sensor   │
│                           ▼                            │
│   ┌────────────────────────────────────────────────┐   │
│   │ Last-Value Store                         │   │
│   │ ({topicPrefix}/{chassisId}/sensors/{sensorId}) │   │
│   └───────────────────────┬────────────────────────┘   │
│                           │                            │
│   ┌───────────────────────┴────────────────────────┐   │
│   │ Redfish HTTP REST Server (/redfish/v1/...)     │   │
│   └───────────────────────┬────────────────────────┘   │
└───────────────────────────┼────────────────────────────┘
                            │
                            │ Standard HTTP GET (/redfish/v1/Chassis/EdgeNode/Sensors)
                            ▼
        ┌───────────────────────────────────────┐
        │ DCIM / Prometheus / Zabbix / Clients  │
        └───────────────────────────────────────┘
```

---

## 3. Configuration

Redfish configuration consists of two layers:
1. **Server Configuration** (`config.yaml`): Feature enablement and HTTP listener settings.
2. **Data Gateways** (Dynamic in `DeviceConfigStore`): Topic subscriptions and payload mappings managed via GraphQL / Web Dashboard.

### 3.1 Server Configuration (`config.yaml`)

Enable the Redfish feature in `config.yaml`:

```yaml
GraphQL:
  Enabled: true
  Port: 4000

Features:
  Redfish: true

Redfish:
  Enabled: true
  MountPath: "/redfish/v1"        # Path where Redfish REST is mounted (default: /redfish/v1)
  DefaultChassisId: "EdgeNode"    # Default Chassis ID if not specified in mapping
  DefaultSystemId: "edge-node"    # System ID exposed under /redfish/v1/Systems
  DefaultManagerId: "monstermq"   # Manager ID exposed under /redfish/v1/Managers
  AnonymousEnabled: true          # Allow unauthenticated GET access for monitoring tools
```

> **Note**: When `Port` is 0 (or omitted), Redfish is multiplexed onto the main HTTP/GraphQL port (default `4000`).

---

A positive `Redfish.Port` starts a separate HTTP listener. Keep the default mount path when clients follow returned links: the current response models embed `/redfish/v1` URLs.

### 3.2 Gateway Configuration Model

Each Redfish gateway configuration defines how MQTT messages are mapped to Redfish sensors:

| Field | Type | Description | Default |
|:---|:---|:---|:---|
| `name` | `String` | Unique gateway identifier | - |
| `enabled` | `Boolean` | Enable/disable this gateway | `true` |
| `topicPrefix` | `String` | Base topic where normalized sensor states are cached | `"redfish"` |
| `topicFilters` | `[String]` | MQTT subscription patterns to listen to | `["sensors/#"]` |
| `chassisId` | `String` | Redfish Chassis enclosure ID | `"EdgeNode"` |
| `defaultReadingType` | `String` | Default reading type if not in payload (see table below) | `"Temperature"` |
| `defaultReadingUnits` | `String` | Default engineering units (see table below) | `"Cel"` |
| `thresholds` | `Object` | Numeric threshold limits for automatic health calculation | `null` |
| `jsonSchema` | `Object` | Schema containing `mapping` (JSONPath) and `arrayPath` | `{}` |

#### Reading type and unit examples
- `Temperature` (`Cel`, `Fah`, `K`)
- `Voltage` (`V`, `mV`, `kV`)
- `Current` (`A`, `mA`)
- `Power` (`W`, `kW`, `MW`)
- `EnergykWh` (`kWh`, `MWh`)
- `Humidity` (`%`)
- `Pressure` (`Pa`, `kPa`, `Bar`, `mbar`, `psi`)
- `LiquidFlow` (`L/min`, `m3/h`)
- `Frequency` (`Hz`, `kHz`)
- `Percent` (`%`)
- `AirFlow` (`CFM`, `m3/h`)

The mapper passes through type/unit strings without validating the complete Redfish vocabulary or converting units. Choose values accepted by your target client. For fan speed, the examples below use `Rotational` with `RPM`.

#### Health Calculation Rules
When `thresholds` (`upperCaution`, `upperCritical`, `lowerCaution`, `lowerCritical`) are defined:
- **`Critical`**: `reading >= upperCritical` OR `reading <= lowerCritical`
- **`Warning`**: `reading >= upperCaution` OR `reading <= lowerCaution`
- **`OK`**: Within normal bounds

---

## 4. Practical Examples

The gateway JSON examples below show a name, enabled flag, and configuration object. To save one, pass its `config` object as the `$cfg` variable in the GraphQL mutation in section 6, and use its `name` and `enabled` values. REST responses are illustrative excerpts; additional fields may be returned.

### Example 1: Simple Flat JSON Payload

#### MQTT Message
- **Topic**: `sensors/rack1/temperature`
- **Payload**:
  ```json
  {
    "sensor_id": "temp-cpu",
    "temperature": 48.5,
    "unit": "Cel"
  }
  ```

#### Gateway Configuration
```json
{
  "name": "RackTemperatureGateway",
  "enabled": true,
  "config": {
    "topicPrefix": "redfish",
    "topicFilters": ["sensors/+/temperature"],
    "chassisId": "Rack-01",
    "defaultReadingType": "Temperature",
    "defaultReadingUnits": "Cel",
    "thresholds": {
      "upperCaution": 70.0,
      "upperCritical": 85.0
    },
    "jsonSchema": {
      "mapping": {
        "sensorId": "$.sensor_id",
        "reading": "$.temperature",
        "readingUnits": "$.unit"
      }
    }
  }
}
```

#### Resulting Redfish REST Endpoint
**HTTP Request**:
```bash
curl http://localhost:4000/redfish/v1/Chassis/Rack-01/Sensors/temp-cpu
```

**HTTP 200 Response**:
```json
{
  "@odata.context": "/redfish/v1/$metadata#Sensor.Sensor",
  "@odata.id": "/redfish/v1/Chassis/Rack-01/Sensors/temp-cpu",
  "@odata.type": "#Sensor.v1_7_0.Sensor",
  "Id": "temp-cpu",
  "Name": "temp-cpu",
  "Reading": 48.5,
  "ReadingType": "Temperature",
  "ReadingUnits": "Cel",
  "Status": {
    "State": "Enabled",
    "Health": "OK"
  },
  "Thresholds": {
    "UpperCaution": { "Reading": 70 },
    "UpperCritical": { "Reading": 85 }
  }
}
```

---

### Example 2: Nested JSON Telemetry

#### MQTT Message
- **Topic**: `facility/edge-box-42/diagnostics`
- **Payload**:
  ```json
  {
    "device": "edge-box-42",
    "timestamp": "2026-08-20T14:30:00Z",
    "metrics": {
      "ambient": {
        "temp_deg": 31.2,
        "humidity_pct": 58.4
      },
      "electrical": {
        "input_voltage": 231.8,
        "power_draw_watts": 124.5
      }
    }
  }
  ```

#### Gateway Configuration (Single Mapping for Power)
```json
{
  "name": "EdgeBoxPowerGateway",
  "enabled": true,
  "config": {
    "topicPrefix": "redfish",
    "topicFilters": ["facility/+/diagnostics"],
    "chassisId": "EdgeNode",
    "defaultReadingType": "Power",
    "defaultReadingUnits": "W",
    "jsonSchema": {
      "mapping": {
        "sensorId": "$.device",
        "reading": "$.metrics.electrical.power_draw_watts",
        "ts": "$.timestamp"
      }
    }
  }
}
```

---

### Example 3: Multi-Sensor Array Payload (`arrayPath`)

Many edge gateways and loggers send a batch of multiple readings in a single message array. Using `arrayPath: "$.readings[*]"` unrolls each element into an independent Redfish sensor!

#### MQTT Message
- **Topic**: `datacenter/sensors/rack-a`
- **Payload**:
  ```json
  {
    "rack": "Rack-A",
    "timestamp": "2026-08-20T14:35:00Z",
    "readings": [
      { "id": "fan-inlet-1", "label": "Inlet Fan 1", "value": 3200, "type": "Rotational", "unit": "RPM" },
      { "id": "fan-inlet-2", "label": "Inlet Fan 2", "value": 3150, "type": "Rotational", "unit": "RPM" },
      { "id": "temp-exhaust", "label": "Exhaust Air", "value": 41.8, "type": "Temperature", "unit": "Cel" },
      { "id": "bus-voltage", "label": "48V Bus", "value": 48.1, "type": "Voltage", "unit": "V" }
    ]
  }
  ```

#### Gateway Configuration
```json
{
  "name": "RackMultiSensorGateway",
  "enabled": true,
  "config": {
    "topicPrefix": "redfish",
    "topicFilters": ["datacenter/sensors/+"],
    "chassisId": "Rack-A",
    "jsonSchema": {
      "arrayPath": "$.readings[*]",
      "mapping": {
        "sensorId": "$.id",
        "name": "$.label",
        "reading": "$.value",
        "readingType": "$.type",
        "readingUnits": "$.unit",
        "chassisId": "$.rack",
        "ts": "$.timestamp"
      }
    }
  }
}
```

#### Resulting Redfish Sensor Collection
**HTTP Request**:
```bash
curl http://localhost:4000/redfish/v1/Chassis/Rack-A/Sensors
```

**HTTP 200 Response**:
```json
{
  "@odata.context": "/redfish/v1/$metadata#SensorCollection.SensorCollection",
  "@odata.id": "/redfish/v1/Chassis/Rack-A/Sensors",
  "@odata.type": "#SensorCollection.SensorCollection",
  "Name": "Sensors Collection",
  "Members@odata.count": 4,
  "Members": [
    { "@odata.id": "/redfish/v1/Chassis/Rack-A/Sensors/fan-inlet-1" },
    { "@odata.id": "/redfish/v1/Chassis/Rack-A/Sensors/fan-inlet-2" },
    { "@odata.id": "/redfish/v1/Chassis/Rack-A/Sensors/temp-exhaust" },
    { "@odata.id": "/redfish/v1/Chassis/Rack-A/Sensors/bus-voltage" }
  ]
}
```

---

### Example 4: Legacy `/Thermal` and `/Power` Endpoints

Older monitoring systems that predate Redfish v1.6 look for aggregated thermal and power resources:

- **Thermal** (`GET /redfish/v1/Chassis/{chassisId}/Thermal`): Aggregates all sensors where `ReadingType == "Temperature"` into the `Temperatures` list, and fan speed sensors into the `Fans` list.
- **Power** (`GET /redfish/v1/Chassis/{chassisId}/Power`): Aggregates voltage sensors into `Voltages` and power sensors into `PowerControl`.

**Example Thermal Response**:
```json
{
  "@odata.context": "/redfish/v1/$metadata#Thermal.Thermal",
  "@odata.id": "/redfish/v1/Chassis/Rack-A/Thermal",
  "@odata.type": "#Thermal.v1_7_1.Thermal",
  "Id": "Thermal",
  "Name": "Thermal Information",
  "Temperatures": [
    {
      "@odata.id": "/redfish/v1/Chassis/Rack-A/Thermal#/Temperatures/0",
      "MemberId": "temp-exhaust",
      "Name": "Exhaust Air",
      "ReadingCelsius": 41.8,
      "Status": { "State": "Enabled", "Health": "OK" },
      "UpperThresholdNonCritical": 70,
      "UpperThresholdCritical": 85
    }
  ],
  "Fans": [
    {
      "@odata.id": "/redfish/v1/Chassis/Rack-A/Thermal#/Fans/0",
      "MemberId": "fan-inlet-1",
      "Name": "Inlet Fan 1",
      "Reading": 3200,
      "ReadingUnits": "RPM",
      "Status": { "State": "Enabled", "Health": "OK" }
    }
  ]
}
```

---

## 5. Summary of All Redfish REST Endpoints

| Method | Endpoint | Description |
|:---|:---|:---|
| `GET` | `/redfish/v1` | Redfish Service Root |
| `GET` | `/redfish/v1/odata` | OData v4 Service Document |
| `GET` | `/redfish/v1/$metadata` | CSDL XML Metadata Document |
| `GET` | `/redfish/v1/Chassis` | Collection of all known chassis/enclosures |
| `GET` | `/redfish/v1/Chassis/{chassisId}` | Chassis details, health rollup, and resource links |
| `GET` | `/redfish/v1/Chassis/{chassisId}/Sensors` | Modern standalone sensors collection |
| `GET` | `/redfish/v1/Chassis/{chassisId}/Sensors/{sensorId}` | Real-time sensor reading, unit, status, and thresholds |
| `GET` | `/redfish/v1/Chassis/{chassisId}/Thermal` | Legacy thermal data (temperature sensors & fans) |
| `GET` | `/redfish/v1/Chassis/{chassisId}/Power` | Legacy power data (voltages & power control) |
| `GET` | `/redfish/v1/Systems` | Computer system collection |
| `GET` | `/redfish/v1/Systems/{systemId}` | Edge node computer system info |
| `GET` | `/redfish/v1/Managers` | Manager collection |
| `GET` | `/redfish/v1/Managers/{managerId}` | Management controller/broker daemon details |
| `GET` | `/redfish/v1/TelemetryService` | Telemetry service information |
| `GET` | `/redfish/v1/TelemetryService/MetricReports` | Available metric reports |
| `GET` | `/redfish/v1/TelemetryService/MetricReports/{reportId}` | Aggregated sensor metric report |
| `GET` | `/redfish/v1/EventService` | Redfish event notifications |
| `GET` | `/redfish/v1/EventService/Subscriptions` | Empty event subscription collection |
| `GET` | `/redfish/v1/JsonSchemas` | Schema resource collection |
| `GET` | `/redfish/v1/JsonSchemas/{schemaId}` | Schema resource description |

The EventService response advertises `ServerSentEventUri`, but the current server does not register an SSE route or deliver webhook notifications. EventService and schema/metadata resources are limited descriptions, not a claim of full Redfish service conformance.

---

## 6. Managing Redfish Gateways via GraphQL & Web UI

### 6.1 Web Dashboard
In the MonsterMQ Dashboard:
1. Navigate to **Devices -> Redfish Gateways** in the sidebar.
2. Click **Create Gateway** to configure topic filters, chassis ID, and JSON mapping.
3. Use the built-in **Live Sensors Card** to view real-time readings, units, and health states directly in the browser.
4. Click **Open Service Root** or the sensor API links to test endpoints in your browser.

---

### 6.2 GraphQL API

#### List Gateways
```graphql
query {
  redfishMappings {
    name
    enabled
    config {
      topicPrefix
      topicFilters
      chassisId
      defaultReadingType
      defaultReadingUnits
    }
  }
}
```

#### Query Live Sensor Status
```graphql
query {
  redfishLiveSensors(chassisId: "EdgeNode") {
    id
    name
    chassisId
    reading
    readingType
    readingUnits
    health
    lastUpdated
  }
}
```

#### Save / Update a Gateway
```graphql
mutation SaveGateway($cfg: RedfishMappingConfigInput!) {
  saveRedfishMapping(name: "EnvGateway", config: $cfg, enabled: true) {
    success
    message
    redfish {
      name
      enabled
    }
  }
}
```

#### Delete a Gateway
```graphql
mutation DeleteGateway($name: String!) {
  deleteRedfishMapping(name: $name)
}
```

---

## 7. Prometheus & External Monitoring Integration

### Using with Prometheus Redfish Exporter
You can point Prometheus exporters (e.g. `prometheus-redfish-exporter` or custom scrape jobs) to:
```text
http://<monstermq-host>:4000/redfish/v1/Chassis/EdgeNode/Sensors
```
Verify that the chosen exporter supports the sensor resources this gateway exposes; automatic discovery is client-specific. Prometheus itself does not scrape arbitrary Redfish JSON. For MonsterMQ’s own Prometheus-compatible API, see [Grafana integration](grafana.md).

## Implementation references

- [Redfish GraphQL schema](../broker/src/main/resources/schema-redfish.graphqls)
- [REST routes](../broker/src/main/kotlin/extensions/redfish/RedfishServer.kt)
- [Payload mapping](../broker/src/main/kotlin/extensions/redfish/RedfishMapper.kt)
- [Ingestion and last-value storage](../broker/src/main/kotlin/extensions/redfish/RedfishIngestion.kt)
