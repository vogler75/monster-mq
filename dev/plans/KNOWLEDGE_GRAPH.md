# Knowledge Graph Implementation Plan (Apache Jena Triple Store)

Implement an embedded **Semantic Knowledge Graph** for MonsterMQ based on the **W3C Semantic Web stack (RDF / OWL / SPARQL)** using **Apache Jena**.

This feature enables MonsterMQ to act as a **semantic digital twin broker** where physical MQTT topics bind directly to entities in standardized domain ontologies (Brick Schema, W3C Web of Things, SAREF, SOSA/SSN, Asset Administration Shell). Clients can subscribe to semantic classes, locations, or SPARQL patterns, and the broker dynamically routes messages from the underlying physical MQTT topics.

---

## 1. Overview & Architectural Decisions

### Standards-First Architecture
Rather than creating custom graph models or ad-hoc relational tables, MonsterMQ adopts proven open standards:
- **W3C RDF 1.1 / RDF\***: Triple/quad data model (`subject`, `predicate`, `object`, `graph`).
- **W3C SPARQL 1.1**: Standard query and update language over `/sparql`.
- **W3C Web of Things (WoT) Thing Description**: Protocol binding standard mapping RDF nodes to MQTT topics (`td:hasForm`, `td:href`).
- **Brick Schema**: Building automation, HVAC, energy, and spatial relationships.
- **W3C SOSA / SSN & ETSI SAREF**: Sensors, actuators, and observations.
- **Asset Administration Shell (AAS) & OPC UA**: Industrial digital twin structures.

### Core Architecture: In-Memory Engine + Main DB Persistence
1. **Engine**: Pure JVM [Apache Jena](https://jena.apache.org/) (Apache 2.0 license).
2. **Runtime Execution**: In-memory `DatasetGraphInMemory` with RDFS/OWL micro-reasoners for wire-speed graph lookups and zero-latency MQTT dispatch.
3. **Persistence**: Persisted **exclusively via MonsterMQ's configured main databases** (PostgreSQL, SQLite, MongoDB).

### Architecture Limitations & Scope Boundaries
- **In-Memory Working Set**: The active Knowledge Graph dataset lives entirely in JVM memory. It is not stored in separate native graph files (e.g. Jena TDB2 disk files are not used).
- **Scale Target**: Digital twin models, asset hierarchies, and device metadata typically range from 1,000 to 500,000 triples. In-memory storage for this size requires only ~50MB to ~300MB of JVM heap, which fits comfortably within standard broker memory allocations.
- **Not for Telemetry Storage**: The triple store holds **metadata, topology, and topic bindings**, NOT high-volume time-series telemetry. Telemetry remains in `IMessageArchive` (PostgreSQL, SQLite, CrateDB, MongoDB, or Kafka).
- **Persistence Boundary**: All mutations (via SPARQL Update, GraphQL, or UI upload) write to the main database first (or transactionally), then commit to the in-memory Jena dataset and broadcast a reload event across the Hazelcast cluster.

---

## 2. Topic Subscriptions via the Knowledge Graph

### W3C Web of Things (WoT) Topic Binding
In the Knowledge Graph, physical MQTT topics are bound to semantic nodes using standard W3C WoT ontology predicates:

```turtle
@prefix td:    <https://www.w3.org/2019/wot/td#> .
@prefix brick: <https://brickschema.org/schema/Brick#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex:    <https://monstermq.org/assets#> .

ex:Extruder4_MeltTemp a brick:Temperature_Sensor ;
    rdfs:label "Extruder 4 Melt Temperature" ;
    brick:isPointOf ex:Extruder4 ;
    td:hasForm [
        td:href "factory/line1/extruder4/temperature" ;
        td:subprotocol "mqtt"
    ] .

ex:Extruder4 a brick:Equipment ;
    brick:hasLocation ex:Hall_A .
```

### Subscription Modes

#### Mode 1: Semantic Topic Subscriptions (`$kg/...`)
Clients can subscribe to semantic paths through the broker:
- **By Type**: `$kg/by-type/brick:Temperature_Sensor`
- **By Subtree / Location**: `$kg/by-location/ex:Hall_A/#`
- **By Asset**: `$kg/by-asset/ex:Extruder4/#`

**Resolution Flow**:
1. When a client subscribes to `$kg/by-type/brick:Temperature_Sensor`, the broker queries Jena in-memory:
   ```sparql
   PREFIX td: <https://www.w3.org/2019/wot/td#>
   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
   SELECT ?topic WHERE {
       ?sensor a/rdfs:subClassOf* ?targetType ;
               td:hasForm [ td:href ?topic ] .
   }
   ```
2. With RDFS reasoning active, specialized classes (e.g. `brick:Zone_Air_Temperature_Sensor`) are automatically included.
3. The broker registers the client session against all matching physical topics (`factory/line1/extruder4/temperature`) in `SubscriptionManager`.
4. Outgoing messages can optionally be rewritten to the semantic path or enriched with JSON-LD metadata.

#### Mode 2: Inverted Index on Ingest (Hot Path)
1. On startup or graph reload, MonsterMQ builds an in-memory inverted hash map:
   `topicToNodes: Map<String, List<Resource>>` (e.g. `"factory/line1/extruder4/temperature"` $\rightarrow$ `[ex:Extruder4_MeltTemp]`).
2. When a physical publish arrives, the broker does an $O(1)$ map lookup.
3. If matched, subscribers listening to the node's semantic URI or parent graph categories receive the message without running any SPARQL query on the hot path.

#### Mode 3: Dynamic Graph Updates
When a node is updated (e.g. moving a sensor from `Hall_A` to `Hall_B`):
- Graph mutation is committed to the main DB and in-memory Jena dataset.
- The subscription resolver recalculates affected physical topic bindings.
- Active client sessions are updated transparently without requiring MQTT reconnection.

---

## 3. Storage Layer: Reusing Main Databases

### Schema Parity Across Backends

All triples are persisted in a single table / collection across MonsterMQ's supported database backends.

#### PostgreSQL (`stores/dbs/postgres/KnowledgeGraphStorePostgres.kt`)
```sql
CREATE TABLE IF NOT EXISTS kg_triples (
    id VARCHAR(64) PRIMARY KEY,
    subject VARCHAR(512) NOT NULL,
    predicate VARCHAR(512) NOT NULL,
    object TEXT NOT NULL,
    is_literal BOOLEAN DEFAULT FALSE,
    datatype VARCHAR(256),
    lang VARCHAR(16),
    graph_uri VARCHAR(512) DEFAULT 'urn:default',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_kg_spo ON kg_triples(subject, predicate);
CREATE INDEX IF NOT EXISTS idx_kg_pos ON kg_triples(predicate, object);
CREATE INDEX IF NOT EXISTS idx_kg_ops ON kg_triples(object, predicate);
CREATE INDEX IF NOT EXISTS idx_kg_graph ON kg_triples(graph_uri);
```

#### SQLite (`stores/dbs/sqlite/KnowledgeGraphStoreSqlite.kt`)
```sql
CREATE TABLE IF NOT EXISTS kg_triples (
    id TEXT PRIMARY KEY,
    subject TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object TEXT NOT NULL,
    is_literal INTEGER DEFAULT 0,
    datatype TEXT,
    lang TEXT,
    graph_uri TEXT DEFAULT 'urn:default',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_kg_spo ON kg_triples(subject, predicate);
CREATE INDEX IF NOT EXISTS idx_kg_pos ON kg_triples(predicate, object);
CREATE INDEX IF NOT EXISTS idx_kg_ops ON kg_triples(object, predicate);
CREATE INDEX IF NOT EXISTS idx_kg_graph ON kg_triples(graph_uri);
```

#### MongoDB (`stores/dbs/mongodb/KnowledgeGraphStoreMongo.kt`)
Collection `kg_triples`:
```json
{
  "_id": "hash-or-uuid",
  "subject": "https://monstermq.org/assets#Extruder4_MeltTemp",
  "predicate": "https://brickschema.org/schema/Brick#isPointOf",
  "object": "https://monstermq.org/assets#Extruder4",
  "isLiteral": false,
  "datatype": null,
  "lang": null,
  "graphUri": "urn:default",
  "updatedAt": "2026-09-27T07:45:00Z"
}
```
Compound indexes on `{ subject: 1, predicate: 1 }`, `{ predicate: 1, object: 1 }`, `{ graphUri: 1 }`.

### Document Snapshot Storage (`IDeviceConfigStore`)
In addition to the granular triple table, named graphs and standard ontologies (e.g. `brick.ttl`) can be stored as raw Turtle (`.ttl`) documents inside `IDeviceConfigStore` (type: `"KnowledgeGraph"`). This enables:
- One-click import/export of entire `.ttl` / `.jsonld` files.
- Version control / GitOps synchronization.
- Seamless disaster recovery alongside device configurations.

---

## 4. Broker Lifecycle & Cluster Synchronization

```
                                +-----------------------------+
                                |  PostgreSQL / SQLite / DB   |
                                |     Table: kg_triples       |
                                +--------------+--------------+
                                               |
                          1. Boot Load Triples | 4. Persist Mutations
                                               v
+-----------------------------------------------------------------------------------+
|  MonsterMQ Broker Node (JVM)                                                      |
|                                                                                   |
|  +-----------------------------------------------------------------------------+  |
|  |  Apache Jena In-Memory Dataset (DatasetGraphInMemory + RDFS Reasoner)       |  |
|  +-----------------------------------------------------------------------------+  |
|          ^                                      |                                 |
|          | 2. Build Cache                       | 3. Query Topics                 |
|          v                                      v                                 |
|  +-------------------------------+      +--------------------------------------+  |
|  | Inverted Topic-to-Node Cache  |      | Semantic Subscriptions Manager       |  |
|  | (td:href -> Node URI)         |      | ($kg/by-type/..., SPARQL patterns)   |  |
|  +---------------+---------------+      +-------------------+------------------+  |
|                  ^                                          |                     |
|                  | Fast O(1) Match                          | Wire Subscriptions  |
|                  v                                          v                     |
|  +-------------------------------+      +--------------------------------------+  |
|  | Hot-Path MQTT Ingest (Publish)|      | SubscriptionManager / SessionHandler |  |
|  +-------------------------------+      +--------------------------------------+  |
+-----------------------------------------------------------------------------------+
```

### Cluster Synchronization (Hazelcast / Vert.x EventBus)
When a SPARQL update or ontology import occurs on Node A:
1. Node A writes the triples to the central database (`kg_triples`).
2. Node A updates its local in-memory Jena `DatasetGraph` and topic cache.
3. Node A broadcasts `EventBusAddresses.KnowledgeGraph.RELOAD` (or sends triple deltas).
4. Node B & Node C reload the triples from the database into their in-memory Jena datasets and refresh their topic indexes without dropping MQTT client connections.

---

## 5. Protocol Endpoints & API

### 1. W3C SPARQL Protocol Endpoint (`/sparql`)
Mounted on Vert.x HTTP server:
- `GET /sparql?query=...` $\rightarrow$ returns `application/sparql-results+json` or XML.
- `POST /sparql` (body: SPARQL Query/Update) $\rightarrow$ full SPARQL 1.1 Query & Update execution.

### 2. GraphQL Schema Interface
```graphql
type KnowledgeGraphStats {
    totalTriples: Int!
    namedGraphs: [String!]!
    boundTopicsCount: Int!
}

type SparqlQueryResult {
    vars: [String!]!
    bindings: [JSON!]!
    executionTimeMs: Int!
}

extend type Query {
    kgStats: KnowledgeGraphStats!
    kgSparqlQuery(query: String!): SparqlQueryResult!
    kgBoundTopics: [String!]!
}

extend type Mutation {
    kgSparqlUpdate(update: String!): Boolean!
    kgImportTurtle(turtleContent: String!, graphUri: String): Int!
    kgClearGraph(graphUri: String): Boolean!
}
```

### 3. Siemens iX Web Dashboard UI
- **Knowledge Graph Explorer** (`dashboard/src/pages/knowledge-graph.html`):
  - SPARQL interactive query editor with syntax highlighting and results table.
  - Ontology import tool (upload `.ttl`, `.owl`, `.jsonld` files or select Brick Schema preset).
  - Visual topic binding table: see which semantic nodes point to which live MQTT topics.
- **Sidebar Integration**:
  - Added to `sidebar.js` under `Broker` with `feature: 'KnowledgeGraph'`.

---

## 6. Data Catalog & CESMII i3X Unification (Option A: High-Level Facade)

### The Convergence Strategy
MonsterMQ currently includes a **Data Catalog** feature (`Features.DataCatalog`, backed by `IDataCatalogStore`), modeled after the **CESMII i3X specification** (Object Types, Object Instances, and Relations).

Instead of maintaining duplicate database tables and redundant storage:
- **Single Source of Truth**: The Knowledge Graph (Apache Jena) becomes the sole underlying semantic store.
- **Data Catalog as a Facade**: The existing Data Catalog GraphQL queries/mutations, CESMII i3X REST API (`/i3x/v1/*`), and MCP Server tools (`getDataCatalogTypes`, `getDataCatalogInstances`, `getDataCatalogRelations`) remain fully supported, but operate as a **high-level view / projection** directly on top of the in-memory Jena triple store.
- **Zero Breaking Changes**: Existing dashboards, i3X clients, and AI agents continue functioning without API modifications, while gaining the reasoning and semantic traversal capabilities of the triple store.

### Semantic Mapping Model

| Data Catalog Element | Knowledge Graph / RDF Equivalent | Notes |
| :--- | :--- | :--- |
| **`DataCatalogType`** | `rdfs:Class` / `owl:Class` | `type.id` $\leftrightarrow$ Class URI. `structure` (JSON Schema) is stored as a literal property (e.g. `ex:hasJsonSchema`). `topicPattern` maps to `td:hasForm`. |
| **`DataCatalogInstance`** | RDF Individual (Named Subject) | `instance.id` $\leftrightarrow$ Individual URI. Typed with `rdf:type` to its `DataCatalogType`. |
| **`instance.baseTopic`** | W3C WoT `td:hasForm [ td:href ?baseTopic ]` | Automatically enters the inverted topic cache for live MQTT routing. |
| **`instance.properties`** | RDF Predicate-Object literals / JSON-LD | Flattened into RDF properties or preserved as structured JSON literal. |
| **`DataCatalogRelation`** | RDF Triple (`subject predicate object`) | `sourceId` (Subject), `relationType` (Predicate URI), `targetId` (Object). |

### Storage & Architectural Impact
- **Elimination of Redundant Tables**: Dedicated `datacatalogtypes`, `datacataloginstances`, and `datacatalogrelations` tables/collections in PostgreSQL, SQLite, and MongoDB are unified into the central `kg_triples` store.
- **`IDataCatalogStore` Adapter**: Implement `DataCatalogStoreJenaAdapter : IDataCatalogStore` that delegates `getTypes()`, `getInstances()`, `getRelations()`, and save/delete methods to in-memory SPARQL queries against Jena. This preserves full backwards compatibility for `I3xServer` and `McpHandler`.

---

## 7. Implementation Roadmap

- [ ] **Phase 1: Dependencies & Store Interface**
  - Add `org.apache.jena:jena-core` and `org.apache.jena:jena-arq` to `broker/pom.xml`.
  - Define `IKnowledgeGraphStore` interface in `internal/stores/interfaces.go` & `stores/IKnowledgeGraphStore.kt`.
- [ ] **Phase 2: Database Implementations**
  - Implement `KnowledgeGraphStorePostgres`, `KnowledgeGraphStoreSqlite`, and `KnowledgeGraphStoreMongo`.
  - DDL migrations and unit tests for triple CRUD.
- [ ] **Phase 3: Jena In-Memory Engine & Topic Indexing**
  - Implement `KnowledgeGraphManager` verticle wrapping Jena's `DatasetGraphInMemory`.
  - Inverted index for `td:href` physical topic mapping.
  - EventBus reload handler for multi-node cluster sync.
- [ ] **Phase 4: MQTT Subscription Hooks**
  - Wire `$kg/...` subscriptions into `SessionHandler` and `SubscriptionManager`.
  - Wire fast topic-to-node matching into `MqttClient.publishHandler`.
- [ ] **Phase 5: Data Catalog & i3X Facade Adapter**
  - Implement `DataCatalogStoreJenaAdapter : IDataCatalogStore` mapping catalog operations to SPARQL/Jena queries.
  - Verify `I3xServer` and MCP tools seamlessly operate over the Knowledge Graph.
- [ ] **Phase 6: SPARQL Endpoint & GraphQL API**
  - Expose `/sparql` HTTP endpoint on Vert.x.
  - Implement GraphQL queries and mutations (`kgSparqlQuery`, `kgImportTurtle`).
- [ ] **Phase 7: Siemens iX Dashboard & Pytest Suite**
  - Build Knowledge Graph page in `dashboard/src/pages/knowledge-graph.html`.
  - End-to-end integration tests using Brick Schema and W3C WoT with live MQTT publish/subscribe.
