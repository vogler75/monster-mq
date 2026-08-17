# Data Catalog Implementation Plan

Implement a **Data Catalog** feature for MonsterMQ (main and edge brokers) that allows users to define Object Types, Object Instances, and Relations between them, closely mirroring the semantic model of the CESMII i3X specification.

This provides a structured semantic layer over the broker's MQTT topics without cluttering the topic namespace itself with metadata topics. The data catalog is stored in the configuration database, fully manageable and queryable via GraphQL, readable by the MCP server, and exposes the semantic relationships necessary to serve an i3X API endpoint directly from the broker.

---

## 1. Overview & Concepts

- **DataCatalogType (Object Type)**: Defines the schema, structure (JSON Schema), topic pattern, and namespace for a class of devices/entities.
- **DataCatalogInstance (Object Instance)**: Points to a concrete entity mapped to a `baseTopic`, associated with a `typeId`, with custom instance properties.
- **DataCatalogRelation (Relationship)**: Directed relationships between entities (e.g. `sourceId`, `targetId`, `relationType` like `HasParent`, `ConnectedTo`, `ComponentOf`).
- **Import / Export**: Full serialization/deserialization to/from JSON for migration, backups, and synchronizing between central and edge brokers.

---

## 2. Storage Layer Architecture

Tables / Collections shared across backends:
- `data_catalog_types` (`id`, `namespace`, `name`, `description`, `structure`, `topic_pattern`, `created_at`, `updated_at`)
- `data_catalog_instances` (`id`, `type_id`, `name`, `base_topic`, `properties`, `created_at`, `updated_at`)
- `data_catalog_relations` (`source_id`, `target_id`, `relation_type`)

### Backends
- **SQLite**: Pure SQL DDL + JSON serialization (`modernc.org/sqlite` in Go, SQLite JDBC in Kotlin).
- **PostgreSQL**: Same table definitions using `JSONB` for `structure` and `properties` (`pgx/v5` in Go, `DatabaseConnection` in Kotlin).
- **MongoDB**: `datacatalog` database with collections `types`, `instances`, `relations` (`mongo-driver/v2` in Go, reactive streams client in Kotlin).

---

## 3. GraphQL Schema & API

```graphql
type DataCatalogType {
    id: String!
    namespace: String!
    name: String!
    description: String
    structure: JSON!
    topicPattern: String
    createdAt: String
    updatedAt: String
}

input DataCatalogTypeInput {
    id: String!
    namespace: String!
    name: String!
    description: String
    structure: JSON!
    topicPattern: String
}

type DataCatalogInstance {
    id: String!
    typeId: String!
    name: String!
    baseTopic: String!
    properties: JSON!
    createdAt: String
    updatedAt: String
}

input DataCatalogInstanceInput {
    id: String!
    typeId: String!
    name: String!
    baseTopic: String!
    properties: JSON!
}

type DataCatalogRelation {
    sourceId: String!
    targetId: String!
    relationType: String!
}

input DataCatalogRelationInput {
    sourceId: String!
    targetId: String!
    relationType: String!
}

type ImportDataCatalogResult {
    success: Boolean!
    typesImported: Int!
    instancesImported: Int!
    relationsImported: Int!
    failed: Int!
    errors: [String!]!
}

extend type Query {
    dataCatalogTypes(namespace: String): [DataCatalogType!]!
    dataCatalogType(id: String!): DataCatalogType
    
    dataCatalogInstances(typeId: String): [DataCatalogInstance!]!
    dataCatalogInstance(id: String!): DataCatalogInstance
    
    dataCatalogRelations(sourceId: String, targetId: String, relationType: String): [DataCatalogRelation!]!
}

type DataCatalogMutations {
    saveType(input: DataCatalogTypeInput!): DataCatalogType!
    deleteType(id: String!): Boolean!
    
    saveInstance(input: DataCatalogInstanceInput!): DataCatalogInstance!
    deleteInstance(id: String!): Boolean!
    
    saveRelation(input: DataCatalogRelationInput!): DataCatalogRelation!
    deleteRelation(sourceId: String!, targetId: String!, relationType: String!): Boolean!
    
    exportCatalog(namespace: String): JSON!
    importCatalog(data: JSON!): ImportDataCatalogResult!
}

extend type Mutation {
    dataCatalog: DataCatalogMutations
}
```

---

## 4. Integrations

1. **MCP Server**:
   - Exposes tools `get-datacatalog-types`, `get-datacatalog-instances`, and `get-datacatalog-relations` so AI agents can introspect the broker's semantic structure and instances.
2. **i3X API Server**:
   - Mapped natively under `/i3x/v1/objecttypes`, `/i3x/v1/objects`, and `/i3x/v1/relationshiptypes`.
3. **Edge Broker Parity**:
   - Identical GraphQL API and storage layout in Go.
