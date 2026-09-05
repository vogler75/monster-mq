package at.rocworks.stores.sqlite

import at.rocworks.Utils
import at.rocworks.stores.DataCatalogInstance
import at.rocworks.stores.DataCatalogRelation
import at.rocworks.stores.DataCatalogType
import at.rocworks.stores.IDataCatalogStore
import at.rocworks.stores.ImportDataCatalogResult
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.logging.Logger

class DataCatalogStoreSQLite(
    private val vertx: Vertx,
    private val dbPath: String
) : IDataCatalogStore {

    private val logger: Logger = Utils.getLogger(DataCatalogStoreSQLite::class.java)
    private lateinit var sqliteClient: SQLiteClient

    companion object {
        private const val TABLE_TYPES = "datacatalogtypes"
        private const val TABLE_INSTANCES = "datacataloginstances"
        private const val TABLE_RELATIONS = "datacatalogrelations"

        private val CREATE_TYPES_TABLE = """
            CREATE TABLE IF NOT EXISTS $TABLE_TYPES (
                id TEXT PRIMARY KEY,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                structure TEXT NOT NULL,
                topic_pattern TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """

        private val CREATE_INSTANCES_TABLE = """
            CREATE TABLE IF NOT EXISTS $TABLE_INSTANCES (
                id TEXT PRIMARY KEY,
                type_id TEXT NOT NULL,
                name TEXT NOT NULL,
                base_topic TEXT NOT NULL,
                properties TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (type_id) REFERENCES $TABLE_TYPES(id) ON DELETE CASCADE
            )
        """

        private val CREATE_RELATIONS_TABLE = """
            CREATE TABLE IF NOT EXISTS $TABLE_RELATIONS (
                source_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                relation_type TEXT NOT NULL,
                PRIMARY KEY (source_id, target_id, relation_type)
            )
        """

        private const val CREATE_INDEX_TYPES_NAMESPACE = "CREATE INDEX IF NOT EXISTS idx_dct_ns ON $TABLE_TYPES (namespace)"
        private const val CREATE_INDEX_INSTANCES_TYPE = "CREATE INDEX IF NOT EXISTS idx_dci_type ON $TABLE_INSTANCES (type_id)"
        private const val CREATE_INDEX_RELATIONS_SOURCE = "CREATE INDEX IF NOT EXISTS idx_dcr_src ON $TABLE_RELATIONS (source_id)"
        private const val CREATE_INDEX_RELATIONS_TARGET = "CREATE INDEX IF NOT EXISTS idx_dcr_tgt ON $TABLE_RELATIONS (target_id)"
    }

    override fun initialize(): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            sqliteClient = SQLiteClient(vertx, dbPath)
            val initSql = JsonArray()
                .add(CREATE_TYPES_TABLE)
                .add(CREATE_INSTANCES_TABLE)
                .add(CREATE_RELATIONS_TABLE)
                .add(CREATE_INDEX_TYPES_NAMESPACE)
                .add(CREATE_INDEX_INSTANCES_TYPE)
                .add(CREATE_INDEX_RELATIONS_SOURCE)
                .add(CREATE_INDEX_RELATIONS_TARGET)

            sqliteClient.initDatabase(initSql)
                .onSuccess {
                    logger.fine("DataCatalogStoreSQLite initialized successfully")
                    promise.complete()
                }
                .onFailure { error ->
                    logger.severe("Failed to initialize DataCatalogStoreSQLite: ${error.message}")
                    promise.fail(error)
                }
        } catch (e: Exception) {
            logger.severe("Exception starting SQLiteClient: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    private fun parseInstant(str: String?): Instant? {
        if (str.isNullOrEmpty()) return null
        return try {
            Instant.parse(str.replace(' ', 'T') + "Z")
        } catch (e: Exception) {
            null
        }
    }

    override fun getTypes(namespace: String?): Future<List<DataCatalogType>> {
        val promise = Promise.promise<List<DataCatalogType>>()
        val sql = if (namespace != null) "SELECT * FROM $TABLE_TYPES WHERE namespace = ?" else "SELECT * FROM $TABLE_TYPES"
        val params = if (namespace != null) JsonArray().add(namespace) else JsonArray()

        sqliteClient.executeQuery(sql, params)
            .onSuccess { rows ->
                val list = rows.map { r ->
                    val row = r as JsonObject
                    DataCatalogType(
                        id = row.getString("id"),
                        namespace = row.getString("namespace"),
                        name = row.getString("name"),
                        description = row.getString("description"),
                        structure = JsonObject(row.getString("structure") ?: "{}"),
                        topicPattern = row.getString("topic_pattern"),
                        createdAt = parseInstant(row.getString("created_at")),
                        updatedAt = parseInstant(row.getString("updated_at"))
                    )
                }
                promise.complete(list)
            }
            .onFailure { promise.fail(it) }
        return promise.future()
    }

    override fun getType(id: String): Future<DataCatalogType?> {
        val promise = Promise.promise<DataCatalogType?>()
        sqliteClient.executeQuery("SELECT * FROM $TABLE_TYPES WHERE id = ?", JsonArray().add(id))
            .onSuccess { rows ->
                if (rows.isEmpty) {
                    promise.complete(null)
                } else {
                    val row = rows.getJsonObject(0)
                    promise.complete(
                        DataCatalogType(
                            id = row.getString("id"),
                            namespace = row.getString("namespace"),
                            name = row.getString("name"),
                            description = row.getString("description"),
                            structure = JsonObject(row.getString("structure") ?: "{}"),
                            topicPattern = row.getString("topic_pattern"),
                            createdAt = parseInstant(row.getString("created_at")),
                            updatedAt = parseInstant(row.getString("updated_at"))
                        )
                    )
                }
            }
            .onFailure { promise.fail(it) }
        return promise.future()
    }

    override fun saveType(type: DataCatalogType): Future<DataCatalogType> {
        val promise = Promise.promise<DataCatalogType>()
        val sql = """
            INSERT INTO $TABLE_TYPES (id, namespace, name, description, structure, topic_pattern, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(id) DO UPDATE SET
                namespace=excluded.namespace,
                name=excluded.name,
                description=excluded.description,
                structure=excluded.structure,
                topic_pattern=excluded.topic_pattern,
                updated_at=datetime('now')
        """
        val params = JsonArray()
            .add(type.id)
            .add(type.namespace)
            .add(type.name)
            .add(type.description)
            .add(type.structure.encode())
            .add(type.topicPattern)

        sqliteClient.executeUpdate(sql, params)
            .onSuccess { promise.complete(type) }
            .onFailure { promise.fail(it) }
        return promise.future()
    }

    override fun deleteType(id: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        val cleanupSql = """
            DELETE FROM $TABLE_RELATIONS
            WHERE source_id = ? OR target_id = ?
               OR source_id IN (SELECT id FROM $TABLE_INSTANCES WHERE type_id = ?)
               OR target_id IN (SELECT id FROM $TABLE_INSTANCES WHERE type_id = ?)
        """
        sqliteClient.executeUpdate(cleanupSql, JsonArray().add(id).add(id).add(id).add(id))
            .compose { sqliteClient.executeUpdate("DELETE FROM $TABLE_TYPES WHERE id = ?", JsonArray().add(id)) }
            .onSuccess { rows -> promise.complete(rows > 0) }
            .onFailure { promise.fail(it) }
        return promise.future()
    }

    override fun getInstances(typeId: String?): Future<List<DataCatalogInstance>> {
        val promise = Promise.promise<List<DataCatalogInstance>>()
        val sql = if (typeId != null) "SELECT * FROM $TABLE_INSTANCES WHERE type_id = ?" else "SELECT * FROM $TABLE_INSTANCES"
        val params = if (typeId != null) JsonArray().add(typeId) else JsonArray()
        
        sqliteClient.executeQuery(sql, params)
            .onSuccess { rows ->
                val list = rows.map { r ->
                    val row = r as JsonObject
                    DataCatalogInstance(
                        id = row.getString("id"),
                        typeId = row.getString("type_id"),
                        name = row.getString("name"),
                        baseTopic = row.getString("base_topic"),
                        properties = JsonObject(row.getString("properties") ?: "{}"),
                        createdAt = parseInstant(row.getString("created_at")),
                        updatedAt = parseInstant(row.getString("updated_at"))
                    )
                }
                promise.complete(list)
            }
            .onFailure { promise.fail(it) }
        return promise.future()
    }

    override fun getInstance(id: String): Future<DataCatalogInstance?> {
        val promise = Promise.promise<DataCatalogInstance?>()
        sqliteClient.executeQuery("SELECT * FROM $TABLE_INSTANCES WHERE id = ?", JsonArray().add(id))
            .onSuccess { rows ->
                if (rows.isEmpty) {
                    promise.complete(null)
                } else {
                    val row = rows.getJsonObject(0)
                    promise.complete(
                        DataCatalogInstance(
                            id = row.getString("id"),
                            typeId = row.getString("type_id"),
                            name = row.getString("name"),
                            baseTopic = row.getString("base_topic"),
                            properties = JsonObject(row.getString("properties") ?: "{}"),
                            createdAt = parseInstant(row.getString("created_at")),
                            updatedAt = parseInstant(row.getString("updated_at"))
                        )
                    )
                }
            }
            .onFailure { promise.fail(it) }
        return promise.future()
    }

    override fun saveInstance(instance: DataCatalogInstance): Future<DataCatalogInstance> {
        val promise = Promise.promise<DataCatalogInstance>()
        val sql = """
            INSERT INTO $TABLE_INSTANCES (id, type_id, name, base_topic, properties, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            ON CONFLICT(id) DO UPDATE SET
                type_id=excluded.type_id,
                name=excluded.name,
                base_topic=excluded.base_topic,
                properties=excluded.properties,
                updated_at=datetime('now')
        """
        val params = JsonArray()
            .add(instance.id)
            .add(instance.typeId)
            .add(instance.name)
            .add(instance.baseTopic)
            .add(instance.properties.encode())

        sqliteClient.executeUpdate(sql, params)
            .onSuccess { promise.complete(instance) }
            .onFailure { promise.fail(it) }
        return promise.future()
    }

    override fun deleteInstance(id: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        sqliteClient.executeUpdate(
            "DELETE FROM $TABLE_RELATIONS WHERE source_id = ? OR target_id = ?",
            JsonArray().add(id).add(id)
        ).compose { sqliteClient.executeUpdate("DELETE FROM $TABLE_INSTANCES WHERE id = ?", JsonArray().add(id)) }
            .onSuccess { rows -> promise.complete(rows > 0) }
            .onFailure { promise.fail(it) }
        return promise.future()
    }

    override fun getRelations(sourceId: String?, targetId: String?, relationType: String?): Future<List<DataCatalogRelation>> {
        val promise = Promise.promise<List<DataCatalogRelation>>()
        var sql = "SELECT * FROM $TABLE_RELATIONS WHERE 1=1"
        val params = JsonArray()
        if (sourceId != null) { sql += " AND source_id = ?"; params.add(sourceId) }
        if (targetId != null) { sql += " AND target_id = ?"; params.add(targetId) }
        if (relationType != null) { sql += " AND relation_type = ?"; params.add(relationType) }

        sqliteClient.executeQuery(sql, params)
            .onSuccess { rows ->
                val list = rows.map { r ->
                    val row = r as JsonObject
                    DataCatalogRelation(
                        sourceId = row.getString("source_id"),
                        targetId = row.getString("target_id"),
                        relationType = row.getString("relation_type")
                    )
                }
                promise.complete(list)
            }
            .onFailure { promise.fail(it) }
        return promise.future()
    }

    override fun saveRelation(relation: DataCatalogRelation): Future<DataCatalogRelation> {
        val promise = Promise.promise<DataCatalogRelation>()
        val sql = """
            INSERT INTO $TABLE_RELATIONS (source_id, target_id, relation_type)
            VALUES (?, ?, ?)
            ON CONFLICT(source_id, target_id, relation_type) DO NOTHING
        """
        val params = JsonArray()
            .add(relation.sourceId)
            .add(relation.targetId)
            .add(relation.relationType)

        sqliteClient.executeUpdate(sql, params)
            .onSuccess { promise.complete(relation) }
            .onFailure { promise.fail(it) }
        return promise.future()
    }

    override fun deleteRelation(sourceId: String, targetId: String, relationType: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        val sql = "DELETE FROM $TABLE_RELATIONS WHERE source_id = ? AND target_id = ? AND relation_type = ?"
        val params = JsonArray().add(sourceId).add(targetId).add(relationType)
        sqliteClient.executeUpdate(sql, params)
            .onSuccess { promise.complete(true) }
            .onFailure { promise.fail(it) }
        return promise.future()
    }

    override fun close(): Future<Void> {
        return Future.succeededFuture()
    }

    override fun exportCatalog(namespace: String?): Future<JsonObject> =
        at.rocworks.stores.DataCatalogTransfer.export(this, namespace)

    override fun importCatalog(data: JsonObject): Future<ImportDataCatalogResult> =
        at.rocworks.stores.DataCatalogTransfer.import(this, data)
}
