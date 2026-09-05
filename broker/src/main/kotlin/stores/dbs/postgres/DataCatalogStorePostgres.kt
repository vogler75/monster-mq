package at.rocworks.stores.postgres

import at.rocworks.Utils
import at.rocworks.stores.DatabaseConnection
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
import java.sql.Connection
import java.sql.ResultSet
import java.util.logging.Logger

class DataCatalogStorePostgres(
    private val url: String,
    private val user: String,
    private val password: String,
    private val schema: String? = null
) : DatabaseConnection(Utils.getLogger(DataCatalogStorePostgres::class.java), url, user, password), IDataCatalogStore {

    private val logger: Logger = Utils.getLogger(DataCatalogStorePostgres::class.java)

    companion object {
        private const val TABLE_TYPES = "datacatalogtypes"
        private const val TABLE_INSTANCES = "datacataloginstances"
        private const val TABLE_RELATIONS = "datacatalogrelations"

        private val CREATE_TYPES_TABLE = """
            CREATE TABLE IF NOT EXISTS $TABLE_TYPES (
                id VARCHAR(255) PRIMARY KEY,
                namespace VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                structure JSONB NOT NULL,
                topic_pattern VARCHAR(255),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """

        private val CREATE_INSTANCES_TABLE = """
            CREATE TABLE IF NOT EXISTS $TABLE_INSTANCES (
                id VARCHAR(255) PRIMARY KEY,
                type_id VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                base_topic VARCHAR(255) NOT NULL,
                properties JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (type_id) REFERENCES $TABLE_TYPES(id) ON DELETE CASCADE
            )
        """

        private val CREATE_RELATIONS_TABLE = """
            CREATE TABLE IF NOT EXISTS $TABLE_RELATIONS (
                source_id VARCHAR(255) NOT NULL,
                target_id VARCHAR(255) NOT NULL,
                relation_type VARCHAR(255) NOT NULL,
                PRIMARY KEY (source_id, target_id, relation_type)
            )
        """

        private const val CREATE_INDEX_TYPES_NAMESPACE = "CREATE INDEX IF NOT EXISTS idx_dct_ns ON $TABLE_TYPES (namespace);"
        private const val CREATE_INDEX_INSTANCES_TYPE = "CREATE INDEX IF NOT EXISTS idx_dci_type ON $TABLE_INSTANCES (type_id);"
        private const val CREATE_INDEX_RELATIONS_SOURCE = "CREATE INDEX IF NOT EXISTS idx_dcr_src ON $TABLE_RELATIONS (source_id);"
        private const val CREATE_INDEX_RELATIONS_TARGET = "CREATE INDEX IF NOT EXISTS idx_dcr_tgt ON $TABLE_RELATIONS (target_id);"
    }

    override fun initialize(): Future<Void> {
        val promise = Promise.promise<Void>()
        start(Vertx.currentContext().owner(), promise)
        return promise.future()
    }

    override fun init(connection: Connection): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            if (!schema.isNullOrBlank()) {
                connection.createStatement().use { stmt ->
                    stmt.execute("CREATE SCHEMA IF NOT EXISTS \"$schema\"")
                    stmt.execute("SET search_path TO \"$schema\", public")
                }
            }

            connection.createStatement().use { stmt ->
                stmt.execute(CREATE_TYPES_TABLE)
                stmt.execute(CREATE_INSTANCES_TABLE)
                stmt.execute(CREATE_RELATIONS_TABLE)
                stmt.execute(CREATE_INDEX_TYPES_NAMESPACE)
                stmt.execute(CREATE_INDEX_INSTANCES_TYPE)
                stmt.execute(CREATE_INDEX_RELATIONS_SOURCE)
                stmt.execute(CREATE_INDEX_RELATIONS_TARGET)
            }
            logger.fine("DataCatalogStorePostgres initialized successfully")
            promise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to initialize DataCatalogStorePostgres: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    private fun mapResultSetToType(rs: ResultSet): DataCatalogType {
        val structureString = rs.getString("structure")
        return DataCatalogType(
            id = rs.getString("id"),
            namespace = rs.getString("namespace"),
            name = rs.getString("name"),
            description = rs.getString("description"),
            structure = if (structureString != null) JsonObject(structureString) else JsonObject(),
            topicPattern = rs.getString("topic_pattern"),
            createdAt = rs.getTimestamp("created_at")?.toInstant(),
            updatedAt = rs.getTimestamp("updated_at")?.toInstant()
        )
    }

    private fun mapResultSetToInstance(rs: ResultSet): DataCatalogInstance {
        val propertiesString = rs.getString("properties")
        return DataCatalogInstance(
            id = rs.getString("id"),
            typeId = rs.getString("type_id"),
            name = rs.getString("name"),
            baseTopic = rs.getString("base_topic"),
            properties = if (propertiesString != null) JsonObject(propertiesString) else JsonObject(),
            createdAt = rs.getTimestamp("created_at")?.toInstant(),
            updatedAt = rs.getTimestamp("updated_at")?.toInstant()
        )
    }

    private fun mapResultSetToRelation(rs: ResultSet): DataCatalogRelation {
        return DataCatalogRelation(
            sourceId = rs.getString("source_id"),
            targetId = rs.getString("target_id"),
            relationType = rs.getString("relation_type")
        )
    }

    override fun getTypes(namespace: String?): Future<List<DataCatalogType>> {
        val promise = Promise.promise<List<DataCatalogType>>()
        try {
            val sql = if (namespace != null) "SELECT * FROM $TABLE_TYPES WHERE namespace = ?" else "SELECT * FROM $TABLE_TYPES"
            connection!!.prepareStatement(sql).use { stmt ->
                if (namespace != null) {
                    stmt.setString(1, namespace)
                }
                stmt.executeQuery().use { rs ->
                    val list = mutableListOf<DataCatalogType>()
                    while (rs.next()) {
                        list.add(mapResultSetToType(rs))
                    }
                    promise.complete(list)
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to get types: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getType(id: String): Future<DataCatalogType?> {
        val promise = Promise.promise<DataCatalogType?>()
        try {
            connection!!.prepareStatement("SELECT * FROM $TABLE_TYPES WHERE id = ?").use { stmt ->
                stmt.setString(1, id)
                stmt.executeQuery().use { rs ->
                    if (rs.next()) {
                        promise.complete(mapResultSetToType(rs))
                    } else {
                        promise.complete(null)
                    }
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to get type: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun saveType(type: DataCatalogType): Future<DataCatalogType> {
        val promise = Promise.promise<DataCatalogType>()
        val sql = """
            INSERT INTO $TABLE_TYPES (id, namespace, name, description, structure, topic_pattern, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?::jsonb, ?, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET
                namespace = EXCLUDED.namespace,
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                structure = EXCLUDED.structure,
                topic_pattern = EXCLUDED.topic_pattern,
                updated_at = NOW()
        """
        try {
            connection!!.prepareStatement(sql).use { stmt ->
                stmt.setString(1, type.id)
                stmt.setString(2, type.namespace)
                stmt.setString(3, type.name)
                stmt.setString(4, type.description)
                stmt.setString(5, type.structure.encode())
                stmt.setString(6, type.topicPattern)

                stmt.executeUpdate()
                
                getType(type.id).onSuccess { t -> promise.complete(t) }.onFailure { promise.fail(it) }
            }
        } catch (e: Exception) {
            logger.severe("Failed to save type: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun deleteType(id: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        try {
            connection!!.prepareStatement("""
                DELETE FROM $TABLE_RELATIONS
                WHERE source_id = ? OR target_id = ?
                   OR source_id IN (SELECT id FROM $TABLE_INSTANCES WHERE type_id = ?)
                   OR target_id IN (SELECT id FROM $TABLE_INSTANCES WHERE type_id = ?)
            """).use { stmt ->
                repeat(4) { stmt.setString(it + 1, id) }
                stmt.executeUpdate()
            }
            connection!!.prepareStatement("DELETE FROM $TABLE_TYPES WHERE id = ?").use { stmt ->
                stmt.setString(1, id)
                val rowsAffected = stmt.executeUpdate()
                promise.complete(rowsAffected > 0)
            }
        } catch (e: Exception) {
            logger.severe("Failed to delete type: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getInstances(typeId: String?): Future<List<DataCatalogInstance>> {
        val promise = Promise.promise<List<DataCatalogInstance>>()
        try {
            val sql = if (typeId != null) "SELECT * FROM $TABLE_INSTANCES WHERE type_id = ?" else "SELECT * FROM $TABLE_INSTANCES"
            connection!!.prepareStatement(sql).use { stmt ->
                if (typeId != null) {
                    stmt.setString(1, typeId)
                }
                stmt.executeQuery().use { rs ->
                    val list = mutableListOf<DataCatalogInstance>()
                    while (rs.next()) {
                        list.add(mapResultSetToInstance(rs))
                    }
                    promise.complete(list)
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to get instances: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getInstance(id: String): Future<DataCatalogInstance?> {
        val promise = Promise.promise<DataCatalogInstance?>()
        try {
            connection!!.prepareStatement("SELECT * FROM $TABLE_INSTANCES WHERE id = ?").use { stmt ->
                stmt.setString(1, id)
                stmt.executeQuery().use { rs ->
                    if (rs.next()) {
                        promise.complete(mapResultSetToInstance(rs))
                    } else {
                        promise.complete(null)
                    }
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to get instance: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun saveInstance(instance: DataCatalogInstance): Future<DataCatalogInstance> {
        val promise = Promise.promise<DataCatalogInstance>()
        val sql = """
            INSERT INTO $TABLE_INSTANCES (id, type_id, name, base_topic, properties, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?::jsonb, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET
                type_id = EXCLUDED.type_id,
                name = EXCLUDED.name,
                base_topic = EXCLUDED.base_topic,
                properties = EXCLUDED.properties,
                updated_at = NOW()
        """
        try {
            connection!!.prepareStatement(sql).use { stmt ->
                stmt.setString(1, instance.id)
                stmt.setString(2, instance.typeId)
                stmt.setString(3, instance.name)
                stmt.setString(4, instance.baseTopic)
                stmt.setString(5, instance.properties.encode())

                stmt.executeUpdate()

                getInstance(instance.id).onSuccess { i -> promise.complete(i) }.onFailure { promise.fail(it) }
            }
        } catch (e: Exception) {
            logger.severe("Failed to save instance: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun deleteInstance(id: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        try {
            connection!!.prepareStatement(
                "DELETE FROM $TABLE_RELATIONS WHERE source_id = ? OR target_id = ?"
            ).use { stmt ->
                stmt.setString(1, id)
                stmt.setString(2, id)
                stmt.executeUpdate()
            }
            connection!!.prepareStatement("DELETE FROM $TABLE_INSTANCES WHERE id = ?").use { stmt ->
                stmt.setString(1, id)
                val rowsAffected = stmt.executeUpdate()
                promise.complete(rowsAffected > 0)
            }
        } catch (e: Exception) {
            logger.severe("Failed to delete instance: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getRelations(sourceId: String?, targetId: String?, relationType: String?): Future<List<DataCatalogRelation>> {
        val promise = Promise.promise<List<DataCatalogRelation>>()
        try {
            var sql = "SELECT * FROM $TABLE_RELATIONS WHERE 1=1"
            val params = mutableListOf<String>()
            
            if (sourceId != null) { sql += " AND source_id = ?"; params.add(sourceId) }
            if (targetId != null) { sql += " AND target_id = ?"; params.add(targetId) }
            if (relationType != null) { sql += " AND relation_type = ?"; params.add(relationType) }
            
            connection!!.prepareStatement(sql).use { stmt ->
                params.forEachIndexed { index, param ->
                    stmt.setString(index + 1, param)
                }
                stmt.executeQuery().use { rs ->
                    val list = mutableListOf<DataCatalogRelation>()
                    while (rs.next()) {
                        list.add(mapResultSetToRelation(rs))
                    }
                    promise.complete(list)
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to get relations: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun saveRelation(relation: DataCatalogRelation): Future<DataCatalogRelation> {
        val promise = Promise.promise<DataCatalogRelation>()
        val sql = """
            INSERT INTO $TABLE_RELATIONS (source_id, target_id, relation_type)
            VALUES (?, ?, ?)
            ON CONFLICT (source_id, target_id, relation_type) DO NOTHING
        """
        try {
            connection!!.prepareStatement(sql).use { stmt ->
                stmt.setString(1, relation.sourceId)
                stmt.setString(2, relation.targetId)
                stmt.setString(3, relation.relationType)
                stmt.executeUpdate()
                promise.complete(relation)
            }
        } catch (e: Exception) {
            logger.severe("Failed to save relation: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun deleteRelation(sourceId: String, targetId: String, relationType: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        try {
            val sql = "DELETE FROM $TABLE_RELATIONS WHERE source_id = ? AND target_id = ? AND relation_type = ?"
            connection!!.prepareStatement(sql).use { stmt ->
                stmt.setString(1, sourceId)
                stmt.setString(2, targetId)
                stmt.setString(3, relationType)
                val rowsAffected = stmt.executeUpdate()
                promise.complete(rowsAffected > 0)
            }
        } catch (e: Exception) {
            logger.severe("Failed to delete relation: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun close(): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            connection?.close()
            promise.complete()
        } catch (e: Exception) {
            logger.warning("Error closing database connection: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun exportCatalog(namespace: String?): Future<JsonObject> =
        at.rocworks.stores.DataCatalogTransfer.export(this, namespace)

    override fun importCatalog(data: JsonObject): Future<ImportDataCatalogResult> =
        at.rocworks.stores.DataCatalogTransfer.import(this, data)
}
