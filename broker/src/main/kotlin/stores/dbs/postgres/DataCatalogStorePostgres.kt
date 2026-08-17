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
        private const val TABLE_TYPES = "data_catalog_types"
        private const val TABLE_INSTANCES = "data_catalog_instances"
        private const val TABLE_RELATIONS = "data_catalog_relations"

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

    override fun exportCatalog(namespace: String?): Future<JsonObject> {
        val promise = Promise.promise<JsonObject>()
        val result = JsonObject()
        
        getTypes(namespace).compose { types ->
            val typesArr = JsonArray()
            types.forEach { t -> 
                val typeJson = JsonObject()
                    .put("id", t.id)
                    .put("namespace", t.namespace)
                    .put("name", t.name)
                    .put("description", t.description)
                    .put("structure", t.structure)
                    .put("topicPattern", t.topicPattern)
                typesArr.add(typeJson)
            }
            result.put("types", typesArr)
            
            val instanceFutures = types.map { getInstances(it.id) }
            Future.all(instanceFutures)
        }.compose { instanceResults ->
            val instancesArr = JsonArray()
            instanceResults.list<List<DataCatalogInstance>>().flatten().forEach { i ->
                val instJson = JsonObject()
                    .put("id", i.id)
                    .put("typeId", i.typeId)
                    .put("name", i.name)
                    .put("baseTopic", i.baseTopic)
                    .put("properties", i.properties)
                instancesArr.add(instJson)
            }
            result.put("instances", instancesArr)
            
            val allIds = result.getJsonArray("types").map { (it as JsonObject).getString("id") } +
                         result.getJsonArray("instances").map { (it as JsonObject).getString("id") }
            val relFutures = allIds.map { getRelations(sourceId = it) }
            Future.all(relFutures)
        }.onSuccess { relResults ->
            val relationsArr = JsonArray()
            relResults.list<List<DataCatalogRelation>>().flatten().distinctBy { "${it.sourceId}-${it.targetId}-${it.relationType}" }.forEach { r ->
                val relJson = JsonObject()
                    .put("sourceId", r.sourceId)
                    .put("targetId", r.targetId)
                    .put("relationType", r.relationType)
                relationsArr.add(relJson)
            }
            result.put("relations", relationsArr)
            promise.complete(result)
        }.onFailure { promise.fail(it) }
        
        return promise.future()
    }

    override fun importCatalog(data: JsonObject): Future<ImportDataCatalogResult> {
        val promise = Promise.promise<ImportDataCatalogResult>()
        var typesCount = 0
        var instancesCount = 0
        var relationsCount = 0
        
        val types = data.getJsonArray("types", JsonArray())
        val typeFutures = types.map { t ->
            val typeObj = t as JsonObject
            saveType(DataCatalogType(
                id = typeObj.getString("id"),
                namespace = typeObj.getString("namespace"),
                name = typeObj.getString("name"),
                description = typeObj.getString("description"),
                structure = typeObj.getJsonObject("structure", JsonObject()),
                topicPattern = typeObj.getString("topicPattern")
            )).onSuccess { typesCount++ }
        }
        
        Future.all(typeFutures).compose {
            val instances = data.getJsonArray("instances", JsonArray())
            val instanceFutures = instances.map { i ->
                val instObj = i as JsonObject
                saveInstance(DataCatalogInstance(
                    id = instObj.getString("id"),
                    typeId = instObj.getString("typeId"),
                    name = instObj.getString("name"),
                    baseTopic = instObj.getString("baseTopic"),
                    properties = instObj.getJsonObject("properties", JsonObject())
                )).onSuccess { instancesCount++ }
            }
            Future.all(instanceFutures)
        }.compose {
            val relations = data.getJsonArray("relations", JsonArray())
            val relFutures = relations.map { r ->
                val relObj = r as JsonObject
                saveRelation(DataCatalogRelation(
                    sourceId = relObj.getString("sourceId"),
                    targetId = relObj.getString("targetId"),
                    relationType = relObj.getString("relationType")
                )).onSuccess { relationsCount++ }
            }
            Future.all(relFutures)
        }.onSuccess {
            promise.complete(ImportDataCatalogResult.success(typesCount, instancesCount, relationsCount))
        }.onFailure { error ->
            promise.complete(ImportDataCatalogResult.failure(listOf(error.message ?: "Unknown error")))
        }
        
        return promise.future()
    }
}
