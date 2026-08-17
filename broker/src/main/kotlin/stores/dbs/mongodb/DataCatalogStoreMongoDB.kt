package at.rocworks.stores.mongodb

import at.rocworks.Utils
import at.rocworks.stores.DataCatalogInstance
import at.rocworks.stores.DataCatalogRelation
import at.rocworks.stores.DataCatalogType
import at.rocworks.stores.IDataCatalogStore
import at.rocworks.stores.ImportDataCatalogResult
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.IndexOptions
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.bson.Document
import java.time.Instant
import java.util.Date
import java.util.logging.Logger

class DataCatalogStoreMongoDB(
    private val connectionString: String,
    private val databaseName: String = "datacatalog"
) : IDataCatalogStore {

    private val logger: Logger = Utils.getLogger(DataCatalogStoreMongoDB::class.java)
    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase

    private lateinit var typesCollection: MongoCollection<Document>
    private lateinit var instancesCollection: MongoCollection<Document>
    private lateinit var relationsCollection: MongoCollection<Document>

    companion object {
        private const val COLL_TYPES = "types"
        private const val COLL_INSTANCES = "instances"
        private const val COLL_RELATIONS = "relations"
    }

    override fun initialize(): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            mongoClient = MongoClientPool.getClient(connectionString)
            database = mongoClient.getDatabase(databaseName)

            typesCollection = database.getCollection(COLL_TYPES)
            instancesCollection = database.getCollection(COLL_INSTANCES)
            relationsCollection = database.getCollection(COLL_RELATIONS)

            typesCollection.createIndex(Document("id", 1), IndexOptions().unique(true))
            typesCollection.createIndex(Document("namespace", 1))

            instancesCollection.createIndex(Document("id", 1), IndexOptions().unique(true))
            instancesCollection.createIndex(Document("type_id", 1))

            relationsCollection.createIndex(Document("source_id", 1))
            relationsCollection.createIndex(Document("target_id", 1))
            relationsCollection.createIndex(
                Document("source_id", 1).append("target_id", 1).append("relation_type", 1),
                IndexOptions().unique(true)
            )

            promise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to initialize DataCatalogStoreMongoDB: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getTypes(namespace: String?): Future<List<DataCatalogType>> {
        val promise = Promise.promise<List<DataCatalogType>>()
        try {
            val query = if (namespace != null) eq("namespace", namespace) else Document()
            val docs = typesCollection.find(query).toList()
            val types = docs.map { mapDocumentToType(it) }
            promise.complete(types)
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getType(id: String): Future<DataCatalogType?> {
        val promise = Promise.promise<DataCatalogType?>()
        try {
            val doc = typesCollection.find(eq("id", id)).first()
            if (doc != null) {
                promise.complete(mapDocumentToType(doc))
            } else {
                promise.complete(null)
            }
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun saveType(type: DataCatalogType): Future<DataCatalogType> {
        val promise = Promise.promise<DataCatalogType>()
        try {
            val now = Instant.now()
            val t = if (type.createdAt == null) type.copy(createdAt = now, updatedAt = now) else type.copy(updatedAt = now)
            val doc = mapTypeToDocument(t)

            typesCollection.updateOne(
                eq("id", t.id),
                Document("\$set", doc),
                UpdateOptions().upsert(true)
            )
            promise.complete(t)
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun deleteType(id: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        try {
            val res = typesCollection.deleteOne(eq("id", id))
            promise.complete(res.deletedCount > 0)
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getInstances(typeId: String?): Future<List<DataCatalogInstance>> {
        val promise = Promise.promise<List<DataCatalogInstance>>()
        try {
            val query = if (typeId != null) eq("type_id", typeId) else Document()
            val docs = instancesCollection.find(query).toList()
            val instances = docs.map { mapDocumentToInstance(it) }
            promise.complete(instances)
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getInstance(id: String): Future<DataCatalogInstance?> {
        val promise = Promise.promise<DataCatalogInstance?>()
        try {
            val doc = instancesCollection.find(eq("id", id)).first()
            if (doc != null) {
                promise.complete(mapDocumentToInstance(doc))
            } else {
                promise.complete(null)
            }
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun saveInstance(instance: DataCatalogInstance): Future<DataCatalogInstance> {
        val promise = Promise.promise<DataCatalogInstance>()
        try {
            val now = Instant.now()
            val i = if (instance.createdAt == null) instance.copy(createdAt = now, updatedAt = now) else instance.copy(updatedAt = now)
            val doc = mapInstanceToDocument(i)

            instancesCollection.updateOne(
                eq("id", i.id),
                Document("\$set", doc),
                UpdateOptions().upsert(true)
            )
            promise.complete(i)
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun deleteInstance(id: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        try {
            val res = instancesCollection.deleteOne(eq("id", id))
            promise.complete(res.deletedCount > 0)
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getRelations(sourceId: String?, targetId: String?, relationType: String?): Future<List<DataCatalogRelation>> {
        val promise = Promise.promise<List<DataCatalogRelation>>()
        try {
            val filters = mutableListOf<org.bson.conversions.Bson>()
            if (sourceId != null) filters.add(eq("source_id", sourceId))
            if (targetId != null) filters.add(eq("target_id", targetId))
            if (relationType != null) filters.add(eq("relation_type", relationType))

            val query = if (filters.isEmpty()) Document() else and(filters)
            val docs = relationsCollection.find(query).toList()
            val relations = docs.map { mapDocumentToRelation(it) }
            promise.complete(relations)
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun saveRelation(relation: DataCatalogRelation): Future<DataCatalogRelation> {
        val promise = Promise.promise<DataCatalogRelation>()
        try {
            val doc = mapRelationToDocument(relation)
            val query = and(
                eq("source_id", relation.sourceId),
                eq("target_id", relation.targetId),
                eq("relation_type", relation.relationType)
            )
            relationsCollection.updateOne(
                query,
                Document("\$set", doc),
                UpdateOptions().upsert(true)
            )
            promise.complete(relation)
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun deleteRelation(sourceId: String, targetId: String, relationType: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        try {
            val query = and(
                eq("source_id", sourceId),
                eq("target_id", targetId),
                eq("relation_type", relationType)
            )
            val res = relationsCollection.deleteOne(query)
            promise.complete(res.deletedCount > 0)
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun close(): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            MongoClientPool.releaseClient(connectionString)
            promise.complete()
        } catch (e: Exception) {
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
        
        val types = data.getJsonArray("types", JsonArray()) ?: JsonArray()
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
            val instances = data.getJsonArray("instances", JsonArray()) ?: JsonArray()
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
            val relations = data.getJsonArray("relations", JsonArray()) ?: JsonArray()
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

    private fun mapTypeToDocument(type: DataCatalogType): Document {
        return Document().apply {
            put("id", type.id)
            put("namespace", type.namespace)
            put("name", type.name)
            put("description", type.description)
            put("structure", Document.parse(type.structure.encode()))
            put("topic_pattern", type.topicPattern)
            put("created_at", if (type.createdAt != null) Date.from(type.createdAt) else Date())
            put("updated_at", if (type.updatedAt != null) Date.from(type.updatedAt) else Date())
        }
    }

    private fun mapDocumentToType(doc: Document): DataCatalogType {
        val structureDoc = doc.get("structure", Document::class.java)
        return DataCatalogType(
            id = doc.getString("id") ?: "",
            namespace = doc.getString("namespace") ?: "",
            name = doc.getString("name") ?: "",
            description = doc.getString("description"),
            structure = if (structureDoc != null) JsonObject(structureDoc.toJson()) else JsonObject(),
            topicPattern = doc.getString("topic_pattern"),
            createdAt = doc.getDate("created_at")?.toInstant(),
            updatedAt = doc.getDate("updated_at")?.toInstant()
        )
    }

    private fun mapInstanceToDocument(instance: DataCatalogInstance): Document {
        return Document().apply {
            put("id", instance.id)
            put("type_id", instance.typeId)
            put("name", instance.name)
            put("base_topic", instance.baseTopic)
            put("properties", Document.parse(instance.properties.encode()))
            put("created_at", if (instance.createdAt != null) Date.from(instance.createdAt) else Date())
            put("updated_at", if (instance.updatedAt != null) Date.from(instance.updatedAt) else Date())
        }
    }

    private fun mapDocumentToInstance(doc: Document): DataCatalogInstance {
        val propsDoc = doc.get("properties", Document::class.java)
        return DataCatalogInstance(
            id = doc.getString("id") ?: "",
            typeId = doc.getString("type_id") ?: "",
            name = doc.getString("name") ?: "",
            baseTopic = doc.getString("base_topic") ?: "",
            properties = if (propsDoc != null) JsonObject(propsDoc.toJson()) else JsonObject(),
            createdAt = doc.getDate("created_at")?.toInstant(),
            updatedAt = doc.getDate("updated_at")?.toInstant()
        )
    }

    private fun mapRelationToDocument(relation: DataCatalogRelation): Document {
        return Document().apply {
            put("source_id", relation.sourceId)
            put("target_id", relation.targetId)
            put("relation_type", relation.relationType)
        }
    }

    private fun mapDocumentToRelation(doc: Document): DataCatalogRelation {
        return DataCatalogRelation(
            sourceId = doc.getString("source_id") ?: "",
            targetId = doc.getString("target_id") ?: "",
            relationType = doc.getString("relation_type") ?: ""
        )
    }
}
