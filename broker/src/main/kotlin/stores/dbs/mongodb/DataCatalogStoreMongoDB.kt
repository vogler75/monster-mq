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
            val instanceIds = instancesCollection.find(eq("type_id", id))
                .map { it.getString("id") }.toList()
            val endpointIds = instanceIds + id
            relationsCollection.deleteMany(or(`in`("source_id", endpointIds), `in`("target_id", endpointIds)))
            instancesCollection.deleteMany(eq("type_id", id))
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
            relationsCollection.deleteMany(or(eq("source_id", id), eq("target_id", id)))
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

    override fun exportCatalog(namespace: String?): Future<JsonObject> =
        at.rocworks.stores.DataCatalogTransfer.export(this, namespace)

    override fun importCatalog(data: JsonObject): Future<ImportDataCatalogResult> =
        at.rocworks.stores.DataCatalogTransfer.import(this, data)

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
