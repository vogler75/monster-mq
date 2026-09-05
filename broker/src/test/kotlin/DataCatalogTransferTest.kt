package at.rocworks

import at.rocworks.stores.DataCatalogInstance
import at.rocworks.stores.DataCatalogRelation
import at.rocworks.stores.DataCatalogTransfer
import at.rocworks.stores.DataCatalogType
import at.rocworks.stores.IDataCatalogStore
import at.rocworks.stores.ImportDataCatalogResult
import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.TimeUnit

class DataCatalogTransferTest {
    @Test
    fun invalidReferencesAreRejectedBeforeAnyWrites() {
        val store = FakeCatalogStore()
        val document = JsonObject()
            .put("types", JsonArray())
            .put("instances", JsonArray().add(JsonObject()
                .put("id", "pump-1").put("typeId", "missing").put("name", "Pump 1")
                .put("baseTopic", "plant/pump-1").put("properties", JsonObject())))
            .put("relations", JsonArray())

        val result = await(DataCatalogTransfer.import(store, document))

        assertFalse(result.success)
        assertEquals(1, result.failed)
        assertTrue(result.errors.single().contains("Unknown type missing"))
        assertTrue(store.instances.isEmpty())
    }

    @Test
    fun importValidatesThenWritesTypesInstancesAndRelations() {
        val store = FakeCatalogStore()
        val document = JsonObject()
            .put("types", JsonArray().add(JsonObject()
                .put("id", "pump").put("namespace", "factory").put("name", "Pump")
                .put("structure", JsonObject().put("type", "object"))))
            .put("instances", JsonArray().add(JsonObject()
                .put("id", "pump-1").put("typeId", "pump").put("name", "Pump 1")
                .put("baseTopic", "factory/pump-1").put("properties", JsonObject())))
            .put("relations", JsonArray().add(JsonObject()
                .put("sourceId", "pump").put("targetId", "pump-1").put("relationType", "HasInstance")))

        val result = await(DataCatalogTransfer.import(store, document))

        assertTrue(result.success)
        assertEquals(1, result.typesImported)
        assertEquals(1, result.instancesImported)
        assertEquals(1, result.relationsImported)
    }

    @Test
    fun namespaceExportDoesNotLeakExternalRelations() {
        val store = FakeCatalogStore()
        store.types["pump"] = type("pump", "factory-a")
        store.types["valve"] = type("valve", "factory-b")
        store.instances["pump-1"] = instance("pump-1", "pump")
        store.instances["valve-1"] = instance("valve-1", "valve")
        store.relations += DataCatalogRelation("pump", "pump-1", "HasInstance")
        store.relations += DataCatalogRelation("pump-1", "valve-1", "ConnectedTo")

        val exported = await(DataCatalogTransfer.export(store, "factory-a"))

        assertEquals(1, exported.getJsonArray("types").size())
        assertEquals(1, exported.getJsonArray("instances").size())
        assertEquals(1, exported.getJsonArray("relations").size())
        assertEquals("HasInstance", exported.getJsonArray("relations").getJsonObject(0).getString("relationType"))
    }

    private fun type(id: String, namespace: String) = DataCatalogType(
        id, namespace, id, null, JsonObject().put("type", "object"), null
    )

    private fun instance(id: String, typeId: String) = DataCatalogInstance(
        id, typeId, id, "topics/$id", JsonObject()
    )

    private fun <T> await(future: Future<T>): T =
        future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS)

    private class FakeCatalogStore : IDataCatalogStore {
        val types = linkedMapOf<String, DataCatalogType>()
        val instances = linkedMapOf<String, DataCatalogInstance>()
        val relations = linkedSetOf<DataCatalogRelation>()

        override fun initialize(): Future<Void> = Future.succeededFuture()
        override fun getTypes(namespace: String?): Future<List<DataCatalogType>> = Future.succeededFuture(
            types.values.filter { namespace == null || it.namespace == namespace }
        )
        override fun getType(id: String): Future<DataCatalogType?> = Future.succeededFuture(types[id])
        override fun saveType(type: DataCatalogType): Future<DataCatalogType> {
            types[type.id] = type; return Future.succeededFuture(type)
        }
        override fun deleteType(id: String): Future<Boolean> = Future.succeededFuture(types.remove(id) != null)
        override fun getInstances(typeId: String?): Future<List<DataCatalogInstance>> = Future.succeededFuture(
            instances.values.filter { typeId == null || it.typeId == typeId }
        )
        override fun getInstance(id: String): Future<DataCatalogInstance?> = Future.succeededFuture(instances[id])
        override fun saveInstance(instance: DataCatalogInstance): Future<DataCatalogInstance> {
            instances[instance.id] = instance; return Future.succeededFuture(instance)
        }
        override fun deleteInstance(id: String): Future<Boolean> = Future.succeededFuture(instances.remove(id) != null)
        override fun getRelations(sourceId: String?, targetId: String?, relationType: String?): Future<List<DataCatalogRelation>> =
            Future.succeededFuture(relations.filter {
                (sourceId == null || it.sourceId == sourceId) &&
                    (targetId == null || it.targetId == targetId) &&
                    (relationType == null || it.relationType == relationType)
            })
        override fun saveRelation(relation: DataCatalogRelation): Future<DataCatalogRelation> {
            relations += relation; return Future.succeededFuture(relation)
        }
        override fun deleteRelation(sourceId: String, targetId: String, relationType: String): Future<Boolean> =
            Future.succeededFuture(relations.remove(DataCatalogRelation(sourceId, targetId, relationType)))
        override fun close(): Future<Void> = Future.succeededFuture()
        override fun exportCatalog(namespace: String?): Future<JsonObject> = DataCatalogTransfer.export(this, namespace)
        override fun importCatalog(data: JsonObject): Future<ImportDataCatalogResult> = DataCatalogTransfer.import(this, data)
    }
}
