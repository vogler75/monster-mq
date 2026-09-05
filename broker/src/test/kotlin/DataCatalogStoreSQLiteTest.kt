package at.rocworks

import at.rocworks.stores.DataCatalogInstance
import at.rocworks.stores.DataCatalogRelation
import at.rocworks.stores.DataCatalogType
import at.rocworks.stores.sqlite.DataCatalogStoreSQLite
import at.rocworks.stores.sqlite.SQLiteVerticle
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.nio.file.Files
import java.util.concurrent.TimeUnit

class DataCatalogStoreSQLiteTest {
    @Test
    fun deletingTypeRemovesInstancesAndEveryAttachedRelation() {
        val vertx = Vertx.vertx()
        val database = Files.createTempFile("monstermq-datacatalog-", ".db")
        try {
            await(vertx.deployVerticle(SQLiteVerticle()))
            val store = DataCatalogStoreSQLite(vertx, database.toString())
            await(store.initialize())
            await(store.saveType(type("line")))
            await(store.saveType(type("pump")))
            await(store.saveInstance(instance("line-1", "line")))
            await(store.saveInstance(instance("pump-1", "pump")))
            await(store.saveRelation(DataCatalogRelation("line-1", "pump-1", "HasComponent")))
            await(store.saveRelation(DataCatalogRelation("pump", "pump-1", "HasInstance")))

            assertTrue(await(store.deleteType("pump")))
            assertNull(await(store.getInstance("pump-1")))
            assertEquals(emptyList<DataCatalogRelation>(), await(store.getRelations()))
            assertFalse(await(store.deleteType("missing")))
        } finally {
            await(vertx.close())
            Files.deleteIfExists(database)
            Files.deleteIfExists(database.resolveSibling(database.fileName.toString() + "-wal"))
            Files.deleteIfExists(database.resolveSibling(database.fileName.toString() + "-shm"))
        }
    }

    private fun type(id: String) = DataCatalogType(id, "factory", id, null, JsonObject(), null)
    private fun instance(id: String, typeId: String) = DataCatalogInstance(id, typeId, id, "topics/$id", JsonObject())
    private fun <T> await(future: Future<T>): T = future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)
}
