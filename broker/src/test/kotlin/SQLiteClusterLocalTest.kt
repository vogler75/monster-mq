package at.rocworks

import at.rocworks.stores.sqlite.SQLiteClient
import at.rocworks.stores.sqlite.SQLiteVerticle
import com.hazelcast.config.Config
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.eventbus.EventBusOptions
import io.vertx.core.json.JsonArray
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import org.junit.Assert.assertEquals
import org.junit.Test
import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * In -cluster mode every node deploys its own SQLiteVerticle on the same addresses. Point-to-point
 * requests on a clustered event bus round-robin over all nodes that registered the address, so a
 * request from SQLiteClient must stay on the node that sent it (its own db file, and no JSON wire
 * codec that turns byte[] params into base64 strings).
 */
class SQLiteClusterLocalTest {

    private val sqliteAddresses = setOf(
        SQLiteVerticle.EB_INIT_DB,
        SQLiteVerticle.EB_EXECUTE_UPDATE,
        SQLiteVerticle.EB_EXECUTE_QUERY,
        SQLiteVerticle.EB_EXECUTE_BATCH
    )

    private fun <T> await(future: Future<T>): T =
        future.toCompletionStage().toCompletableFuture().get(60, TimeUnit.SECONDS)

    private fun clusteredVertx(clusterName: String, port: Int): Vertx {
        val config = Config()
        config.clusterName = clusterName
        config.networkConfig.port = port
        config.networkConfig.isPortAutoIncrement = false
        config.networkConfig.join.multicastConfig.isEnabled = false
        config.networkConfig.join.tcpIpConfig.isEnabled = true
        config.networkConfig.join.tcpIpConfig.addMember("127.0.0.1:${port - 1}")
        config.networkConfig.join.tcpIpConfig.addMember("127.0.0.1:${port + 1}")
        config.networkConfig.interfaces.isEnabled = true
        config.networkConfig.interfaces.addInterface("127.0.0.1")
        config.setProperty("hazelcast.phone.home.enabled", "false")
        return await(
            Vertx.builder()
                .with(io.vertx.core.VertxOptions().setEventBusOptions(EventBusOptions().setHost("127.0.0.1")))
                .withClusterManager(HazelcastClusterManager(config))
                .buildClustered()
        )
    }

    @Test
    fun sqliteRequestsStayOnTheSendingNode() {
        val clusterName = "sqlite-local-test-" + UUID.randomUUID()
        val basePort = 15701 + (Math.random() * 1000).toInt() * 2
        val nodeA = clusteredVertx(clusterName, basePort)
        val nodeB = clusteredVertx(clusterName, basePort + 1)
        val dbFile = File.createTempFile("monstermq-cluster-", ".db")
        dbFile.deleteOnExit()
        try {
            await(nodeA.deployVerticle(SQLiteVerticle()))
            await(nodeB.deployVerticle(SQLiteVerticle()))

            // Wait until node A sees node B in the cluster.
            val deadline = System.currentTimeMillis() + 30_000
            while ((nodeA as io.vertx.core.internal.VertxInternal).clusterManager().nodes.size < 2) {
                if (System.currentTimeMillis() > deadline) throw AssertionError("node B did not join the cluster")
                Thread.sleep(100)
            }

            val handledOnB = AtomicInteger()
            nodeB.eventBus().addInboundInterceptor<Any> { ctx ->
                if (ctx.message().address() in sqliteAddresses) handledOnB.incrementAndGet()
                ctx.next()
            }

            val client = SQLiteClient(nodeA, dbFile.absolutePath)
            await(
                client.initDatabase(JsonArray().add("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v BLOB)"))
            )
            val requests = 20
            repeat(requests) { i ->
                await(client.executeUpdate("INSERT INTO t (id, v) VALUES (?, ?)", JsonArray().add(i).add(byteArrayOf(i.toByte()))))
            }
            val batch = JsonArray()
            repeat(requests) { i -> batch.add(JsonArray().add(1000 + i).add(byteArrayOf(i.toByte()))) }
            await(client.executeBatch("INSERT INTO t (id, v) VALUES (?, ?)", batch))
            val rows = await(client.executeQuery("SELECT COUNT(*) AS n FROM t"))

            assertEquals(2 * requests, rows.getJsonObject(0).getInteger("n"))
            assertEquals("SQLite requests from node A were delivered to node B", 0, handledOnB.get())
        } finally {
            runCatching { await(nodeA.close()) }
            runCatching { await(nodeB.close()) }
            dbFile.delete()
        }
    }
}
