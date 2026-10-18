package at.rocworks

import at.rocworks.extensions.graphql.ArchiveGroupResolver
import at.rocworks.extensions.graphql.AuthorizationResult
import at.rocworks.extensions.graphql.GraphQLAuthContext
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.stores.IArchiveConfigStore
import at.rocworks.stores.MessageArchiveType
import at.rocworks.stores.MessageStoreType
import at.rocworks.stores.postgres.ArchiveConfigStorePostgres
import at.rocworks.stores.sqlite.ArchiveConfigStoreSQLite
import at.rocworks.stores.sqlite.SQLiteClient
import at.rocworks.stores.sqlite.SQLiteVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.ThreadingModel
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.concurrent.TimeUnit

class WinCCOAArchiveGroupTest {

    private lateinit var vertx: Vertx
    private val expectedMessage = "WINCCOA stores need MonsterMQ Edge embedded in the WinCC OA manager (WCCOAmmq) with WinCCOaNative.Namespace enabled"

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
    }

    @After
    fun tearDown() {
        await(vertx.close())
    }

    private fun <T> await(future: Future<T>, timeoutSeconds: Long = 10): T {
        return future.toCompletionStage().toCompletableFuture().get(timeoutSeconds, TimeUnit.SECONDS)
    }

    @Test
    fun testGraphQLCreateRejectedWithWinCCOA() {
        val authContext = Mockito.mock(GraphQLAuthContext::class.java) { inv: InvocationOnMock ->
            if (inv.method.name == "validateFieldAccess") AuthorizationResult.allowed()
            else Mockito.RETURNS_DEFAULTS.answer(inv)
        }
        val archiveHandler = Mockito.mock(ArchiveHandler::class.java)
        val configStore = Mockito.mock(IArchiveConfigStore::class.java)
        Mockito.`when`(archiveHandler.getConfigStore()).thenReturn(configStore)

        val resolver = ArchiveGroupResolver(vertx, archiveHandler, authContext)
        val createFetcher = resolver.createArchiveGroup()

        // 1. Reject WINCCOA lastValType
        val envLastVal = Mockito.mock(graphql.schema.DataFetchingEnvironment::class.java)
        Mockito.`when`(envLastVal.getArgument<Map<String, Any?>>("input")).thenReturn(
            mapOf(
                "name" to "WcGroupLastVal",
                "topicFilter" to listOf("winccoa/#"),
                "lastValType" to "WINCCOA",
                "archiveType" to "NONE"
            )
        )
        @Suppress("UNCHECKED_CAST")
        val resultLastVal = createFetcher.get(envLastVal).get(5, TimeUnit.SECONDS) as Map<String, Any?>
        assertEquals(false, resultLastVal["success"])
        assertEquals(expectedMessage, resultLastVal["message"])

        // 2. Reject WINCCOA archiveType
        val envArchive = Mockito.mock(graphql.schema.DataFetchingEnvironment::class.java)
        Mockito.`when`(envArchive.getArgument<Map<String, Any?>>("input")).thenReturn(
            mapOf(
                "name" to "WcGroupArchive",
                "topicFilter" to listOf("winccoa/#"),
                "lastValType" to "MEMORY",
                "archiveType" to "WINCCOA"
            )
        )
        @Suppress("UNCHECKED_CAST")
        val resultArchive = createFetcher.get(envArchive).get(5, TimeUnit.SECONDS) as Map<String, Any?>
        assertEquals(false, resultArchive["success"])
        assertEquals(expectedMessage, resultArchive["message"])
    }

    @Test
    fun testSQLiteConfigStoreLoadsWinCCOAAndStartupSkipsIt() {
        val tempDir: Path = Files.createTempDirectory("monstermq-winccoa-test")
        val dbPath = tempDir.resolve("monstermq.db").toString()

        try {
            await(vertx.deployVerticle(SQLiteVerticle()))
            val configStore = ArchiveConfigStoreSQLite(dbPath)
            await(vertx.deployVerticle(configStore))

            val client = SQLiteClient(vertx, dbPath)
            // Insert NormalGroup
            await(client.executeUpdate(
                """
                INSERT INTO archiveconfigs (name, enabled, topic_filter, retained_only, last_val_type, archive_type)
                VALUES (?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                JsonArray().add("NormalGroup").add(1).add("""["normal/#"]""").add(0).add("MEMORY").add("NONE")
            ))

            // Insert WinCCOAGroup
            await(client.executeUpdate(
                """
                INSERT INTO archiveconfigs (name, enabled, topic_filter, retained_only, last_val_type, archive_type)
                VALUES (?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                JsonArray().add("WinCCOAGroup").add(1).add("""["winccoa/#"]""").add(0).add("WINCCOA").add("WINCCOA")
            ))

            // Verify both groups load from SQLite config store
            val allGroups = await(configStore.getAllArchiveGroups())
            assertEquals(2, allGroups.size)
            val winccoaConfig = allGroups.find { it.archiveGroup.name == "WinCCOAGroup" }
            val normalConfig = allGroups.find { it.archiveGroup.name == "NormalGroup" }
            assertTrue(winccoaConfig != null)
            assertTrue(normalConfig != null)
            assertEquals(MessageStoreType.WINCCOA, winccoaConfig?.archiveGroup?.getLastValType())
            assertEquals(MessageArchiveType.WINCCOA, winccoaConfig?.archiveGroup?.getArchiveType())
            assertEquals(MessageStoreType.MEMORY, normalConfig?.archiveGroup?.getLastValType())

            // Start via ArchiveHandler
            val brokerConfig = JsonObject()
                .put("ConfigStoreType", "SQLITE")
                .put("SQLite", JsonObject().put("Path", tempDir.toString()))

            val archiveHandler = ArchiveHandler(vertx, brokerConfig, null, null)
            val deployedGroups = await(archiveHandler.initialize())

            // Verify NormalGroup deployed and WinCCOAGroup was skipped
            assertEquals(1, deployedGroups.size)
            assertEquals("NormalGroup", deployedGroups[0].name)
            val deployedMap = archiveHandler.getDeployedArchiveGroups()
            assertTrue("NormalGroup should be deployed", deployedMap.containsKey("NormalGroup"))
            assertFalse("WinCCOAGroup should not be deployed", deployedMap.containsKey("WinCCOAGroup"))
        } finally {
            tempDir.toFile().deleteRecursively()
        }
    }

    @Test
    fun testPostgresConfigStoreLoadsWinCCOAAndDeployFails() {
        val mockConnection = Mockito.mock(Connection::class.java)
        val mockStatement = Mockito.mock(PreparedStatement::class.java)
        val mockResultSet = Mockito.mock(ResultSet::class.java)

        Mockito.`when`(mockConnection.prepareStatement(Mockito.anyString())).thenReturn(mockStatement)
        Mockito.`when`(mockStatement.executeQuery()).thenReturn(mockResultSet)

        var rowCount = 0
        Mockito.`when`(mockResultSet.next()).thenAnswer {
            rowCount++
            rowCount <= 2
        }

        Mockito.`when`(mockResultSet.getString("name")).thenAnswer {
            if (rowCount == 1) "NormalGroup" else "WinCCOAGroup"
        }
        Mockito.`when`(mockResultSet.getBoolean("enabled")).thenReturn(true)
        Mockito.`when`(mockResultSet.getString("topic_filter")).thenAnswer {
            if (rowCount == 1) """["normal/#"]""" else """["winccoa/#"]"""
        }
        Mockito.`when`(mockResultSet.getBoolean("retained_only")).thenReturn(false)
        Mockito.`when`(mockResultSet.getString("last_val_type")).thenAnswer {
            if (rowCount == 1) "MEMORY" else "WINCCOA"
        }
        Mockito.`when`(mockResultSet.getString("archive_type")).thenAnswer {
            if (rowCount == 1) "NONE" else "WINCCOA"
        }
        Mockito.`when`(mockResultSet.getString("payload_format")).thenReturn("DEFAULT")
        Mockito.`when`(mockResultSet.getString("queue_type")).thenReturn("NONE")
        Mockito.`when`(mockResultSet.getInt("queue_size")).thenReturn(100000)
        Mockito.`when`(mockResultSet.getInt("bulk_size")).thenReturn(4000)
        Mockito.`when`(mockResultSet.getLong("bulk_timeout_ms")).thenReturn(250L)
        Mockito.`when`(mockResultSet.getString("queue_disk_path")).thenReturn("data/queue")
        Mockito.`when`(mockResultSet.getBoolean("last_val_read_only")).thenReturn(false)
        Mockito.`when`(mockResultSet.getBoolean("archive_read_only")).thenReturn(false)

        val postgresStore = ArchiveConfigStorePostgres("jdbc:postgresql://localhost/dummy", "user", "pass")
        postgresStore.init(vertx, vertx.orCreateContext)
        val dbField = ArchiveConfigStorePostgres::class.java.getDeclaredField("db").apply { isAccessible = true }
        val dbObj = dbField.get(postgresStore)
        val connField = dbObj.javaClass.superclass.getDeclaredField("connection").apply { isAccessible = true }
        connField.set(dbObj, mockConnection)

        val allGroups = await(postgresStore.getAllArchiveGroups())
        assertEquals(2, allGroups.size)
        val normalGroup = allGroups.find { it.archiveGroup.name == "NormalGroup" }?.archiveGroup
        val winccoaGroup = allGroups.find { it.archiveGroup.name == "WinCCOAGroup" }?.archiveGroup
        assertTrue(normalGroup != null)
        assertTrue(winccoaGroup != null)
        assertEquals(MessageStoreType.WINCCOA, winccoaGroup?.getLastValType())
        assertEquals(MessageArchiveType.WINCCOA, winccoaGroup?.getArchiveType())

        // Test deployment: NormalGroup succeeds, WinCCOAGroup fails
        val options = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
        val normalDeploymentId = await(vertx.deployVerticle(normalGroup, options))
        assertTrue(normalDeploymentId.isNotEmpty())

        var winccoaFailed = false
        try {
            await(vertx.deployVerticle(winccoaGroup, options))
        } catch (e: Exception) {
            winccoaFailed = true
        }
        assertTrue("Deploying WINCCOA group should fail", winccoaFailed)
    }
}
