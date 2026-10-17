package at.rocworks

import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IMessageArchiveExtended
import at.rocworks.stores.PayloadFormat
import at.rocworks.stores.cratedb.MessageArchiveCrateDB
import at.rocworks.stores.mongodb.MessageArchiveMongoDB
import at.rocworks.stores.postgres.MessageArchivePostgres
import at.rocworks.stores.sqlite.MessageArchiveSQLite
import at.rocworks.stores.sqlite.SQLiteVerticle
import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeTrue
import org.junit.Before
import org.junit.Test
import java.io.File
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Aggregation with field "" counts a payload only if it is a plain JSON number (monster-mq#207).
 * A JSON object such as a WinCC OA tag {"time":..,"value":..}, a string, a boolean or plain text
 * is NULL: it moves neither AVG/MIN/MAX nor COUNT, and no backend fails on it. The same payloads
 * and expectations are in monster-mq-edge test/integration/archive_aggregate_test.go.
 *
 * Postgres, MongoDB and CrateDB run when their environment variables are set:
 * MONSTERMQ_TEST_POSTGRES_URL (jdbc:postgresql://..) with MONSTERMQ_TEST_POSTGRES_USER and
 * MONSTERMQ_TEST_POSTGRES_PASSWORD, MONSTERMQ_TEST_MONGODB_URL (mongodb://..) with optional
 * MONSTERMQ_TEST_MONGODB_DB, and MONSTERMQ_TEST_CRATEDB_URL (jdbc:postgresql://..:5432/doc).
 */
class MessageArchiveAggregationTest {

    private lateinit var vertx: Vertx

    private val payloads = listOf(
        "agg/mixed" to "1",
        "agg/mixed" to "3",
        "agg/mixed" to """{"time":"2026-10-17T10:00:00.000Z","value":42.5}""",
        "agg/other" to " 5 ",
        "agg/other" to "-2.5e1",
        "agg/other" to "\"7\"",
        "agg/other" to "true",
        "agg/other" to "[1]",
        "agg/other" to "abc",
        "agg/other" to "007",
        "agg/other" to "+5",
        "agg/other" to "NaN"
    )

    private val rawWant = mapOf(
        "agg/mixed_avg" to 2.0, "agg/mixed_min" to 1.0, "agg/mixed_max" to 3.0, "agg/mixed_count" to 2.0,
        "agg/other_avg" to -10.0, "agg/other_min" to -25.0, "agg/other_max" to 5.0, "agg/other_count" to 2.0
    )

    private val day = Instant.now().truncatedTo(ChronoUnit.DAYS)

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
    }

    @After
    fun tearDown() {
        val latch = CountDownLatch(1)
        vertx.close().onComplete { latch.countDown() }
        latch.await(10, TimeUnit.SECONDS)
    }

    @Test
    fun sqliteRawFieldSkipsNonNumbers() {
        val dbFile = File.createTempFile("monstermq-agg-", ".db")
        dbFile.deleteOnExit()
        deploy(SQLiteVerticle(), "sqlite verticle")
        for (format in PayloadFormat.entries) {
            val archive = MessageArchiveSQLite(uniqueName(format), dbFile.absolutePath, format)
            deploy(archive, "sqlite archive")
            check(archive, format)
        }
    }

    @Test
    fun postgresRawFieldSkipsNonNumbers() {
        val url = System.getenv("MONSTERMQ_TEST_POSTGRES_URL")
        assumeTrue("MONSTERMQ_TEST_POSTGRES_URL not set", !url.isNullOrBlank())
        val user = System.getenv("MONSTERMQ_TEST_POSTGRES_USER") ?: ""
        val password = System.getenv("MONSTERMQ_TEST_POSTGRES_PASSWORD") ?: ""
        for (format in PayloadFormat.entries) {
            val archive = MessageArchivePostgres(uniqueName(format), url!!, user, password, format)
            deploy(archive, "postgres archive")
            withTable(archive) { check(archive, format) }
        }
    }

    @Test
    fun mongodbRawFieldSkipsNonNumbers() {
        val url = System.getenv("MONSTERMQ_TEST_MONGODB_URL")
        assumeTrue("MONSTERMQ_TEST_MONGODB_URL not set", !url.isNullOrBlank())
        val db = System.getenv("MONSTERMQ_TEST_MONGODB_DB") ?: "monstermq_test"
        for (format in PayloadFormat.entries) {
            val archive = MessageArchiveMongoDB(uniqueName(format), url!!, db, format)
            deploy(archive, "mongodb archive")
            withTable(archive) { check(archive, format) }
        }
    }

    @Test
    fun cratedbRawFieldSkipsNonNumbers() {
        val url = System.getenv("MONSTERMQ_TEST_CRATEDB_URL")
        assumeTrue("MONSTERMQ_TEST_CRATEDB_URL not set", !url.isNullOrBlank())
        for (format in PayloadFormat.entries) {
            val archive = MessageArchiveCrateDB(uniqueName(format), url!!, "crate", "", format)
            deploy(archive, "cratedb archive")
            withTable(archive) { check(archive, format) }
        }
    }

    private fun uniqueName(format: PayloadFormat) = "AggTest${format.name}${System.nanoTime()}Archive"

    private fun withTable(archive: IMessageArchiveExtended, block: () -> Unit) {
        assertTrue("createTable failed for ${archive.getName()}", runBlocking { archive.createTable() })
        try {
            block()
        } finally {
            archive.dropStorage()
        }
    }

    // Archives the payloads one second apart in one ONE_DAY bucket, then checks field "" and,
    // for the JSON format, field "value" (which only reads JSON objects).
    private fun check(archive: IMessageArchiveExtended, format: PayloadFormat) {
        val base = day.plus(12, ChronoUnit.HOURS)
        archive.addHistory(payloads.mapIndexed { i, (topic, payload) ->
            BrokerMessage(
                topicName = topic,
                payload = payload.toByteArray(),
                qosLevel = 1,
                isRetain = false,
                clientId = "agg-test",
                time = base.plusSeconds(i.toLong())
            )
        })
        val functions = listOf("AVG", "MIN", "MAX", "COUNT")
        expect(archive, format, rawWant) {
            archive.getAggregatedHistory(listOf("agg/mixed", "agg/other"), day, day.plus(1, ChronoUnit.DAYS), 1440, functions, emptyList())
        }
        if (format == PayloadFormat.JSON) {
            expect(archive, format, mapOf("agg/mixed.value_avg" to 42.5, "agg/mixed.value_count" to 1.0)) {
                archive.getAggregatedHistory(listOf("agg/mixed"), day, day.plus(1, ChronoUnit.DAYS), 1440, functions, listOf("value"))
            }
        }
    }

    // Polls until the single bucket holds the wanted values, so a CrateDB refresh does not fail the test.
    private fun expect(
        archive: IMessageArchiveExtended,
        format: PayloadFormat,
        want: Map<String, Double>,
        query: () -> io.vertx.core.json.JsonObject
    ) {
        val deadline = System.currentTimeMillis() + 10_000
        while (true) {
            val result = query()
            val columns = result.getJsonArray("columns")
            val rows = result.getJsonArray("rows")
            assertTrue("${archive.getName()}: expected one ONE_DAY bucket, got $rows", rows.size() <= 1)
            val row = mutableMapOf<String, Any?>()
            if (rows.size() == 1) {
                val values = rows.getJsonArray(0)
                for (i in 0 until columns.size()) row[columns.getString(i)] = values.getValue(i)
            }
            val diffs = want.mapNotNull { (col, w) ->
                val v = (row[col] as? Number)?.toDouble()
                if (v == w) null else "$col = ${row[col]}, want $w"
            }
            if (diffs.isEmpty()) return
            if (System.currentTimeMillis() > deadline) {
                throw AssertionError("${archive.getName()} ($format): ${diffs.joinToString("; ")} (row $row)")
            }
            Thread.sleep(200)
        }
    }

    private fun deploy(verticle: AbstractVerticle, label: String) {
        val latch = CountDownLatch(1)
        val errorRef = AtomicReference<Throwable?>()
        vertx.deployVerticle(verticle).onComplete { result ->
            if (result.failed()) errorRef.set(result.cause())
            latch.countDown()
        }
        assertTrue("Timed out deploying $label", latch.await(30, TimeUnit.SECONDS))
        errorRef.get()?.let { throw AssertionError("Failed to deploy $label", it) }
    }
}
