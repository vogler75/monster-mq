import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Timestamp
import java.time.Instant
import java.util.*
import kotlin.random.Random

/**
 * Standalone QuestDB test program for bulk inserts
 * Tests bulk insert performance into the monstermq1 table
 * Uses the EXACT same writeBulk function as PostgreSQLLogger.kt
 */
class QuestDBBulkTest(
    private val host: String = "localhost",
    private val port: Int = 8812,
    private val database: String = "qdb",
    private val numBatches: Int = 10
) {
    
    companion object {
        // Configuration constants
        const val BATCH_SIZE = 1000
        const val USERNAME = "admin"
        const val PASSWORD = "quest"
        const val TABLE_NAME = "monstermq1"
        const val BULK_DELAY_MS = 10  // Delay between bulk writes in milliseconds
    }

    /**
     * Buffered row ready for database write - EXACT copy from JDBCLoggerBase.kt
     */
    data class BufferedRow(
        val tableName: String,
        val fields: Map<String, Any?>,
        val timestamp: Instant,
        val topic: String
    )

    private var connection: Connection? = null
    private val questdbUrl = "jdbc:postgresql://$host:$port/$database"

    fun connect() {
        try {
            println("Connecting to QuestDB: $questdbUrl")
            
            // Load PostgreSQL JDBC driver (QuestDB uses PostgreSQL wire protocol)
            Class.forName("org.postgresql.Driver")
            
            val properties = Properties()
            properties.setProperty("user", USERNAME)
            properties.setProperty("password", PASSWORD)
            
            connection = DriverManager.getConnection(questdbUrl, properties)
            
            println("Connected to QuestDB successfully")
            
        } catch (e: Exception) {
            println("Failed to connect to QuestDB: ${e.javaClass.name}: ${e.message}")
            e.printStackTrace()
            throw e
        }
    }

    fun disconnect() {
        try {
            connection?.close()
            connection = null
            println("Disconnected from QuestDB")
        } catch (e: Exception) {
            println("Error disconnecting from QuestDB: ${e.message}")
        }
    }

    /**
     * EXACT copy of writeBulk function from PostgreSQLLogger.kt
     * This is the actual function used in production MonsterMQ
     */
    fun writeBulk(tableName: String, rows: List<BufferedRow>) {
        val conn = connection ?: throw SQLException("Not connected to PostgreSQL")

        if (rows.isEmpty()) {
            return
        }

        try {
            // Build INSERT statement based on fields in first row
            val fields = rows.first().fields.keys.toList()
            val placeholders = fields.joinToString(", ") { "?" }
            val fieldNames = fields.joinToString(", ")

            val sql = "INSERT INTO $tableName ($fieldNames) VALUES ($placeholders)"

            println("Writing bulk of ${rows.size} rows to table $tableName SQL: $sql")

            conn.prepareStatement(sql).use { ps ->
                rows.forEach { row ->
                    // Set parameter values with type-specific setters for better performance
                    fields.forEachIndexed { index, fieldName ->
                        val value = row.fields[fieldName]
                        val paramIndex = index + 1
                        println("Setting parameter $paramIndex to value '$value' (${value?.javaClass?.name ?: "null"})")
                        when (value) {
                            null -> ps.setNull(paramIndex, java.sql.Types.NULL)
                            is String -> ps.setString(paramIndex, value)
                            is Int -> ps.setInt(paramIndex, value)
                            is Long -> ps.setLong(paramIndex, value)
                            is Double -> ps.setDouble(paramIndex, value)
                            is Float -> ps.setFloat(paramIndex, value)
                            is Boolean -> ps.setBoolean(paramIndex, value)
                            is java.sql.Timestamp -> ps.setTimestamp(paramIndex, value)
                            is java.sql.Date -> ps.setDate(paramIndex, value)
                            is java.sql.Time -> ps.setTime(paramIndex, value)
                            is java.math.BigDecimal -> ps.setBigDecimal(paramIndex, value)
                            is ByteArray -> ps.setBytes(paramIndex, value)
                            else -> ps.setObject(paramIndex, value)  // Fallback for other types
                        }
                    }
                    ps.addBatch()
                }

                // Execute batch
                val results = ps.executeBatch()

                println("Successfully wrote ${results.size} rows to table $tableName")
            }

        } catch (e: SQLException) {
            println("SQL error writing to table $tableName: ${e.javaClass.name}: ${e.message}")
            println("SQL State: ${e.sqlState}, Error Code: ${e.errorCode}")

            // Log the full exception with stack trace
            val sw = java.io.StringWriter()
            e.printStackTrace(java.io.PrintWriter(sw))
            println("Stack trace:\n$sw")

            // Check for table not found errors
            if (e.message?.contains("table", ignoreCase = true) == true ||
                e.message?.contains("relation", ignoreCase = true) == true ||
                e.sqlState == "42P01") {  // PostgreSQL error code for undefined_table
                println("=".repeat(80))
                println("ERROR: Table '$tableName' does not exist!")
                println("You need to create the table first based on your JSON schema.")
                println("=".repeat(80))
            }

            throw e
        }
    }

    fun createTestTable() {
        val conn = connection ?: throw SQLException("Not connected")
        
        try {
            val createTableSQL = """
                CREATE TABLE IF NOT EXISTS '$TABLE_NAME' ( 
                    ts TIMESTAMP,
                    info STRING,
                    value DOUBLE
                ) timestamp(ts) PARTITION BY DAY WAL
            """.trimIndent()
            
            println("Creating table if not exists...")
            println("SQL: $createTableSQL")
            
            conn.createStatement().use { stmt ->
                stmt.execute(createTableSQL)
            }
            
            println("Table $TABLE_NAME is ready")
            
        } catch (e: SQLException) {
            println("Error creating table: ${e.message}")
            e.printStackTrace()
            throw e
        }
    }

    fun runBulkTest() {
        try {
            connect()
            createTestTable()
            
            println("Starting bulk insert test...")
            println("Batch size: $BATCH_SIZE")
            println("Number of batches: $numBatches")
            println("Delay between batches: ${BULK_DELAY_MS}ms")
            println("Total rows: ${BATCH_SIZE * numBatches}")
            
            val totalStartTime = System.currentTimeMillis()
            
            repeat(numBatches) { batchNum ->
                println("\n--- Batch ${batchNum + 1}/$numBatches ---")
                
                // Generate test data
                val rows = generateTestData(BATCH_SIZE)
                
                // Write bulk using EXACT PostgreSQLLogger.kt function
                writeBulk(TABLE_NAME, rows)
                
                // Add delay between bulk writes (except after the last batch)
                if (batchNum < numBatches - 1) {
                    println("Waiting ${BULK_DELAY_MS}ms before next batch...")
                    Thread.sleep(BULK_DELAY_MS.toLong())
                }
            }
            
            val totalEndTime = System.currentTimeMillis()
            val totalTime = totalEndTime - totalStartTime
            val totalRows = BATCH_SIZE * numBatches
            val rowsPerSecond = (totalRows * 1000.0) / totalTime
            
            println("\n=== Test Results ===")
            println("Total time: ${totalTime}ms")
            println("Total rows: $totalRows")
            println("Rows per second: %.2f".format(rowsPerSecond))
            
        } catch (e: Exception) {
            println("Test failed: ${e.message}")
            e.printStackTrace()
        } finally {
            disconnect()
        }
    }

    private fun generateTestData(count: Int): List<BufferedRow> {
        val rows = mutableListOf<BufferedRow>()
        val baseTime = System.currentTimeMillis()
        
        repeat(count) { i ->
            val ts = Timestamp(baseTime + (i * 1000)) // 1 second intervals
            val info = "test_info_${i}_${Random.nextInt(1000)}"
            val value = Random.nextDouble(0.0, 100.0)
            
            // Create BufferedRow with field map - EXACT same as PostgreSQLLogger.kt uses
            val fields = mapOf(
                "ts" to ts,
                "info" to info, 
                "value" to value
            )
            
            rows.add(BufferedRow(
                tableName = TABLE_NAME,
                fields = fields,
                timestamp = Instant.ofEpochMilli(ts.time),
                topic = "test/topic/$i"
            ))
        }
        
        return rows
    }
}

fun main(args: Array<String>) {
    println("QuestDB Bulk Insert Test")
    println("========================")
    
    // Parse command line arguments
    var host = "localhost"
    var port = 8812
    var database = "qdb"
    var batches = 10
    
    if (args.isNotEmpty()) {
        // Usage: QuestDBTest [host] [port] [database] [batches]
        // Example: QuestDBTest localhost 8812 qdb 20
        //          QuestDBTest 192.168.1.100 9999 mydb 50
        
        when (args.size) {
            1 -> {
                host = args[0]
                println("Using host: $host (port: $port, database: $database, batches: $batches)")
            }
            2 -> {
                host = args[0]
                port = args[1].toIntOrNull() ?: 8812
                println("Using host: $host, port: $port (database: $database, batches: $batches)")
            }
            3 -> {
                host = args[0] 
                port = args[1].toIntOrNull() ?: 8812
                database = args[2]
                println("Using host: $host, port: $port, database: $database (batches: $batches)")
            }
            4 -> {
                host = args[0] 
                port = args[1].toIntOrNull() ?: 8812
                database = args[2]
                batches = args[3].toIntOrNull() ?: 10
                println("Using host: $host, port: $port, database: $database, batches: $batches")
            }
            else -> {
                println("Usage: QuestDBTest [host] [port] [database] [batches]")
                println("Example: QuestDBTest localhost 8812 qdb 20")
                println("         QuestDBTest linux5 8812 qdb 50")
                println("Using defaults: localhost:8812/qdb with 10 batches")
            }
        }
    } else {
        println("Using defaults: $host:$port/$database with $batches batches")
        println("Usage: QuestDBTest [host] [port] [database] [batches]")
    }
    
    val test = QuestDBBulkTest(host, port, database, batches)
    test.runBulkTest()
}