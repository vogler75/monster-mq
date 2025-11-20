package at.rocworks.logger

import java.sql.SQLException

/**
 * QuestDB implementation of JDBC Logger
 * Uses PostgreSQL wire protocol but with QuestDB-specific table partitioning
 * Optimized for time-series data with fast ingestion and out-of-order inserts
 *
 * Duplicate Key Handling:
 * - QuestDB supports PostgreSQL's ON CONFLICT DO NOTHING syntax (inherited from PostgreSQLLogger)
 * - When ignoreDuplicates=true and uniqueKeyColumns are configured, duplicate rows are silently skipped
 * - QuestDB uses same SQL State codes as PostgreSQL (23505 for unique_violation)
 */
class QuestDBLogger : PostgreSQLLogger() {

    /**
     * Creates a QuestDB table with proper partitioning for time-series data.
     * Overrides PostgreSQL implementation to use QuestDB-specific syntax:
     * - Single quotes for table names (QuestDB convention)
     * - QuestDB types (STRING, DOUBLE, LONG instead of TEXT, DOUBLE PRECISION, BIGINT)
     * - timestamp(field) designation for time-series optimization
     * - PARTITION BY clause for out-of-order insert support
     * - No SERIAL primary key (QuestDB handles row IDs internally)
     */
    override fun createTableIfNotExists(tableName: String) {
        val conn = connection ?: throw SQLException("Not connected")

        try {
            logger.info("Checking if QuestDB table '$tableName' exists...")

            // Extract field definitions from JSON schema
            val schemaProperties = cfg.jsonSchema.getJsonObject("properties")
            if (schemaProperties == null || schemaProperties.isEmpty) {
                logger.warning("No properties defined in JSON schema, skipping table creation")
                return
            }

            // Find timestamp field (first one with format=timestamp or format=timestampms)
            var timestampField: String? = null
            val columns = mutableListOf<String>()

            schemaProperties.fieldNames().forEach { fieldName ->
                val fieldSchema = schemaProperties.getJsonObject(fieldName)
                val fieldType = fieldSchema?.getString("type") ?: "string"
                val format = fieldSchema?.getString("format")

                // Determine QuestDB type
                val sqlType = when {
                    format == "timestamp" || format == "timestampms" -> {
                        if (timestampField == null) {
                            timestampField = fieldName  // Remember first timestamp field
                        }
                        "TIMESTAMP"
                    }
                    fieldType == "string" -> "STRING"
                    fieldType == "number" -> "DOUBLE"
                    fieldType == "integer" -> "LONG"
                    fieldType == "boolean" -> "BOOLEAN"
                    else -> "STRING"
                }

                columns.add("    $fieldName $sqlType")
            }

            // Add topic name column if configured
            if (cfg.topicNameColumn != null) {
                columns.add("    ${cfg.topicNameColumn} STRING")
            }

            if (timestampField == null) {
                logger.warning("No timestamp field found in JSON schema, cannot create QuestDB table (timestamp designation required)")
                return
            }

            // Build CREATE TABLE statement with QuestDB partitioning
            val columnsSQL = columns.joinToString(",\n")
            val createTableSQL = """
                CREATE TABLE IF NOT EXISTS '$tableName' (
                $columnsSQL
                ) timestamp($timestampField) PARTITION BY ${cfg.partitionBy}                
                """.trimIndent()

            logger.fine("Executing CREATE TABLE:\n$createTableSQL")

            conn.createStatement().use { stmt ->
                stmt.execute(createTableSQL)
            }

            logger.info("QuestDB table '$tableName' is ready with PARTITION BY ${cfg.partitionBy}")

        } catch (e: SQLException) {
            logger.warning("Error creating table '$tableName': ${e.message}")
            // Don't throw - table might already exist, which is fine
        }
    }
}
