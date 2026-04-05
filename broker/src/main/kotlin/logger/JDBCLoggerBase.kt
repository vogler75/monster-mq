package at.rocworks.logger

import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.devices.JDBCLoggerConfig
import io.vertx.core.json.JsonObject

/**
 * Abstract base class for JDBC Logger implementations.
 * Provides JDBC-specific parameter and type handling.
 * Subclasses implement database-specific connection and write operations.
 */
abstract class JDBCLoggerBase : LoggerBase<JDBCLoggerConfig>() {

    override fun loadConfig(cfgJson: JsonObject): JDBCLoggerConfig {
        return JDBCLoggerConfig.fromJson(cfgJson)
    }

    override fun getMetricsAddress(): String {
        return EventBusAddresses.JDBCLoggerBridge.connectorMetrics(device.name)
    }

    /**
     * Sets a parameter value in a PreparedStatement with appropriate type handling
     * @param ps The PreparedStatement to set the parameter on
     * @param paramIndex The 1-based parameter index
     * @param value The value to set (can be null)
     */
    protected fun setParameterValue(ps: java.sql.PreparedStatement, paramIndex: Int, value: Any?) {
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

    /**
     * Converts a value to its corresponding PostgreSQL SQL type string
     * Used for generating CREATE TABLE examples in error messages
     * @param value The value to determine the SQL type for
     * @return The PostgreSQL SQL type string
     */
    protected fun getSQLType(value: Any?): String {
        return when (value) {
            null -> "TEXT"
            is String -> "TEXT"
            is Int -> "INTEGER"
            is Long -> "BIGINT"
            is Double -> "DOUBLE PRECISION"
            is Float -> "REAL"
            is Boolean -> "BOOLEAN"
            is java.sql.Timestamp -> "TIMESTAMP"
            is java.sql.Date -> "DATE"
            is java.sql.Time -> "TIME"
            is java.math.BigDecimal -> "DECIMAL"
            is ByteArray -> "BYTEA"
            else -> "TEXT"
        }
    }

    // Subclasses still need to implement:
    // abstract fun connect(): Future<Void>
    // abstract fun disconnect(): Future<Void>
    // abstract fun writeBulk(tableName: String, rows: List<BufferedRow>)
    // abstract fun isConnectionError(e: Exception): Boolean
}
