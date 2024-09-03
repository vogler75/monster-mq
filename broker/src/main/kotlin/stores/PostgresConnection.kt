package at.rocworks.stores

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement
import java.util.logging.Logger

abstract class PostgresConnection(
    private val logger: Logger,
    private val url: String,
    private val username: String,
    private val password: String
) {
    var connection: Connection? = null

    fun checkConnection(): Boolean {
        if (connection != null && !connection!!.isClosed) {
            connection!!.prepareStatement("SELECT 1").use { stmt ->
                stmt.executeQuery().use { rs ->
                    if (rs.next()) {
                        return true // Connection is good
                    }
                }
            }
        }
        return false
    }

    fun connectDatabase() {
        try {
            logger.info("Connect to PostgreSQL Database...")
            DriverManager.getConnection(url, username, password)
                ?.let { // TODO: check failure and retry to connect
                    it.autoCommit = true
                    logger.info("Connection established.")
                    checkTable(it)
                    connection = it
                }
        } catch (e: Exception) {
            logger.warning("Error opening connection [${e.message}]")
        }
    }

    abstract fun checkTable(connection: Connection)
}