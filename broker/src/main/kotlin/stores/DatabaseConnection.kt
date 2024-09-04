package at.rocworks.stores

import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.Callable
import java.util.logging.Logger

abstract class DatabaseConnection(
    private val logger: Logger,
    private val url: String,
    private val username: String,
    private val password: String
) {
    var connection: Connection? = null
    private val defaultRetryWaitTime = 3000L
    private var reconnectOngoing = false

    fun start(vertx: Vertx, startPromise: Promise<Void>) = connect(vertx, startPromise)

    private fun connect(vertx: Vertx, connectPromise: Promise<Void>) {
        vertx.executeBlocking(Callable {
            try {
                open().onComplete { result ->
                    if (result.succeeded()) {
                        vertx.setPeriodic(5000) { // TODO: configurable
                            if (!check()) reconnect(vertx)
                        }
                        connectPromise.complete()
                    } else {
                        logger.warning("Connect failed!")
                        vertx.setTimer(defaultRetryWaitTime) {
                            connect(vertx, connectPromise)
                        }
                    }
                }
            } catch (e: Exception) {
                logger.warning("Error in connect [${e.message}]")
                connectPromise.fail(e)
            }
        })
    }

    private fun open(): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            logger.info("Connect to database...")
            DriverManager.getConnection(url, username, password)
                ?.let {
                    it.autoCommit = true
                    logger.info("Connection established.")
                    checkTable(it)
                    connection = it
                    promise.complete()
                }
        } catch (e: Exception) {
            logger.warning("Error opening connection [${e.message}]")
            promise.fail(e)
        }
        return promise.future()
    }

    private fun check(): Boolean {
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


    private fun reconnect(vertx: Vertx) {
        if (!reconnectOngoing) {
            logger.info("Reconnect...")
            reconnectOngoing = true
            val promise = Promise.promise<Void>()
            promise.future().onComplete {
                reconnectOngoing = false
            }
            connect(vertx, promise)
        }
    }

    abstract fun checkTable(connection: Connection)
}