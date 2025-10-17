package at.rocworks.stores

import at.rocworks.Utils
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
                        logger.warning("Connect failed! [${result.cause().message}] [${Utils.getCurrentFunctionName()}]")
                        vertx.setTimer(defaultRetryWaitTime) {
                            connect(vertx, connectPromise)
                        }
                    }
                }
            } catch (e: Exception) {
                logger.warning("Error in connect [${e.message}] [${Utils.getCurrentFunctionName()}]")
                connectPromise.fail(e)
            }
        })
    }

    private fun open(): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            logger.fine { "Connect to database [${Utils.getCurrentFunctionName()}]" }
            DriverManager.getConnection(url, username, password)
                ?.let { connection ->
                    this.connection = connection
                    logger.info("Connection established [${Utils.getCurrentFunctionName()}]")
                    init(connection).onSuccess { promise.complete() }.onFailure { promise.fail(it) }
                }
        } catch (e: Exception) {
            logger.warning("Error opening connection [${e.message}] [${Utils.getCurrentFunctionName()}]")
            promise.fail(e)
        }
        return promise.future()
    }

    fun check(): Boolean {
        if (connection != null && !connection!!.isClosed) {
            try {
                connection!!.prepareStatement("SELECT 1").use { stmt ->
                    stmt.executeQuery().use { rs ->
                        if (rs.next()) {
                            return true // Connection is good
                        }
                    }
                }
            } catch (e: Exception) {
                logger.warning("Error checking connection [${e.message}] [${Utils.getCurrentFunctionName()}]")
                // If the connection has an aborted transaction, try to rollback
                if (e.message?.contains("aborted") == true || e.message?.contains("transaction") == true) {
                    try {
                        connection?.rollback()
                        logger.info("Rolled back aborted transaction during connection check [${Utils.getCurrentFunctionName()}]")
                    } catch (rollbackEx: Exception) {
                        logger.warning("Error rolling back aborted transaction: ${rollbackEx.message} [${Utils.getCurrentFunctionName()}]")
                    }
                }
            }
        }
        return false
    }


    private fun reconnect(vertx: Vertx) {
        if (!reconnectOngoing) {
            logger.info("Reconnect [${Utils.getCurrentFunctionName()}]")
            reconnectOngoing = true
            val promise = Promise.promise<Void>()
            promise.future().onComplete {
                reconnectOngoing = false
            }
            connect(vertx, promise)
        }
    }

    abstract fun init(connection: Connection): Future<Void>
}