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
    @Volatile
    private var disconnectedLogged = false
    @Volatile
    private var isStopped = false
    @Volatile
    private var isConnected = false
    private var periodicTimerId: Long? = null
    private var retryTimerId: Long? = null
    private var vertxInstance: Vertx? = null

    fun start(vertx: Vertx, startPromise: Promise<Void>) {
        this.vertxInstance = vertx
        this.isStopped = false
        connect(vertx, startPromise)
    }

    fun stop(): Future<Void> {
        isStopped = true
        isConnected = false
        vertxInstance?.let { v ->
            periodicTimerId?.let { id -> v.cancelTimer(id) }
            retryTimerId?.let { id -> v.cancelTimer(id) }
        }
        periodicTimerId = null
        retryTimerId = null
        try {
            connection?.close()
        } catch (e: Exception) {
            logger.finer { "Error closing connection: ${e.message}" }
        }
        connection = null
        return Future.succeededFuture()
    }

    fun getConnectionStatus(): Boolean {
        return !isStopped && isConnected && connection != null
    }

    private fun connect(vertx: Vertx, connectPromise: Promise<Void>) {
        if (isStopped) {
            isConnected = false
            connectPromise.tryFail("Database connection is stopped")
            return
        }
        vertx.executeBlocking(Callable {
            if (isStopped) return@Callable
            try {
                open().onComplete { result ->
                    if (isStopped) return@onComplete
                    if (result.succeeded()) {
                        periodicTimerId = vertx.setPeriodic(5000) { // TODO: configurable
                            if (isStopped) return@setPeriodic
                            // Execute blocking health check on worker thread pool
                            vertx.executeBlocking(Callable {
                                if (isStopped) false else check()
                            }).onComplete { checkResult ->
                                if (isStopped) return@onComplete
                                if (checkResult.succeeded()) {
                                    if (!checkResult.result()) {
                                        reconnect(vertx)
                                    }
                                } else {
                                    logger.warning("Health check failed: ${checkResult.cause()?.message} [${Utils.getCurrentFunctionName()}]")
                                    reconnect(vertx)
                                }
                            }
                        }
                        connectPromise.tryComplete()
                    } else {
                        isConnected = false
                        if (!isStopped) {
                            logger.warning("Connect failed! [${result.cause().message}] [${Utils.getCurrentFunctionName()}]")
                            retryTimerId = vertx.setTimer(defaultRetryWaitTime) {
                                if (!isStopped) {
                                    connect(vertx, connectPromise)
                                }
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                isConnected = false
                logger.warning("Error in connect [${e.message}] [${Utils.getCurrentFunctionName()}]")
                connectPromise.tryFail(e)
            }
        })
    }

    private fun open(): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            logger.fine { "Connect to database [${Utils.getCurrentFunctionName()}]" }
            val finalUrl = if (url.startsWith("jdbc:postgresql:") && !url.contains("reWriteBatchedInserts=")) {
                if (url.contains("?")) {
                    "$url&reWriteBatchedInserts=true"
                } else {
                    "$url?reWriteBatchedInserts=true"
                }
            } else {
                url
            }
            DriverManager.getConnection(finalUrl, username, password)
                ?.let { connection ->
                    this.connection = connection
                    if (disconnectedLogged) {
                        disconnectedLogged = false
                        logger.info("Database connection restored [${Utils.getCurrentFunctionName()}]")
                    } else {
                        logger.fine("Connection established [${Utils.getCurrentFunctionName()}]")
                    }
                    init(connection).onSuccess {
                        isConnected = true
                        promise.complete()
                    }.onFailure {
                        isConnected = false
                        promise.fail(it)
                    }
                } ?: run {
                    isConnected = false
                    promise.fail("DriverManager returned null connection")
                }
        } catch (e: Exception) {
            isConnected = false
            logger.warning("Error opening connection [${e.message}] [${Utils.getCurrentFunctionName()}]")
            promise.fail(e)
        }
        return promise.future()
    }

    fun check(): Boolean {
        if (isStopped) {
            isConnected = false
            return false
        }
        if (connection != null && !connection!!.isClosed) {
            try {
                val valid = connection!!.isValid(3)
                isConnected = valid
                return valid
            } catch (e: Exception) {
                isConnected = false
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
        isConnected = false
        return false
    }


    private fun reconnect(vertx: Vertx) {
        if (isStopped) return
        if (!reconnectOngoing) {
            if (!disconnectedLogged) {
                disconnectedLogged = true
                logger.warning("Database connection lost, attempting reconnect [${Utils.getCurrentFunctionName()}]")
            }
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