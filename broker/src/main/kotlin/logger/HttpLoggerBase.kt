package at.rocworks.logger

import at.rocworks.stores.devices.ILoggerConfig
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.buffer.Buffer
import io.vertx.ext.web.client.HttpRequest
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions

/**
 * Abstract base class for HTTP-based loggers.
 * Provides WebClient lifecycle, authentication, and HTTP POST sending.
 * Subclasses implement format-specific writeBulk (e.g., InfluxDB Line Protocol).
 */
abstract class HttpLoggerBase<T : ILoggerConfig> : LoggerBase<T>() {

    protected var client: WebClient? = null

    override fun connect(): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            val options = WebClientOptions()
                .setUserAgent("MonsterMQ-HttpLogger")
                .setConnectTimeout(5000)
                .setIdleTimeout(30)

            client = WebClient.create(vertx, options)
            logger.info("HTTP Logger client initialized for endpoint: ${getEndpointUrl()}")
            promise.complete()
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun disconnect(): Future<Void> {
        client?.close()
        client = null
        return Future.succeededFuture()
    }

    override fun isConnectionError(e: Exception): Boolean {
        val msg = e.message?.lowercase() ?: ""
        return msg.contains("timeout") || msg.contains("connection refused") ||
               msg.contains("host unreachable") || msg.contains("reset by peer")
    }

    /**
     * Returns the endpoint URL from the config. Subclasses must provide this.
     */
    protected abstract fun getEndpointUrl(): String

    /**
     * Returns the auth type from the config (NONE, BASIC, TOKEN).
     */
    protected abstract fun getAuthType(): String

    /**
     * Applies authentication headers/params to the request.
     */
    protected abstract fun applyAuth(request: HttpRequest<Buffer>)

    /**
     * Applies custom headers from config to the request.
     */
    protected abstract fun applyHeaders(request: HttpRequest<Buffer>)

    /**
     * Sends an HTTP POST with the given payload string to the default endpoint URL.
     */
    protected fun sendPost(payload: String, configureRequest: (HttpRequest<Buffer>) -> Unit = {}) {
        sendPostTo(getEndpointUrl(), payload, configureRequest)
    }

    /**
     * Sends an HTTP POST with the given payload string to an explicit URL.
     */
    protected fun sendPostTo(url: String, payload: String, configureRequest: (HttpRequest<Buffer>) -> Unit = {}) {
        val client = this.client ?: throw IllegalStateException("WebClient not initialized")

        var request = client.postAbs(url)

        applyAuth(request)
        applyHeaders(request)
        configureRequest(request)

        val promise = Promise.promise<Void>()
        request.sendBuffer(Buffer.buffer(payload))
            .onSuccess { response ->
                if (response.statusCode() in 200..299) {
                    promise.complete()
                } else {
                    val errorMsg = "HTTP error ${response.statusCode()}: ${response.bodyAsString()}"
                    logger.warning(errorMsg)
                    promise.fail(RuntimeException(errorMsg))
                }
            }
            .onFailure { error ->
                promise.fail(error)
            }

        try {
            promise.future().toCompletionStage().toCompletableFuture().get(15, java.util.concurrent.TimeUnit.SECONDS)
        } catch (e: Exception) {
            throw if (e.cause is Exception) e.cause as Exception else e
        }
    }
}
