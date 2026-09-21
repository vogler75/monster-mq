package at.rocworks.genai.decision

import at.rocworks.Utils
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import java.net.URI
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

class OpenRouterDecisionProvider(
    private val vertx: Vertx,
    private val apiKey: String,
    private val endpoint: String? = null
) : IDecisionProvider {
    override val providerName: String = "openrouter"
    private val logger: Logger = Utils.getLogger(OpenRouterDecisionProvider::class.java)

    private val webClient: WebClient by lazy {
        val options = WebClientOptions()
            .setKeepAlive(true)
            .setConnectTimeout(10000)
            .setTrustAll(true)
            .setVerifyHost(false)
        WebClient.create(vertx, options)
    }

    override fun decide(
        model: String,
        questions: JsonObject,
        state: JsonObject,
        timeoutSeconds: Long
    ): CompletableFuture<JsonObject> {
        val future = CompletableFuture<JsonObject>()
        try {
            val targetUri = resolveDecisionsUri(endpoint)
            val isSsl = targetUri.scheme?.equals("https", ignoreCase = true) ?: true
            val port = if (targetUri.port != -1) targetUri.port else (if (isSsl) 443 else 80)
            val host = targetUri.host ?: "openrouter.ai"
            val requestPath = if (targetUri.path.isNullOrBlank()) "/api/alpha/decisions" else targetUri.path

            val requestBody = JsonObject()
                .put("model", model)
                .put("questions", questions)
                .put("state", state)

            logger.fine { "Sending decision request to $host:$port$requestPath (model=$model)" }

            webClient.post(port, host, requestPath)
                .ssl(isSsl)
                .timeout(timeoutSeconds * 1000L)
                .putHeader("Authorization", "Bearer $apiKey")
                .putHeader("Content-Type", "application/json")
                .putHeader("HTTP-Referer", "https://github.com/vogler75/monster-mq")
                .putHeader("X-Title", "MonsterMQ")
                .sendJsonObject(requestBody)
                .onComplete { ar ->
                    if (ar.succeeded()) {
                        val response = ar.result()
                        val statusCode = response.statusCode()
                        val body = response.bodyAsString() ?: ""
                        if (statusCode in 200..299) {
                            try {
                                val json = JsonObject(body)
                                future.complete(json)
                            } catch (e: Exception) {
                                future.completeExceptionally(IllegalStateException("Invalid JSON response from OpenRouter ($statusCode): $body", e))
                            }
                        } else {
                            future.completeExceptionally(IllegalStateException("OpenRouter decision API returned HTTP $statusCode: $body"))
                        }
                    } else {
                        future.completeExceptionally(ar.cause())
                    }
                }
        } catch (e: Exception) {
            future.completeExceptionally(e)
        }
        return future
    }

    /**
     * Resolves the decision endpoint URI.
     * If empty, defaults to https://openrouter.ai/api/alpha/decisions.
     * If an OpenAI-compatible endpoint is passed (e.g. https://openrouter.ai/api/v1), translates it to the decisions path.
     */
    fun resolveDecisionsUri(endpoint: String?): URI {
        if (endpoint.isNullOrBlank()) {
            return URI.create("https://openrouter.ai/api/alpha/decisions")
        }
        val clean = endpoint.trimEnd('/')
        return if (clean.endsWith("/alpha/decisions")) {
            URI.create(clean)
        } else if (clean.endsWith("/api/v1")) {
            val base = clean.removeSuffix("/v1")
            URI.create("$base/alpha/decisions")
        } else if (clean.endsWith("/api")) {
            URI.create("$clean/alpha/decisions")
        } else if (clean.contains("/decisions")) {
            URI.create(clean)
        } else {
            URI.create("$clean/api/alpha/decisions")
        }
    }

    fun close() {
        try {
            webClient.close()
        } catch (e: Exception) {
            logger.fine { "Error closing WebClient: ${e.message}" }
        }
    }
}
