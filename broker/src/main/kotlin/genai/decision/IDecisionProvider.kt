package at.rocworks.genai.decision

import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture

/**
 * Common abstraction for structured System-1 decision providers (e.g. OpenRouter Jev).
 */
interface IDecisionProvider {
    val providerName: String

    /**
     * Executes a structured decision request.
     *
     * @param model Decision model identifier (e.g. "typesafe/jev-1.13")
     * @param questions Questions definition (noul, choice, score with criteria)
     * @param state Context state to evaluate (trigger, current lastval, historical aggregates)
     * @param timeoutSeconds Timeout in seconds
     * @return CompletableFuture containing the decision response (e.g. containing "answers" object)
     */
    fun decide(
        model: String,
        questions: JsonObject,
        state: JsonObject,
        timeoutSeconds: Long = 30
    ): CompletableFuture<JsonObject>
}
