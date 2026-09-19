package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Script configuration for standalone Python/Starlark/JavaScript script devices.
 */
data class ScriptConfig(
    val language: String = DEFAULT_LANGUAGE,
    val script: String = "",
    val triggerType: String = TRIGGER_TOPIC,
    val topicFilters: List<String> = emptyList(),
    val triggerOnChangeOnly: Boolean = false,
    val timerIntervalMs: Int = 0,
    val instanceMode: String = MODE_SINGLETON,
    val timeoutMs: Int = DEFAULT_TIMEOUT_MS,
    val description: String? = null
) {
    companion object {
        const val DEFAULT_LANGUAGE = "python"
        const val DEFAULT_TIMEOUT_MS = 200

        const val TRIGGER_TOPIC = "TOPIC"
        const val TRIGGER_TIMER = "TIMER"
        const val TRIGGER_BOTH = "BOTH"
        const val TRIGGER_CALLABLE = "CALLABLE"

        const val MODE_SINGLETON = "SINGLETON"
        const val MODE_MULTI_INSTANCE = "MULTI_INSTANCE"

        fun fromJsonObject(json: JsonObject): ScriptConfig {
            val filters = when {
                json.containsKey("topicFilters") -> {
                    json.getJsonArray("topicFilters", JsonArray()).mapNotNull { it?.toString() }
                }
                json.containsKey("topicFilter") -> {
                    val single = json.getString("topicFilter")
                    if (!single.isNullOrBlank()) listOf(single) else emptyList()
                }
                else -> emptyList()
            }

            return ScriptConfig(
                language = json.getString("language", DEFAULT_LANGUAGE).lowercase(),
                script = json.getString("script", ""),
                triggerType = json.getString("triggerType", TRIGGER_TOPIC).uppercase(),
                topicFilters = filters,
                triggerOnChangeOnly = json.getBoolean("triggerOnChangeOnly", false),
                timerIntervalMs = json.getInteger("timerIntervalMs", 0),
                instanceMode = json.getString("instanceMode", MODE_SINGLETON).uppercase(),
                timeoutMs = json.getInteger("timeoutMs", DEFAULT_TIMEOUT_MS).let { if (it <= 0) DEFAULT_TIMEOUT_MS else it },
                description = json.getString("description")
            )
        }
    }

    fun toJsonObject(): JsonObject = JsonObject()
        .put("language", language)
        .put("script", script)
        .put("triggerType", triggerType)
        .put("topicFilters", JsonArray(topicFilters))
        .put("triggerOnChangeOnly", triggerOnChangeOnly)
        .put("timerIntervalMs", timerIntervalMs)
        .put("instanceMode", instanceMode)
        .put("timeoutMs", timeoutMs)
        .apply {
            if (description != null) put("description", description)
        }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()
        if (script.isBlank()) {
            errors.add("Script code cannot be blank")
        }
        if (triggerType !in setOf(TRIGGER_TOPIC, TRIGGER_TIMER, TRIGGER_BOTH, TRIGGER_CALLABLE)) {
            errors.add("Invalid triggerType: $triggerType. Expected TOPIC, TIMER, BOTH, or CALLABLE")
        }
        if ((triggerType == TRIGGER_TOPIC || triggerType == TRIGGER_BOTH) && topicFilters.isEmpty()) {
            errors.add("At least one topic filter is required when triggerType is $triggerType")
        }
        if ((triggerType == TRIGGER_TIMER || triggerType == TRIGGER_BOTH) && timerIntervalMs <= 0) {
            errors.add("timerIntervalMs must be > 0 when triggerType is $triggerType")
        }
        if (instanceMode !in setOf(MODE_SINGLETON, MODE_MULTI_INSTANCE)) {
            errors.add("Invalid instanceMode: $instanceMode. Expected SINGLETON or MULTI_INSTANCE")
        }
        if (timeoutMs <= 0) {
            errors.add("timeoutMs must be positive")
        }
        return errors
    }
}
