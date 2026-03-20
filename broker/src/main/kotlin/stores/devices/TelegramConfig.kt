package at.rocworks.stores.devices

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Telegram Bot Client configuration.
 *
 * Topic structure (driven by device namespace):
 *   {namespace}/chats        — retained JSON array of registered chat IDs
 *   {namespace}/in/{chatId}  — incoming messages from Telegram (published by bot)
 *   {namespace}/out/{chatId} — outgoing messages to Telegram (subscribe by bot, forward to chat)
 *
 * Bot commands:
 *   /start — registers the chat, adds chatId to the retained chats topic
 *   /stop  — unregisters the chat, removes chatId from the retained chats topic
 */
data class TelegramClientConfig(
    val botToken: String = "",
    val pollingTimeoutSeconds: Int = 30,
    val reconnectDelayMs: Long = 5000,
    val proxyHost: String? = null,
    val proxyPort: Int? = null,
    val parseMode: String = "Text",             // Default parse mode: Text, HTML, Markdown, MarkdownV2
    val allowedUsers: List<String> = emptyList() // Allowed Telegram usernames (empty = all allowed)
) {
    companion object {
        fun fromJson(json: JsonObject): TelegramClientConfig {
            val allowedUsers = try {
                json.getJsonArray("allowedUsers")?.map { it.toString() } ?: emptyList()
            } catch (e: Exception) {
                emptyList()
            }

            return TelegramClientConfig(
                botToken = json.getString("botToken", ""),
                pollingTimeoutSeconds = json.getInteger("pollingTimeoutSeconds", 30),
                reconnectDelayMs = json.getLong("reconnectDelayMs", 5000L),
                proxyHost = json.getString("proxyHost"),
                proxyPort = json.getInteger("proxyPort"),
                parseMode = json.getString("parseMode", "Text"),
                allowedUsers = allowedUsers
            )
        }
    }

    fun toJsonObject(): JsonObject {
        return JsonObject()
            .put("botToken", botToken)
            .put("pollingTimeoutSeconds", pollingTimeoutSeconds)
            .put("reconnectDelayMs", reconnectDelayMs)
            .put("proxyHost", proxyHost)
            .put("proxyPort", proxyPort)
            .put("parseMode", parseMode)
            .put("allowedUsers", JsonArray(allowedUsers))
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()
        if (botToken.isBlank()) errors.add("botToken cannot be blank")
        if (pollingTimeoutSeconds < 0) errors.add("pollingTimeoutSeconds must be >= 0")
        if (parseMode !in listOf("Text", "HTML", "Markdown", "MarkdownV2"))
            errors.add("parseMode must be one of Text, HTML, Markdown, MarkdownV2")
        return errors
    }

    fun maskedBotToken(): String {
        if (botToken.length <= 4) return "****"
        return "****${botToken.takeLast(4)}"
    }

    fun isUserAllowed(username: String?): Boolean {
        if (allowedUsers.isEmpty()) return true
        if (username.isNullOrBlank()) return false
        return allowedUsers.any { it.equals(username, ignoreCase = true) }
    }
}
