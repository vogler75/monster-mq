package at.rocworks.stores

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.util.Base64

/**
 * Decodes archived message payloads for history output with priority:
 *   1. Parsed JSON value (JsonObject / JsonArray) if the input parses as JSON.
 *   2. UTF-8 text string if the raw bytes are valid UTF-8.
 *   3. Base64 string as a last resort.
 *
 * Used by every getHistory() implementation to write a single `payload` field
 * on the row (or `payload_base64` when binary-only).
 */
object PayloadDecoder {

    private val base64Encoder = Base64.getEncoder()

    /**
     * Result of decoding a payload. Either `payload` is non-null (parsed JSON,
     * text string), or `base64` is non-null (binary fallback). Never both.
     */
    data class Decoded(val payload: Any?, val base64: String?) {
        fun applyTo(row: JsonObject) {
            if (payload != null) row.put("payload", payload)
            else if (base64 != null) row.put("payload_base64", base64)
        }
    }

    /**
     * Decode raw payload bytes into a display-ready form.
     */
    fun decode(bytes: ByteArray?): Decoded {
        if (bytes == null || bytes.isEmpty()) return Decoded(null, null)
        parseJson(bytes)?.let { return Decoded(it, null) }
        decodeUtf8(bytes)?.let { return Decoded(it, null) }
        return Decoded(null, base64Encoder.encodeToString(bytes))
    }

    /**
     * Decode when a separately-stored structured JSON string is already available
     * (e.g. Postgres `payload_json`, CrateDB `payload_obj`). If the string parses
     * as JSON, return the parsed value. If it does not, fall back to treating the
     * string as text (and, if that also fails, the caller-supplied raw bytes).
     */
    fun decode(structuredJson: String?, rawBytes: ByteArray?): Decoded {
        if (structuredJson != null) {
            parseJson(structuredJson)?.let { return Decoded(it, null) }
            // Structured string was not valid JSON — treat as text.
            return Decoded(structuredJson, null)
        }
        return decode(rawBytes)
    }

    /**
     * Decode a pre-parsed JSON-ish value (e.g. MongoDB native BSON Document/List).
     * Returned as a `JsonObject` / `JsonArray` when possible, so consumers don't
     * have to re-parse.
     */
    fun decodeNative(native: Any?): Decoded {
        return when (native) {
            null -> Decoded(null, null)
            is JsonObject, is JsonArray -> Decoded(native, null)
            is Map<*, *> -> Decoded(JsonObject(native.mapKeys { it.key.toString() }), null)
            is List<*> -> Decoded(JsonArray(native), null)
            is String -> decode(structuredJson = native, rawBytes = null)
            is ByteArray -> decode(native)
            else -> Decoded(native.toString(), null)
        }
    }

    private fun parseJson(text: String): Any? {
        val trimmed = text.trimStart()
        if (trimmed.isEmpty()) return null
        return try {
            when (trimmed[0]) {
                '{' -> JsonObject(text)
                '[' -> JsonArray(text)
                else -> null
            }
        } catch (_: Exception) {
            null
        }
    }

    private fun parseJson(bytes: ByteArray): Any? {
        val text = decodeUtf8(bytes) ?: return null
        return parseJson(text)
    }

    private fun decodeUtf8(bytes: ByteArray): String? {
        val decoder = StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
        return try {
            decoder.decode(java.nio.ByteBuffer.wrap(bytes)).toString()
        } catch (_: CharacterCodingException) {
            null
        }
    }
}
