package at.rocworks.stores.devices

import io.nats.client.Nats
import io.nats.client.Options
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.io.FileInputStream
import java.security.KeyStore
import java.time.Duration
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory

/**
 * NATS Client address configuration for subscriptions and publications.
 *  mode = SUBSCRIBE  → NATS subject → MQTT topic  (inbound)
 *  mode = PUBLISH    → MQTT topic   → NATS subject (outbound)
 */
data class NatsClientAddress(
    val mode: String,            // "SUBSCRIBE" or "PUBLISH"
    val natsSubject: String,     // NATS subject (supports wildcards: * and >)
    val mqttTopic: String,       // Local MQTT topic (supports wildcards: + and #)
    val qos: Int = 0,
    val autoConvert: Boolean = true,  // Auto-translate / ↔ ., # ↔ >, + ↔ * when true
    val removePath: Boolean = true    // Strip topic prefix before wildcard and prepend target prefix
) {
    companion object {
        const val MODE_SUBSCRIBE = "SUBSCRIBE"
        const val MODE_PUBLISH = "PUBLISH"

        fun fromJsonObject(json: JsonObject) = NatsClientAddress(
            mode = json.getString("mode"),
            natsSubject = json.getString("natsSubject"),
            mqttTopic = json.getString("mqttTopic"),
            qos = json.getInteger("qos", 0),
            autoConvert = json.getBoolean("autoConvert", true),
            removePath = json.getBoolean("removePath", true)
        )
    }

    fun toJsonObject(): JsonObject = JsonObject()
        .put("mode", mode)
        .put("natsSubject", natsSubject)
        .put("mqttTopic", mqttTopic)
        .put("qos", qos)
        .put("autoConvert", autoConvert)
        .put("removePath", removePath)

    fun validate(): List<String> {
        val errors = mutableListOf<String>()
        if (mode != MODE_SUBSCRIBE && mode != MODE_PUBLISH)
            errors.add("mode must be '$MODE_SUBSCRIBE' or '$MODE_PUBLISH'")
        if (natsSubject.isBlank()) errors.add("natsSubject cannot be blank")
        if (mqttTopic.isBlank()) errors.add("mqttTopic cannot be blank")
        
        // Validate mqttTopic for wildcards in PUBLISH mode (MQTT spec [MQTT-3.3.2-2])
        // In PUBLISH mode, mqttTopic is the publish destination (no wildcards allowed)
        // In SUBSCRIBE mode, mqttTopic is the subscription source (wildcards are okay)
        if (mode == MODE_PUBLISH && (mqttTopic.contains('+') || mqttTopic.contains('#'))) {
            errors.add("mqttTopic cannot contain wildcard characters (+ or #) in PUBLISH mode - wildcards are only allowed in subscription filters")
        }
        
        if (qos !in 0..2) errors.add("qos must be 0, 1, or 2")
        return errors
    }

    fun isSubscribe() = mode == MODE_SUBSCRIBE
    fun isPublish() = mode == MODE_PUBLISH

    // ── Topic/subject translation helpers ──────────────────────────────────

    /**
     * Convert an incoming MQTT topic to a NATS subject.
     *
     * When [removePath] is true and the configured [mqttTopic] contains wildcards,
     * the base path (everything before the first wildcard) is stripped from the
     * incoming topic and the configured [natsSubject] is prepended.
     *
     * Example: mqttTopic="test/#", natsSubject="abc", removePath=true
     *   incoming "test/a/b" → "abc.a.b"
     */
    fun mqttToNatsSubject(incomingMqttTopic: String): String {
        if (!autoConvert) return incomingMqttTopic

        val mqttHasWildcard = mqttTopic.contains('#') || mqttTopic.contains('+')

        if (removePath && mqttHasWildcard) {
            val wildcardIdx = mqttTopic.indexOfFirst { it == '#' || it == '+' }
            val basePath = mqttTopic.substring(0, wildcardIdx).trimEnd('/')
            val suffix = if (basePath.isNotEmpty() && incomingMqttTopic.startsWith(basePath)) {
                incomingMqttTopic.substring(basePath.length).trimStart('/')
            } else if (basePath.isEmpty()) {
                incomingMqttTopic
            } else {
                incomingMqttTopic
            }
            val natsBase = natsSubject.replace(Regex("[*>].*"), "").trimEnd('.')
            val natsSuffix = sanitizeNats(suffix.replace('/', '.'))
            return if (natsBase.isNotEmpty() && natsSuffix.isNotEmpty()) "$natsBase.$natsSuffix"
            else if (natsBase.isNotEmpty()) natsBase
            else natsSuffix
        }

        return sanitizeNats(incomingMqttTopic.replace('/', '.').replace("#", ">").replace("+", "*"))
    }

    /**
     * Convert a NATS subject to an MQTT topic.
     *
     * When [removePath] is true and the configured [natsSubject] contains wildcards,
     * the base path is stripped and the configured [mqttTopic] prefix is prepended.
     *
     * Example: natsSubject="abc.>", mqttTopic="test/#", removePath=true
     *   incoming "abc.a.b" → "test/a/b"
     */
    fun natsToMqttTopic(incomingNatsSubject: String): String {
        if (!autoConvert) return incomingNatsSubject

        val natsHasWildcard = natsSubject.contains('*') || natsSubject.contains('>')

        if (removePath && natsHasWildcard) {
            val wildcardIdx = natsSubject.indexOfFirst { it == '*' || it == '>' }
            val basePath = natsSubject.substring(0, wildcardIdx).trimEnd('.')
            val suffix = if (basePath.isNotEmpty() && incomingNatsSubject.startsWith(basePath)) {
                incomingNatsSubject.substring(basePath.length).trimStart('.')
            } else if (basePath.isEmpty()) {
                incomingNatsSubject
            } else {
                incomingNatsSubject
            }
            val mqttBase = mqttTopic.replace(Regex("[+#].*"), "").trimEnd('/')
            val mqttSuffix = suffix.replace('.', '/')
            return if (mqttBase.isNotEmpty() && mqttSuffix.isNotEmpty()) "$mqttBase/$mqttSuffix"
            else if (mqttBase.isNotEmpty()) mqttBase
            else mqttSuffix
        }

        return incomingNatsSubject.replace('.', '/').replace(">", "#").replace("*", "+")
    }

    /** Sanitize a NATS subject: spaces to underscores, collapse empty segments, trim dots. */
    private fun sanitizeNats(subject: String): String =
        subject.replace(' ', '_').replace(Regex("\\.{2,}"), ".").trim('.')
}

/**
 * Authentication type constants for NATS connections.
 */
object NatsAuthType {
    const val ANONYMOUS = "ANONYMOUS"
    const val USERNAME_PASSWORD = "USERNAME_PASSWORD"
    const val TOKEN = "TOKEN"
    const val TLS = "TLS"
}

/**
 * NATS Client connection and data-flow configuration.
 */
data class NatsClientConfig(
    // ── Connection ────────────────────────────────────────────────────────
    val servers: List<String> = listOf("nats://localhost:4222"),
    val authType: String = NatsAuthType.ANONYMOUS,
    val username: String? = null,
    val password: String? = null,
    val token: String? = null,
    val tlsCaCertPath: String? = null,   // Path to CA certificate (PEM or JKS for TLS auth)
    val tlsVerify: Boolean = true,       // Verify server TLS cert (set false for self-signed)
    val connectTimeoutMs: Long = 5000,
    val reconnectDelayMs: Long = 5000,
    val maxReconnectAttempts: Int = -1,  // -1 = unlimited
    // ── JetStream ─────────────────────────────────────────────────────────
    val useJetStream: Boolean = false,
    val streamName: String? = null,              // JetStream stream to bind inbound subjects to
    val consumerDurableName: String? = null,     // Durable consumer name (enables persistence)
    // ── Addresses ─────────────────────────────────────────────────────────
    val addresses: List<NatsClientAddress> = emptyList()
) {
    companion object {
        fun fromJson(json: JsonObject): NatsClientConfig {
            val serversRaw = json.getValue("servers")
            val servers: List<String> = when (serversRaw) {
                is JsonArray -> serversRaw.map { it.toString() }
                is String -> listOf(serversRaw)
                else -> listOf("nats://localhost:4222")
            }

            val addresses = try {
                json.getJsonArray("addresses")?.map {
                    NatsClientAddress.fromJsonObject(it as JsonObject)
                } ?: emptyList()
            } catch (e: Exception) {
                emptyList()
            }

            return NatsClientConfig(
                servers = servers.ifEmpty { listOf("nats://localhost:4222") },
                authType = json.getString("authType", NatsAuthType.ANONYMOUS),
                username = json.getString("username"),
                password = json.getString("password"),
                token = json.getString("token"),
                tlsCaCertPath = json.getString("tlsCaCertPath"),
                tlsVerify = json.getBoolean("tlsVerify", true),
                connectTimeoutMs = json.getLong("connectTimeoutMs", 5000L),
                reconnectDelayMs = json.getLong("reconnectDelayMs", 5000L),
                maxReconnectAttempts = json.getInteger("maxReconnectAttempts", -1),
                useJetStream = json.getBoolean("useJetStream", false),
                streamName = json.getString("streamName"),
                consumerDurableName = json.getString("consumerDurableName"),
                addresses = addresses
            )
        }
    }

    fun toJsonObject(): JsonObject {
        val addressesArray = JsonArray()
        addresses.forEach { addressesArray.add(it.toJsonObject()) }
        return JsonObject()
            .put("servers", JsonArray(servers))
            .put("authType", authType)
            .put("username", username)
            .put("password", password)
            .put("token", token)
            .put("tlsCaCertPath", tlsCaCertPath)
            .put("tlsVerify", tlsVerify)
            .put("connectTimeoutMs", connectTimeoutMs)
            .put("reconnectDelayMs", reconnectDelayMs)
            .put("maxReconnectAttempts", maxReconnectAttempts)
            .put("useJetStream", useJetStream)
            .put("streamName", streamName)
            .put("consumerDurableName", consumerDurableName)
            .put("addresses", addressesArray)
    }

    fun validate(): List<String> {
        val errors = mutableListOf<String>()
        if (servers.isEmpty()) errors.add("at least one server URL must be specified")
        servers.forEach { s ->
            if (!s.startsWith("nats://") && !s.startsWith("nats+tls://") && !s.startsWith("tls://"))
                errors.add("server URL must start with 'nats://', 'nats+tls://', or 'tls://': $s")
        }
        if (authType !in listOf(NatsAuthType.ANONYMOUS, NatsAuthType.USERNAME_PASSWORD, NatsAuthType.TOKEN, NatsAuthType.TLS))
            errors.add("authType must be one of ANONYMOUS, USERNAME_PASSWORD, TOKEN, TLS")
        if (authType == NatsAuthType.USERNAME_PASSWORD && username.isNullOrBlank())
            errors.add("username is required for USERNAME_PASSWORD auth")
        if (authType == NatsAuthType.TOKEN && token.isNullOrBlank())
            errors.add("token is required for TOKEN auth")
        if (useJetStream && streamName.isNullOrBlank())
            errors.add("streamName is required when useJetStream is true")
        addresses.forEachIndexed { i, a -> errors.addAll(a.validate().map { "address[$i]: $it" }) }
        return errors
    }

    /**
     * Build a jnats [Options] object from this configuration.
     * Must not be called on the Vert.X event loop (connects are blocking).
     */
    fun buildOptions(): Options {
        val builder = Options.Builder()
            .servers(servers.toTypedArray())
            .connectionTimeout(Duration.ofMillis(connectTimeoutMs))
            .reconnectWait(Duration.ofMillis(reconnectDelayMs))
            .maxReconnects(maxReconnectAttempts)

        when (authType) {
            NatsAuthType.USERNAME_PASSWORD -> builder.userInfo(
                requireNotNull(username) { "username required" },
                requireNotNull(password) { "password required" }
            )
            NatsAuthType.TOKEN -> builder.token(
                requireNotNull(token) { "token required" }.toCharArray()
            )
            NatsAuthType.TLS -> {
                if (!tlsVerify) {
                    // Trust-all SSL context (dev/test only)
                    val ctx = SSLContext.getInstance("TLS")
                    ctx.init(null, arrayOf(object : javax.net.ssl.X509TrustManager {
                        override fun checkClientTrusted(certs: Array<java.security.cert.X509Certificate>, authType: String) {}
                        override fun checkServerTrusted(certs: Array<java.security.cert.X509Certificate>, authType: String) {}
                        override fun getAcceptedIssuers() = arrayOf<java.security.cert.X509Certificate>()
                    }), null)
                    builder.sslContext(ctx)
                } else if (!tlsCaCertPath.isNullOrBlank()) {
                    val ks = KeyStore.getInstance(KeyStore.getDefaultType())
                    FileInputStream(tlsCaCertPath).use { ks.load(it, null) }
                    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                    tmf.init(ks)
                    val ctx = SSLContext.getInstance("TLS")
                    ctx.init(null, tmf.trustManagers, null)
                    builder.sslContext(ctx)
                } else {
                    builder.opentls()  // Use default JVM trust store
                }
            }
            // ANONYMOUS: no auth options needed
        }

        return builder.build()
    }
}
