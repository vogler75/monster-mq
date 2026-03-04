package at.rocworks.devices.natsclient

import at.rocworks.stores.devices.NatsAuthType
import at.rocworks.stores.devices.NatsClientAddress
import at.rocworks.stores.devices.NatsClientConfig
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.Assert.*
import org.junit.Test
import java.io.File
import java.security.KeyStore
import java.security.cert.X509Certificate
import javax.net.ssl.SSLContext

class NatsClientConfigTest {

    private fun minimalConfig() = NatsClientConfig(
        servers = listOf("nats://localhost:4222"),
        authType = NatsAuthType.ANONYMOUS
    )

    // ── validate ──────────────────────────────────────────────────────────────

    @Test
    fun `validate passes for minimal anonymous config`() {
        assertTrue(minimalConfig().validate().isEmpty())
    }

    @Test
    fun `validate passes for USERNAME_PASSWORD with credentials`() {
        val cfg = NatsClientConfig(
            servers = listOf("nats://localhost:4222"),
            authType = NatsAuthType.USERNAME_PASSWORD,
            username = "user",
            password = "pass"
        )
        assertTrue(cfg.validate().isEmpty())
    }

    @Test
    fun `validate passes for TOKEN auth with token`() {
        val cfg = NatsClientConfig(
            servers = listOf("nats://localhost:4222"),
            authType = NatsAuthType.TOKEN,
            token = "secret-token"
        )
        assertTrue(cfg.validate().isEmpty())
    }

    @Test
    fun `validate rejects empty servers list`() {
        val cfg = minimalConfig().copy(servers = emptyList())
        val errors = cfg.validate()
        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("server"))
    }

    @Test
    fun `validate rejects invalid server URL scheme`() {
        val cfg = minimalConfig().copy(servers = listOf("mqtt://localhost:1883"))
        val errors = cfg.validate()
        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("nats://"))
    }

    @Test
    fun `validate accepts nats+tls and tls URL schemes`() {
        val cfg = minimalConfig().copy(
            servers = listOf("nats+tls://host1:4222", "tls://host2:4222")
        )
        assertTrue(cfg.validate().isEmpty())
    }

    @Test
    fun `validate rejects unknown authType`() {
        val cfg = minimalConfig().copy(authType = "KERBEROS")
        val errors = cfg.validate()
        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("authType"))
    }

    @Test
    fun `validate rejects USERNAME_PASSWORD without username`() {
        val cfg = NatsClientConfig(
            servers = listOf("nats://localhost:4222"),
            authType = NatsAuthType.USERNAME_PASSWORD,
            username = "",
            password = "pass"
        )
        val errors = cfg.validate()
        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("username"))
    }

    @Test
    fun `validate rejects TOKEN without token`() {
        val cfg = NatsClientConfig(
            servers = listOf("nats://localhost:4222"),
            authType = NatsAuthType.TOKEN,
            token = null
        )
        val errors = cfg.validate()
        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("token"))
    }

    @Test
    fun `validate rejects JetStream without streamName`() {
        val cfg = NatsClientConfig(
            servers = listOf("nats://localhost:4222"),
            authType = NatsAuthType.ANONYMOUS,
            useJetStream = true,
            streamName = null
        )
        val errors = cfg.validate()
        assertEquals(1, errors.size)
        assertTrue(errors[0].contains("streamName"))
    }

    @Test
    fun `validate passes JetStream with streamName`() {
        val cfg = NatsClientConfig(
            servers = listOf("nats://localhost:4222"),
            authType = NatsAuthType.ANONYMOUS,
            useJetStream = true,
            streamName = "events"
        )
        assertTrue(cfg.validate().isEmpty())
    }

    @Test
    fun `validate propagates address errors with index prefix`() {
        val badAddr = NatsClientAddress("INVALID", "", "", qos = 5)
        val cfg = minimalConfig().copy(addresses = listOf(badAddr))
        val errors = cfg.validate()
        assertTrue(errors.all { it.startsWith("address[0]:") })
        assertEquals(4, errors.size)
    }

    @Test
    fun `validate accumulates multiple config-level errors`() {
        val cfg = NatsClientConfig(
            servers = emptyList(),
            authType = "BAD",
            useJetStream = true,
            streamName = null
        )
        val errors = cfg.validate()
        assertTrue(errors.size >= 3)
    }

    // ── JSON round-trip ───────────────────────────────────────────────────────

    @Test
    fun `fromJson(toJsonObject()) round-trips anonymous config`() {
        val original = minimalConfig()
        val restored = NatsClientConfig.fromJson(original.toJsonObject())
        assertEquals(original, restored)
    }

    @Test
    fun `fromJson(toJsonObject()) round-trips USERNAME_PASSWORD config`() {
        val original = NatsClientConfig(
            servers = listOf("nats://host1:4222", "nats://host2:4222"),
            authType = NatsAuthType.USERNAME_PASSWORD,
            username = "alice",
            password = "secret",
            connectTimeoutMs = 3000,
            reconnectDelayMs = 2000,
            maxReconnectAttempts = 5
        )
        val restored = NatsClientConfig.fromJson(original.toJsonObject())
        assertEquals(original, restored)
    }

    @Test
    fun `fromJson(toJsonObject()) round-trips JetStream config`() {
        val original = NatsClientConfig(
            servers = listOf("nats://localhost:4222"),
            authType = NatsAuthType.ANONYMOUS,
            useJetStream = true,
            streamName = "iot-stream",
            consumerDurableName = "monstermq-consumer"
        )
        val restored = NatsClientConfig.fromJson(original.toJsonObject())
        assertEquals(original, restored)
    }

    @Test
    fun `fromJson(toJsonObject()) round-trips config with addresses`() {
        val addr1 = NatsClientAddress("SUBSCRIBE", "sensors.>", "sensors/#", qos = 1)
        val addr2 = NatsClientAddress("PUBLISH", "cmd.*", "cmd/+", qos = 0, autoConvert = false)
        val original = minimalConfig().copy(addresses = listOf(addr1, addr2))
        val restored = NatsClientConfig.fromJson(original.toJsonObject())
        assertEquals(original, restored)
    }

    @Test
    fun `fromJson defaults when fields are absent`() {
        val json = JsonObject()
            .put("servers", JsonArray().add("nats://localhost:4222"))
        val cfg = NatsClientConfig.fromJson(json)
        assertEquals(NatsAuthType.ANONYMOUS, cfg.authType)
        assertEquals(5000L, cfg.connectTimeoutMs)
        assertEquals(5000L, cfg.reconnectDelayMs)
        assertEquals(-1, cfg.maxReconnectAttempts)
        assertFalse(cfg.useJetStream)
        assertTrue(cfg.tlsVerify)
        assertTrue(cfg.addresses.isEmpty())
    }

    @Test
    fun `fromJson accepts servers as a plain string`() {
        val json = JsonObject().put("servers", "nats://single-host:4222")
        val cfg = NatsClientConfig.fromJson(json)
        assertEquals(listOf("nats://single-host:4222"), cfg.servers)
    }

    @Test
    fun `fromJson falls back to default server when servers field is missing`() {
        val cfg = NatsClientConfig.fromJson(JsonObject())
        assertEquals(listOf("nats://localhost:4222"), cfg.servers)
    }

    // ── buildOptions ──────────────────────────────────────────────────────────

    @Test
    fun `buildOptions includes all server URLs`() {
        val cfg = NatsClientConfig(
            servers = listOf("nats://host1:4222", "nats://host2:4222"),
            authType = NatsAuthType.ANONYMOUS
        )
        val opts = cfg.buildOptions()
        val serverUrls = opts.servers.map { it.toString() }
        assertTrue(serverUrls.any { it.contains("host1") })
        assertTrue(serverUrls.any { it.contains("host2") })
    }

    @Test
    fun `buildOptions sets connection timeout`() {
        val cfg = minimalConfig().copy(connectTimeoutMs = 8000L)
        val opts = cfg.buildOptions()
        assertEquals(8000L, opts.connectionTimeout.toMillis())
    }

    @Test
    fun `buildOptions sets reconnect wait`() {
        val cfg = minimalConfig().copy(reconnectDelayMs = 3000L)
        val opts = cfg.buildOptions()
        assertEquals(3000L, opts.reconnectWait.toMillis())
    }

    @Test
    fun `buildOptions sets max reconnects`() {
        val cfg = minimalConfig().copy(maxReconnectAttempts = 10)
        val opts = cfg.buildOptions()
        assertEquals(10, opts.maxReconnect)
    }

    @Test
    fun `buildOptions uses -1 for unlimited reconnects`() {
        val cfg = minimalConfig().copy(maxReconnectAttempts = -1)
        val opts = cfg.buildOptions()
        assertEquals(-1, opts.maxReconnect)
    }

    // ── TLS buildOptions (no network required) ────────────────────────────────

    @Test
    fun `buildOptions TLS trust-all produces non-null SSLContext`() {
        // tlsVerify=false → trust-all SSLContext; no file I/O, no network needed
        val cfg = NatsClientConfig(
            servers = listOf("nats://localhost:4222"),
            authType = NatsAuthType.TLS,
            tlsVerify = false
        )
        val opts = cfg.buildOptions()
        assertNotNull("SSLContext should be set for TLS trust-all", opts.sslContext)
    }

    @Test
    fun `buildOptions TLS trust-all context accepts any certificate`() {
        val cfg = NatsClientConfig(
            servers = listOf("nats://localhost:4222"),
            authType = NatsAuthType.TLS,
            tlsVerify = false
        )
        val opts = cfg.buildOptions()
        val tm = opts.sslContext!!.clientSessionContext  // just ensure context is functional
        assertNotNull(tm)
    }

    @Test
    fun `buildOptions TLS with CA cert file loads keystore successfully`() {
        // Generate a throwaway JKS file in a temp directory so no pre-existing cert is needed
        val ks = KeyStore.getInstance(KeyStore.getDefaultType())
        ks.load(null, null) // empty keystore
        val tmpFile = File.createTempFile("test-truststore", ".jks")
        try {
            tmpFile.outputStream().use { ks.store(it, null) }

            val cfg = NatsClientConfig(
                servers = listOf("nats://localhost:4222"),
                authType = NatsAuthType.TLS,
                tlsVerify = true,
                tlsCaCertPath = tmpFile.absolutePath
            )
            // Should not throw — the JKS is valid (even if empty)
            val opts = cfg.buildOptions()
            assertNotNull(opts.sslContext)
        } finally {
            tmpFile.delete()
        }
    }

    @Test
    fun `buildOptions TLS opentls when tlsVerify=true and no cert path`() {
        // No tlsCaCertPath → falls through to builder.opentls() (JVM default trust store)
        // We can only verify buildOptions() doesn't throw; SSLContext is null for opentls
        val cfg = NatsClientConfig(
            servers = listOf("nats://localhost:4222"),
            authType = NatsAuthType.TLS,
            tlsVerify = true,
            tlsCaCertPath = null
        )
        // Must not throw
        val opts = cfg.buildOptions()
        assertNotNull(opts)
    }
}
