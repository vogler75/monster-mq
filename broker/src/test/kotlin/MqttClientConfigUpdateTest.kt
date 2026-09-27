package at.rocworks.graphql

import at.rocworks.extensions.graphql.redactTlsClientKeyPassword
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.MqttClientConnectionConfig
import io.vertx.core.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class MqttClientConfigUpdateTest {
    private fun config(
        cert: String? = "/client.pem",
        key: String? = "/client.key",
        format: String = MqttClientConnectionConfig.TLS_KEY_FORMAT_PEM
    ) = MqttClientConnectionConfig(
        brokerUrl = "ssl://broker.example.com:8883",
        clientId = "bridge",
        tlsCaCertPath = "/ca.pem",
        tlsClientCertPath = cert,
        tlsClientKeyPath = key,
        tlsClientKeyPassword = "secret",
        tlsClientKeyFormat = format,
        tlsAlpnProtocols = listOf("mqtt"),
        tlsServerName = "mqtt.example.com"
    )

    @Test
    fun legacyUpdatePreservesPkcs12AndOtherTlsSettings() {
        val existing = config(cert = "/client.p12", key = null, format = MqttClientConnectionConfig.TLS_KEY_FORMAT_PKCS12)
        // GraphQL Java inserts the PEM input default even when an older client sends no TLS fields.
        val incoming = MqttClientConnectionConfig(brokerUrl = existing.brokerUrl, clientId = "new-client-id")

        val merged = mergeMqttClientTlsForUpdate(existing, incoming, mapOf("brokerUrl" to existing.brokerUrl, "tlsClientKeyFormat" to "PEM"))

        assertEquals("new-client-id", merged.clientId)
        assertEquals("/ca.pem", merged.tlsCaCertPath)
        assertEquals("/client.p12", merged.tlsClientCertPath)
        assertNull(merged.tlsClientKeyPath)
        assertEquals("secret", merged.tlsClientKeyPassword)
        assertEquals("PKCS12", merged.tlsClientKeyFormat)
        assertEquals(listOf("mqtt"), merged.tlsAlpnProtocols)
        assertEquals("mqtt.example.com", merged.tlsServerName)
        assertTrue(merged.validate().isEmpty())
    }

    @Test
    fun explicitEmptyValuesClearTlsAndPassword() {
        val existing = config()
        val supplied = mapOf<String, Any?>(
            "tlsCaCertPath" to "",
            "tlsClientCertPath" to "",
            "tlsClientKeyPath" to "",
            "tlsClientKeyPassword" to "",
            "tlsAlpnProtocols" to emptyList<String>(),
            "tlsServerName" to ""
        )
        val incoming = MqttClientConnectionConfig.fromJsonObject(JsonObject()
            .put("brokerUrl", "tcp://broker.example.com:1883")
            .put("clientId", "bridge"))

        val merged = mergeMqttClientTlsForUpdate(existing, incoming, supplied)

        assertNull(merged.tlsCaCertPath)
        assertNull(merged.tlsClientCertPath)
        assertNull(merged.tlsClientKeyPath)
        assertNull(merged.tlsClientKeyPassword)
        assertNull(merged.tlsAlpnProtocols)
        assertNull(merged.tlsServerName)
        assertTrue(merged.validate().isEmpty())
    }

    @Test
    fun partialCertificateUpdateKeepsKeyAndCanClearPasswordSeparately() {
        val existing = config()
        val incoming = existing.copy(tlsClientCertPath = "/replacement.pem", tlsClientKeyPath = null, tlsClientKeyPassword = null)

        val merged = mergeMqttClientTlsForUpdate(existing, incoming, mapOf("tlsClientCertPath" to "/replacement.pem"))
        assertEquals("/replacement.pem", merged.tlsClientCertPath)
        assertEquals("/client.key", merged.tlsClientKeyPath)
        assertEquals("secret", merged.tlsClientKeyPassword)
        assertTrue(merged.validate().isEmpty())

        val clearedPassword = mergeMqttClientTlsForUpdate(existing, incoming, mapOf("tlsClientKeyPassword" to ""))
        assertNull(clearedPassword.tlsClientKeyPassword)
        assertEquals("/client.pem", clearedPassword.tlsClientCertPath)

        val removedCert = mergeMqttClientTlsForUpdate(existing,
            incoming.copy(tlsClientCertPath = null, tlsClientKeyPath = null),
            mapOf("tlsClientCertPath" to "", "tlsClientKeyPath" to ""))
        assertNull(removedCert.tlsClientKeyPassword)
    }

    @Test
    fun explicitPemKeySwitchesFromPkcs12() {
        val existing = config(cert = "/client.p12", key = null, format = MqttClientConnectionConfig.TLS_KEY_FORMAT_PKCS12)
        val incoming = existing.copy(tlsClientCertPath = "/client.pem", tlsClientKeyPath = "/client.key", tlsClientKeyFormat = "PEM")

        val merged = mergeMqttClientTlsForUpdate(existing, incoming,
            mapOf("tlsClientCertPath" to "/client.pem", "tlsClientKeyPath" to "/client.key", "tlsClientKeyFormat" to "PEM"))

        assertEquals("PEM", merged.tlsClientKeyFormat)
        assertEquals("/client.key", merged.tlsClientKeyPath)
        assertTrue(merged.validate().isEmpty())

        val missingPemKey = mergeMqttClientTlsForUpdate(existing, incoming.copy(tlsClientKeyPath = null),
            mapOf("tlsClientCertPath" to "/client.pem", "tlsClientKeyFormat" to "PEM"))
        assertEquals("PEM", missingPemKey.tlsClientKeyFormat)
        assertTrue(missingPemKey.validate().any { it.contains("tlsClientKeyPath is required") })
    }

    @Test
    fun genericExportRedactsKeyPasswordWithoutMutatingStoredConfig() {
        val stored = mutableMapOf<String, Any?>("tlsClientKeyPassword" to "secret", "brokerUrl" to "ssl://broker.example.com:8883")
        val device = mapOf<String, Any?>("type" to DeviceConfig.DEVICE_TYPE_MQTT_CLIENT, "config" to stored)

        val exported = redactTlsClientKeyPassword(device)
        val config = exported["config"] as Map<*, *>

        assertFalse(config.containsKey("tlsClientKeyPassword"))
        assertEquals("ssl://broker.example.com:8883", config["brokerUrl"])
        assertEquals("secret", stored["tlsClientKeyPassword"])

        val jsonExport = redactTlsClientKeyPassword(device + ("config" to JsonObject(stored)))
        assertFalse((jsonExport["config"] as Map<*, *>).containsKey("tlsClientKeyPassword"))
        val stringExport = redactTlsClientKeyPassword(device + ("config" to JsonObject(stored).encode()))
        assertFalse((stringExport["config"] as Map<*, *>).containsKey("tlsClientKeyPassword"))
        val malformedExport = redactTlsClientKeyPassword(device + ("config" to "{bad:tlsClientKeyPassword}"))
        assertNull(malformedExport["config"])

        val wrongType = redactTlsClientKeyPassword(device + ("type" to "MISTYPED-CLIENT"))
        assertFalse((wrongType["config"] as Map<*, *>).containsKey("tlsClientKeyPassword"))
    }
}
