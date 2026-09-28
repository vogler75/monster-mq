package at.rocworks

import at.rocworks.devices.opcuaserver.OpcUaServerSecurity
import io.vertx.core.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class OpcUaServerSecurityTest {

    private val existing = OpcUaServerSecurity(
        keystorePassword = "secret",
        securityPolicies = listOf("Basic256Sha256"),
        allowAnonymous = false,
        allowUnencrypted = false,
        certificateDir = "/data/opcua",
        createSelfSigned = false
    )

    @Test
    fun testJsonRoundTrip() {
        val restored = OpcUaServerSecurity.fromJsonObject(existing.toJsonObject())
        assertEquals(existing, restored)
        assertFalse(existing.toJsonObject().containsKey("keystorePath"))
        assertFalse(existing.toJsonObject().containsKey("certificateAlias"))
    }

    @Test
    fun testLegacyRequireAuthenticationDisablesAnonymous() {
        val legacy = JsonObject()
            .put("keystorePath", "server-keystore.jks")
            .put("certificateAlias", "server-cert")
            .put("allowAnonymous", true)
            .put("requireAuthentication", true)
        assertFalse(OpcUaServerSecurity.fromJsonObject(legacy).allowAnonymous)
        assertTrue(OpcUaServerSecurity.fromJsonObject(JsonObject()).allowAnonymous)
    }

    @Test
    fun testPartialInputKeepsExistingValues() {
        val merged = OpcUaServerSecurity.fromInput(mapOf("allowAnonymous" to true), existing)
        assertEquals(existing.copy(allowAnonymous = true), merged)
    }

    @Test
    fun testBlankPasswordKeepsExistingPassword() {
        val merged = OpcUaServerSecurity.fromInput(mapOf("keystorePassword" to ""), existing)
        assertEquals("secret", merged.keystorePassword)
        val changed = OpcUaServerSecurity.fromInput(mapOf("keystorePassword" to "new"), existing)
        assertEquals("new", changed.keystorePassword)
    }

    @Test
    fun testDeprecatedInputFields() {
        val merged = OpcUaServerSecurity.fromInput(
            mapOf("keystorePath" to "other.jks", "certificateAlias" to "x", "requireAuthentication" to true),
            OpcUaServerSecurity()
        )
        assertEquals(OpcUaServerSecurity().copy(allowAnonymous = false), merged)
    }

    @Test
    fun testValidation() {
        assertTrue(OpcUaServerSecurity().validate().isEmpty())
        assertTrue(existing.validate().isEmpty())
        assertFalse(OpcUaServerSecurity(securityPolicies = emptyList()).validate().isEmpty())
        assertFalse(OpcUaServerSecurity(securityPolicies = listOf("Basic256")).validate().isEmpty())
        assertFalse(OpcUaServerSecurity(securityPolicies = listOf("None"), allowUnencrypted = false).validate().isEmpty())
    }
}
