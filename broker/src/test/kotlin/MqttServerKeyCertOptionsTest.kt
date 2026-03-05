package at.rocworks

import io.vertx.core.net.JksOptions
import io.vertx.core.net.PfxOptions
import org.junit.Assert.assertTrue
import org.junit.Test

class MqttServerKeyCertOptionsTest {

    @Test
    fun `JKS type returns JksOptions`() {
        val options = MqttServer.buildKeyCertOptions("keystore.jks", "password", "JKS")
        assertTrue(options is JksOptions)
    }

    @Test
    fun `default empty type returns JksOptions`() {
        val options = MqttServer.buildKeyCertOptions("keystore.jks", "password", "")
        assertTrue(options is JksOptions)
    }

    @Test
    fun `unknown type falls back to JksOptions`() {
        val options = MqttServer.buildKeyCertOptions("keystore.jks", "password", "UNKNOWN")
        assertTrue(options is JksOptions)
    }

    @Test
    fun `PKCS12 type returns PfxOptions`() {
        val options = MqttServer.buildKeyCertOptions("keystore.p12", "changeit", "PKCS12")
        assertTrue(options is PfxOptions)
    }

    @Test
    fun `PFX type returns PfxOptions`() {
        val options = MqttServer.buildKeyCertOptions("keystore.pfx", "changeit", "PFX")
        assertTrue(options is PfxOptions)
    }

    @Test
    fun `P12 type returns PfxOptions`() {
        val options = MqttServer.buildKeyCertOptions("keystore.p12", "changeit", "P12")
        assertTrue(options is PfxOptions)
    }

    @Test
    fun `type matching is case-insensitive`() {
        assertTrue(MqttServer.buildKeyCertOptions("k.p12", "pw", "pkcs12") is PfxOptions)
        assertTrue(MqttServer.buildKeyCertOptions("k.pfx", "pw", "pfx") is PfxOptions)
        assertTrue(MqttServer.buildKeyCertOptions("k.p12", "pw", "p12") is PfxOptions)
        assertTrue(MqttServer.buildKeyCertOptions("k.jks", "pw", "jks") is JksOptions)
    }

    @Test
    fun `path and password are set correctly for JKS`() {
        val options = MqttServer.buildKeyCertOptions("my/keystore.jks", "secret", "JKS") as JksOptions
        assertTrue(options.path == "my/keystore.jks")
        assertTrue(options.password == "secret")
    }

    @Test
    fun `path and password are set correctly for PKCS12`() {
        val options = MqttServer.buildKeyCertOptions("my/keystore.p12", "topsecret", "PKCS12") as PfxOptions
        assertTrue(options.path == "my/keystore.p12")
        assertTrue(options.password == "topsecret")
    }
}
