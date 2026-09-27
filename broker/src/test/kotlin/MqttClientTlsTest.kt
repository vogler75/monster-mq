package at.rocworks

import at.rocworks.devices.mqttclient.MqttClientTls
import at.rocworks.devices.mqttclient.MqttClientTlsException
import at.rocworks.stores.devices.MqttClientConnectionConfig
import io.vertx.core.json.JsonObject
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.BasicConstraints
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralNames
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.openssl.PKCS8Generator
import org.bouncycastle.openssl.jcajce.JcaPEMWriter
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.eclipse.paho.client.mqttv3.MqttAsyncClient
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.bouncycastle.util.io.pem.PemObject
import org.junit.AfterClass
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.BeforeClass
import org.junit.Test
import java.io.DataInputStream
import java.io.File
import java.io.FileWriter
import java.math.BigInteger
import java.nio.file.Files
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.KeyStore
import java.security.cert.X509Certificate
import java.util.Date
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import javax.net.ssl.ExtendedSSLSession
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SNIHostName
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLServerSocket
import javax.net.ssl.SSLSocket
import javax.net.ssl.TrustManagerFactory
import kotlin.concurrent.thread

class MqttClientTlsTest {
    companion object {
        private val bc = BouncyCastleProvider()
        private lateinit var dir: File
        private lateinit var caKey: KeyPair
        private lateinit var caCert: X509Certificate
        private lateinit var serverKey: KeyPair
        private lateinit var serverCert: X509Certificate
        private lateinit var clientKey: KeyPair
        private lateinit var clientCert: X509Certificate

        @BeforeClass
        @JvmStatic
        fun setup() {
            dir = Files.createTempDirectory("mqtt-client-tls").toFile()
            caKey = rsaKeyPair()
            caCert = issue("CN=Test CA", caKey, "CN=Test CA", caKey, ca = true)
            serverKey = rsaKeyPair()
            // Deliberately no "localhost" SAN: connecting to localhost only verifies via tlsServerName
            serverCert = issue("CN=mqtt.test", serverKey, "CN=Test CA", caKey, dnsName = "mqtt.test")
            clientKey = rsaKeyPair()
            clientCert = issue("CN=bridge-client", clientKey, "CN=Test CA", caKey)

            writePem("ca.pem", caCert)
            writePem("client.pem", clientCert)
            writePem("client-pkcs1.key", clientKey.private)
            writePem("client-pkcs8.key", JcaPKCS8Generator(clientKey.private, null))
            writePem(
                "client-pkcs8-enc.key",
                JcaPKCS8Generator(
                    clientKey.private,
                    JceOpenSSLPKCS8EncryptorBuilder(PKCS8Generator.AES_256_CBC).setProvider(bc)
                        .setPassword("secret".toCharArray()).build()
                )
            )
            JcaPEMWriter(FileWriter(File(dir, "client-legacy-enc.key"))).use {
                it.writeObject(clientKey.private, JcePEMEncryptorBuilder("AES-128-CBC").setProvider(bc).build("secret".toCharArray()))
            }
            KeyStore.getInstance("PKCS12").apply {
                load(null, null)
                setKeyEntry("client", clientKey.private, "secret".toCharArray(), arrayOf(clientCert, caCert))
                File(dir, "client.p12").outputStream().use { store(it, "secret".toCharArray()) }
            }

            val ecKey = KeyPairGenerator.getInstance("EC").apply { initialize(256) }.generateKeyPair()
            writePem("client-ec.pem", issue("CN=ec-client", ecKey, "CN=Test CA", caKey))
            // SEC1 "EC PRIVATE KEY" as OpenSSL writes it, with the optional public key omitted
            val ecInfo = PrivateKeyInfo.getInstance(ecKey.private.encoded)
            val sec1 = org.bouncycastle.asn1.sec.ECPrivateKey(
                256, (ecKey.private as java.security.interfaces.ECPrivateKey).s, ecInfo.privateKeyAlgorithm.parameters
            )
            writePem("client-ec.key", PemObject("EC PRIVATE KEY", sec1.encoded))
            writePem("other.key", rsaKeyPair().private)
        }

        @AfterClass
        @JvmStatic
        fun cleanup() {
            dir.deleteRecursively()
        }

        private fun rsaKeyPair(): KeyPair = KeyPairGenerator.getInstance("RSA").apply { initialize(2048) }.generateKeyPair()

        private fun issue(
            subject: String, subjectKey: KeyPair, issuer: String, issuerKey: KeyPair,
            ca: Boolean = false, dnsName: String? = null
        ): X509Certificate {
            val now = System.currentTimeMillis()
            val builder = JcaX509v3CertificateBuilder(
                X500Name(issuer), BigInteger.valueOf(now + subject.hashCode()), Date(now - 60_000), Date(now + 86_400_000),
                X500Name(subject), subjectKey.public
            )
            builder.addExtension(Extension.basicConstraints, true, BasicConstraints(ca))
            dnsName?.let {
                builder.addExtension(Extension.subjectAlternativeName, false, GeneralNames(GeneralName(GeneralName.dNSName, it)))
            }
            val signer = JcaContentSignerBuilder("SHA256withRSA").build(issuerKey.private)
            return JcaX509CertificateConverter().getCertificate(builder.build(signer))
        }

        private fun writePem(name: String, obj: Any) {
            JcaPEMWriter(FileWriter(File(dir, name))).use { it.writeObject(obj) }
        }

        private fun path(name: String) = File(dir, name).absolutePath
    }

    private fun config(
        url: String = "ssl://localhost:8883",
        verify: Boolean = true,
        ca: String? = "ca.pem",
        cert: String? = "client.pem",
        key: String? = "client-pkcs8.key",
        password: String? = null,
        format: String = MqttClientConnectionConfig.TLS_KEY_FORMAT_PEM,
        alpn: List<String>? = null,
        serverName: String? = "mqtt.test"
    ) = MqttClientConnectionConfig(
        brokerUrl = url,
        clientId = "test",
        sslVerifyCertificate = verify,
        tlsCaCertPath = ca?.let { path(it) },
        tlsClientCertPath = cert?.let { path(it) },
        tlsClientKeyPath = key?.let { path(it) },
        tlsClientKeyPassword = password,
        tlsClientKeyFormat = format,
        tlsAlpnProtocols = alpn,
        tlsServerName = serverName
    )

    private fun assertTlsError(expected: String, block: () -> Unit) {
        try {
            block()
            fail("expected MqttClientTlsException containing '$expected'")
        } catch (e: MqttClientTlsException) {
            assertTrue("'${e.message}' should contain '$expected'", e.message!!.contains(expected))
        }
    }

    // --- validation and JSON ---

    @Test
    fun defaultConfigNeedsNoCustomFactory() {
        val cfg = MqttClientConnectionConfig(brokerUrl = "ssl://broker:8883", clientId = "c")
        assertFalse(cfg.hasTlsOptions())
        assertTrue(cfg.validate().isEmpty())
        assertNull(MqttClientTls.buildSocketFactory(cfg))
        assertFalse(cfg.toJsonObject().containsKey("tlsClientKeyFormat"))
    }

    @Test
    fun jsonRoundTrip() {
        val cfg = config(password = "secret", alpn = listOf("x-amzn-mqtt-ca"))
        val back = MqttClientConnectionConfig.fromJsonObject(cfg.toJsonObject())
        assertEquals(cfg, back)
    }

    @Test
    fun jsonWithoutTlsFieldsStillParses() {
        val cfg = MqttClientConnectionConfig.fromJsonObject(
            JsonObject().put("brokerUrl", "ssl://broker:8883").put("clientId", "c").put("sslVerifyCertificate", false)
        )
        assertNull(cfg.tlsClientCertPath)
        assertEquals(MqttClientConnectionConfig.TLS_KEY_FORMAT_PEM, cfg.tlsClientKeyFormat)
        assertTrue(cfg.validate().isEmpty())
    }

    @Test
    fun validationRules() {
        assertTrue(config().validate().isEmpty())
        assertTrue(config(key = null).validate().any { it.contains("tlsClientKeyPath is required") })
        assertTrue(config(cert = null).validate().any { it.contains("tlsClientCertPath is required") })
        assertTrue(config(format = "JKS").validate().any { it.contains("tlsClientKeyFormat") })
        assertTrue(config(format = "PKCS12", cert = null, key = null).validate().any { it.contains("PKCS12") })
        assertTrue(config(format = "PKCS12", cert = "client.p12").validate().any { it.contains("not used") })
        assertTrue(config(format = "PKCS12", cert = "client.p12", key = null).validate().isEmpty())
        assertTrue(config(alpn = listOf("")).validate().any { it.contains("tlsAlpnProtocols") })
        assertTrue(config(alpn = listOf("a".repeat(256))).validate().any { it.contains("tlsAlpnProtocols") })
        assertTrue(config(url = "tcp://localhost:1883").validate().any { it.contains("ssl:// or wss://") })
        assertTrue(config(url = "wss://localhost:443/mqtt").validate().isEmpty())
        // A stored key password alone must not block switching a bridge to plain TCP
        assertTrue(config(url = "tcp://localhost:1883", ca = null, cert = null, key = null, serverName = null, password = "x")
            .validate().isEmpty())
    }

    // --- key material loading ---

    @Test
    fun loadsSupportedKeyFormats() {
        assertNotNull(MqttClientTls.buildSocketFactory(config(key = "client-pkcs1.key")))
        assertNotNull(MqttClientTls.buildSocketFactory(config(key = "client-pkcs8.key")))
        assertNotNull(MqttClientTls.buildSocketFactory(config(key = "client-pkcs8-enc.key", password = "secret")))
        assertNotNull(MqttClientTls.buildSocketFactory(config(key = "client-legacy-enc.key", password = "secret")))
        assertNotNull(MqttClientTls.buildSocketFactory(config(cert = "client-ec.pem", key = "client-ec.key")))
        assertNotNull(MqttClientTls.buildSocketFactory(config(format = "PKCS12", cert = "client.p12", key = null, password = "secret")))
    }

    @Test
    fun reportsActionableErrors() {
        assertTlsError("tlsCaCertPath") { MqttClientTls.buildSocketFactory(config(ca = "missing.pem")) }
        assertTlsError("file not found") { MqttClientTls.buildSocketFactory(config(key = "missing.key")) }
        assertTlsError("no tlsClientKeyPassword") { MqttClientTls.buildSocketFactory(config(key = "client-pkcs8-enc.key")) }
        assertTlsError("wrong tlsClientKeyPassword") {
            MqttClientTls.buildSocketFactory(config(key = "client-pkcs8-enc.key", password = "wrong"))
        }
        assertTlsError("wrong tlsClientKeyPassword") {
            MqttClientTls.buildSocketFactory(config(format = "PKCS12", cert = "client.p12", key = null, password = "wrong"))
        }
        assertTlsError("does not match") { MqttClientTls.buildSocketFactory(config(key = "other.key")) }
        assertTlsError("no PEM private key") { MqttClientTls.buildSocketFactory(config(key = "client.pem")) }
        assertTlsError("tlsServerName") { MqttClientTls.buildSocketFactory(config(serverName = "bad name!")) }
    }

    // --- end-to-end through Paho against a minimal mTLS MQTT endpoint ---

    private class HandshakeInfo(val sni: String?, val alpn: String?, val clientSubject: String?)

    /**
     * Accepts one TLS connection requiring a client certificate, answers the MQTT CONNECT with a
     * CONNACK and reports what the client presented.
     */
    private fun startFakeBroker(alpn: String?): Pair<Int, CompletableFuture<HandshakeInfo>> {
        val keyStore = KeyStore.getInstance("PKCS12").apply {
            load(null, null)
            setKeyEntry("server", serverKey.private, CharArray(0), arrayOf(serverCert, caCert))
        }
        val trustStore = KeyStore.getInstance("PKCS12").apply { load(null, null); setCertificateEntry("ca", caCert) }
        val ctx = SSLContext.getInstance("TLS")
        ctx.init(
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()).apply { init(keyStore, CharArray(0)) }.keyManagers,
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()).apply { init(trustStore) }.trustManagers,
            null
        )
        val server = ctx.serverSocketFactory.createServerSocket(0) as SSLServerSocket
        server.needClientAuth = true
        alpn?.let { server.sslParameters = server.sslParameters.apply { applicationProtocols = arrayOf(it) } }

        val result = CompletableFuture<HandshakeInfo>()
        thread(isDaemon = true) {
            server.use {
                try {
                    val socket = server.accept() as SSLSocket
                    socket.soTimeout = 10_000
                    socket.startHandshake()
                    val session = socket.session as ExtendedSSLSession
                    val info = HandshakeInfo(
                        (session.requestedServerNames.firstOrNull() as? SNIHostName)?.asciiName,
                        socket.applicationProtocol?.ifEmpty { null },
                        (session.peerCertificates.first() as X509Certificate).subjectX500Principal.name
                    )
                    val input = DataInputStream(socket.inputStream)
                    input.readUnsignedByte() // CONNECT fixed header
                    var remaining = 0
                    var multiplier = 1
                    do {
                        val b = input.readUnsignedByte()
                        remaining += (b and 0x7F) * multiplier
                        multiplier *= 128
                    } while (b and 0x80 != 0)
                    input.skipNBytes(remaining.toLong())
                    socket.outputStream.write(byteArrayOf(0x20, 0x02, 0x00, 0x00))
                    socket.outputStream.flush()
                    result.complete(info)
                    runCatching { while (input.read() >= 0) { } }
                    socket.close()
                } catch (e: Exception) {
                    result.completeExceptionally(e)
                }
            }
        }
        return server.localPort to result
    }

    private fun connectWithPaho(port: Int, cfg: MqttClientConnectionConfig) {
        val client = MqttAsyncClient("ssl://localhost:$port", "tls-test", MemoryPersistence())
        val options = MqttConnectOptions().apply {
            connectionTimeout = 5
            socketFactory = MqttClientTls.buildSocketFactory(cfg)
        }
        try {
            client.connect(options).waitForCompletion(10_000)
            assertTrue(client.isConnected)
            client.disconnect().waitForCompletion(5_000)
        } finally {
            client.close()
        }
    }

    @Test
    fun mutualTlsWithAlpnAndSniThroughPaho() {
        val (port, handshake) = startFakeBroker(alpn = "x-amzn-mqtt-ca")
        connectWithPaho(port, config(url = "ssl://localhost:$port", alpn = listOf("x-amzn-mqtt-ca")))
        val info = handshake.get(10, TimeUnit.SECONDS)
        assertEquals("mqtt.test", info.sni)
        assertEquals("x-amzn-mqtt-ca", info.alpn)
        assertEquals("CN=bridge-client", info.clientSubject)
    }

    @Test
    fun pkcs12ClientCertificateThroughPaho() {
        val (port, handshake) = startFakeBroker(alpn = null)
        connectWithPaho(port, config(url = "ssl://localhost:$port", format = "PKCS12", cert = "client.p12", key = null, password = "secret"))
        assertEquals("CN=bridge-client", handshake.get(10, TimeUnit.SECONDS).clientSubject)
    }

    @Test
    fun hostnameIsVerifiedWithoutServerNameOverride() {
        val (port, _) = startFakeBroker(alpn = null)
        try {
            connectWithPaho(port, config(url = "ssl://localhost:$port", serverName = null))
            fail("connection must fail: server certificate is not valid for localhost")
        } catch (e: org.eclipse.paho.client.mqttv3.MqttException) {
            // expected
        }
    }

    @Test
    fun insecureModeStillPresentsClientCertificate() {
        val (port, handshake) = startFakeBroker(alpn = null)
        connectWithPaho(port, config(url = "ssl://localhost:$port", verify = false, ca = null, serverName = null))
        assertEquals("CN=bridge-client", handshake.get(10, TimeUnit.SECONDS).clientSubject)
    }
}
