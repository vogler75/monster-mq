package at.rocworks.peerlink.tls

import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.BasicConstraints
import org.bouncycastle.asn1.x509.ExtendedKeyUsage
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralNames
import org.bouncycastle.asn1.x509.KeyPurposeId
import org.bouncycastle.asn1.x509.KeyUsage
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import org.junit.Assert.*
import org.junit.Test
import java.io.File
import java.math.BigInteger
import java.net.ServerSocket
import java.net.Socket
import java.nio.file.Files
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.SSLSocket

class PeerTlsTest {

    private fun generateCert(
        keyPair: KeyPair,
        subjectCn: String,
        signerKey: KeyPair? = null,
        signerCert: X509Certificate? = null,
        isCa: Boolean = false,
        uris: List<String> = emptyList(),
        dnsNames: List<String> = emptyList(),
        usages: List<KeyPurposeId>? = listOf(KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth),
        notBefore: Date = Date(System.currentTimeMillis() - 3600_000L),
        notAfter: Date = Date(System.currentTimeMillis() + 86400_000L * 365)
    ): X509Certificate {
        val subject = X500Name("CN=$subjectCn, O=MonsterMQ")
        val issuer = if (signerCert != null) X500Name.getInstance(signerCert.subjectX500Principal.encoded) else subject
        val serial = BigInteger(128, SecureRandom())
        val builder = JcaX509v3CertificateBuilder(
            issuer,
            serial,
            notBefore,
            notAfter,
            subject,
            keyPair.public
        )

        if (isCa) {
            builder.addExtension(Extension.basicConstraints, true, BasicConstraints(true))
            builder.addExtension(Extension.keyUsage, true, KeyUsage(KeyUsage.keyCertSign or KeyUsage.cRLSign))
        } else {
            builder.addExtension(Extension.basicConstraints, true, BasicConstraints(false))
            builder.addExtension(Extension.keyUsage, true, KeyUsage(KeyUsage.digitalSignature))
        }

        if (usages != null) {
            val eku = ExtendedKeyUsage(usages.toTypedArray())
            builder.addExtension(Extension.extendedKeyUsage, false, eku)
        }

        val sanList = mutableListOf<GeneralName>()
        for (u in uris) {
            sanList.add(GeneralName(GeneralName.uniformResourceIdentifier, u))
        }
        for (d in dnsNames) {
            sanList.add(GeneralName(GeneralName.dNSName, d))
        }
        if (sanList.isNotEmpty()) {
            builder.addExtension(Extension.subjectAlternativeName, false, GeneralNames(sanList.toTypedArray()))
        }

        val signer = JcaContentSignerBuilder("SHA256withECDSA").build(signerKey?.private ?: keyPair.private)
        return JcaX509CertificateConverter().getCertificate(builder.build(signer))
    }

    private fun genEC(): KeyPair {
        val kpg = KeyPairGenerator.getInstance("EC")
        kpg.initialize(256, SecureRandom())
        return kpg.generateKeyPair()
    }

    @Test
    fun testParsePins() {
        val hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        val p1 = parsePin(hex)
        assertEquals(hex, p1.toString())

        val formatted = "01:23:45:67:89:ab:cd:ef:01:23:45:67:89:ab:cd:ef:01:23:45:67:89:ab:cd:ef:01:23:45:67:89:ab:cd:ef"
        val p2 = parsePin(formatted)
        assertEquals(p1, p2)

        try {
            parsePin("0123")
            fail("Expected IllegalArgumentException for short pin")
        } catch (_: IllegalArgumentException) {}
    }

    @Test
    fun testDecodeSecrets() {
        val raw = ByteArray(32) { it.toByte() }
        val stdB64 = Base64.getEncoder().encodeToString(raw)
        val urlB64 = Base64.getUrlEncoder().encodeToString(raw)

        assertArrayEquals(raw, decodeSecret(stdB64))
        assertArrayEquals(raw, decodeSecret(urlB64))

        // Short secret
        val shortB64 = Base64.getEncoder().encodeToString(ByteArray(10))
        try {
            decodeSecret(shortB64)
            fail("Expected exception for <16 bytes secret")
        } catch (_: IllegalArgumentException) {}
    }

    @Test
    fun testAutoGenerateAndLoad() {
        val tempDir = Files.createTempDirectory("peerlink-test").toFile()
        tempDir.deleteOnExit()
        val certPath = File(tempDir, "peer-node-a.pem").absolutePath
        val keyPath = File(tempDir, "peer-node-a.key").absolutePath

        val (spki, created) = ensurePeerCertificate(certPath, keyPath, "node-a")
        assertTrue(created)
        assertTrue(spki.isNotEmpty())
        assertEquals(64, spki.length)

        // Calling again should load existing and created=false
        val (spki2, created2) = ensurePeerCertificate(certPath, keyPath, "node-a")
        assertFalse(created2)
        assertEquals(spki, spki2)

        val (chain, pk) = loadKeyPair(certPath, keyPath)
        assertEquals(1, chain.size)
        assertNotNull(pk)
        assertEquals(spki, spkiFingerprint(chain[0]))
        assertEquals("node-a", extractCN(chain[0]))
        assertEquals(listOf("urn:monstermq:node:node-a"), extractURIs(chain[0]))
    }

    @Test
    fun testVerifyChainAndIdentity() {
        val rootKp = genEC()
        val rootCert = generateCert(rootKp, "root-ca", isCa = true)

        val interKp = genEC()
        val interCert = generateCert(interKp, "intermediate-ca", signerKey = rootKp, signerCert = rootCert, isCa = true)

        val leafKp = genEC()
        val leafCert = generateCert(
            leafKp, "oa-a",
            signerKey = interKp, signerCert = interCert,
            uris = listOf("urn:monstermq:node:oa-a")
        )

        val trust = TrustConfig(roots = listOf(rootCert))

        // Verify valid chain
        trust.verify(arrayOf(leafCert, interCert), PeerIdentity(nodeId = "oa-a"), EKU_SERVER_AUTH)

        // Missing intermediate
        try {
            trust.verify(arrayOf(leafCert), PeerIdentity(nodeId = "oa-a"), EKU_SERVER_AUTH)
            fail("Expected UntrustedCertificateException for missing intermediate")
        } catch (_: UntrustedCertificateException) {}

        // Foreign CA
        val foreignKp = genEC()
        val foreignCert = generateCert(foreignKp, "foreign-ca", isCa = true)
        val foreignLeafKp = genEC()
        val foreignLeafCert = generateCert(foreignLeafKp, "oa-a", signerKey = foreignKp, signerCert = foreignCert, uris = listOf("urn:monstermq:node:oa-a"))

        try {
            trust.verify(arrayOf(foreignLeafCert), PeerIdentity(nodeId = "oa-a"), EKU_SERVER_AUTH)
            fail("Expected UntrustedCertificateException for foreign root")
        } catch (_: UntrustedCertificateException) {}

        // Identity mismatch
        try {
            trust.verify(arrayOf(leafCert, interCert), PeerIdentity(nodeId = "oa-b"), EKU_SERVER_AUTH)
            fail("Expected IdentityMismatchException")
        } catch (_: IdentityMismatchException) {}

        // Case insensitive NodeId in URI
        trust.verify(arrayOf(leafCert, interCert), PeerIdentity(nodeId = "OA-A"), EKU_SERVER_AUTH)
    }

    @Test
    fun testVerifyIdentityFallback() {
        val rootKp = genEC()
        val rootCert = generateCert(rootKp, "root-ca", isCa = true)

        // Cert with only DNS SAN
        val dnsKp = genEC()
        val dnsCert = generateCert(dnsKp, "other-cn", signerKey = rootKp, signerCert = rootCert, dnsNames = listOf("edge-node"))

        val trustDns = TrustConfig(roots = listOf(rootCert), fallback = IdentityFallback.DNS)
        trustDns.verify(arrayOf(dnsCert), PeerIdentity(nodeId = "edge-node"), EKU_SERVER_AUTH)

        val trustNone = TrustConfig(roots = listOf(rootCert), fallback = IdentityFallback.NONE)
        try {
            trustNone.verify(arrayOf(dnsCert), PeerIdentity(nodeId = "edge-node"), EKU_SERVER_AUTH)
            fail("Expected IdentityMismatchException when fallback is NONE")
        } catch (_: IdentityMismatchException) {}

        // Cert with only CN
        val cnKp = genEC()
        val cnCert = generateCert(cnKp, "edge-node", signerKey = rootKp, signerCert = rootCert)
        val trustCn = TrustConfig(roots = listOf(rootCert), fallback = IdentityFallback.CN)
        trustCn.verify(arrayOf(cnCert), PeerIdentity(nodeId = "edge-node"), EKU_SERVER_AUTH)
    }

    @Test
    fun testPinsMatch() {
        val kp = genEC()
        val cert = generateCert(kp, "node-p", uris = listOf("urn:monstermq:node:node-p"))

        val spki = spkiPin(cert)
        val certP = certPin(cert)

        val trustEmpty = TrustConfig(roots = emptyList())

        // Verified with SPKI pin without any roots
        trustEmpty.verify(arrayOf(cert), PeerIdentity(nodeId = "node-p", pins = listOf(spki)), EKU_SERVER_AUTH)
        // Verified with Cert pin
        trustEmpty.verify(arrayOf(cert), PeerIdentity(nodeId = "node-p", pins = listOf(certP)), EKU_SERVER_AUTH)

        // Pin mismatch
        val otherPin = Pin(ByteArray(32) { 99.toByte() })
        try {
            trustEmpty.verify(arrayOf(cert), PeerIdentity(nodeId = "node-p", pins = listOf(otherPin)), EKU_SERVER_AUTH)
            fail("Expected PinMismatchException")
        } catch (_: PinMismatchException) {}
    }

    @Test
    fun testMutualTlsHandshakeAndKeyingMaterial() {
        val caKp = genEC()
        val caCert = generateCert(caKp, "peer-ca", isCa = true)

        val serverKp = genEC()
        val serverCert = generateCert(serverKp, "main", signerKey = caKp, signerCert = caCert, uris = listOf("urn:monstermq:node:main"))

        val clientKp = genEC()
        val clientCert = generateCert(clientKp, "edge-a", signerKey = caKp, signerCert = caCert, uris = listOf("urn:monstermq:node:edge-a"))

        val trust = TrustConfig(roots = listOf(caCert))

        val serverSsl = createServerSSLContext(
            ServerTlsOptions(
                certChain = arrayOf(serverCert),
                privateKey = serverKp.private,
                trust = trust,
                clientAuth = ClientAuth.REQUIRED,
                peers = listOf(PeerIdentity(nodeId = "edge-a")),
                sharedSecret = true
            )
        )

        val clientSsl = createClientSSLContext(
            ClientTlsOptions(
                certChain = arrayOf(clientCert),
                privateKey = clientKp.private,
                trust = trust,
                peer = PeerIdentity(nodeId = "main"),
                sharedSecret = true
            )
        )

        val serverSocket = ServerSocket(0)
        val port = serverSocket.localPort

        val serverEkm = AtomicReference<ByteArray>()
        val clientEkm = AtomicReference<ByteArray>()
        val serverErr = AtomicReference<Throwable>()
        val clientErr = AtomicReference<Throwable>()
        val latch = CountDownLatch(2)

        Thread.ofVirtual().start {
            try {
                val raw = serverSocket.accept()
                val firstByte = raw.getInputStream().read()
                assertEquals(0x16, firstByte)

                val ssl = wrapServerSocket(serverSsl, raw, byteArrayOf(firstByte.toByte()), ClientAuth.REQUIRED, sharedSecret = true)
                ssl.startHandshake()
                val b = ssl.inputStream.read()
                assertEquals(42, b)
                serverEkm.set(exportKeyingMaterial(ssl, "monstermq-peer/1", ByteArray(0), 32))
            } catch (t: Throwable) {
                if (serverErr.get() == null) serverErr.set(t)
            } finally {
                latch.countDown()
            }
        }

        Thread.ofVirtual().start {
            try {
                val raw = Socket("127.0.0.1", port)
                val ssl = wrapClientSocket(clientSsl, raw, "127.0.0.1", port, sharedSecret = true)
                ssl.startHandshake()
                ssl.outputStream.write(42)
                ssl.outputStream.flush()
                clientEkm.set(exportKeyingMaterial(ssl, "monstermq-peer/1", ByteArray(0), 32))
            } catch (t: Throwable) {
                if (clientErr.get() == null) clientErr.set(t)
            } finally {
                latch.countDown()
            }
        }

        assertTrue("Handshake timed out", latch.await(10, TimeUnit.SECONDS))
        assertNull("Server error: ${serverErr.get()}", serverErr.get())
        assertNull("Client error: ${clientErr.get()}", clientErr.get())

        val sBytes = serverEkm.get()
        val cBytes = clientEkm.get()
        assertNotNull(sBytes)
        assertNotNull(cBytes)
        assertEquals(32, sBytes.size)
        assertArrayEquals("Exported keying material must match on both ends!", sBytes, cBytes)

        serverSocket.close()
    }
}
