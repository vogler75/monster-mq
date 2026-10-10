package at.rocworks.peerlink.tls

import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x509.BasicConstraints
import org.bouncycastle.asn1.x509.ExtendedKeyUsage
import org.bouncycastle.asn1.x509.Extension
import org.bouncycastle.asn1.x509.GeneralName
import org.bouncycastle.asn1.x509.GeneralNames
import org.bouncycastle.asn1.x509.KeyPurposeId
import org.bouncycastle.asn1.x509.KeyUsage
import org.bouncycastle.cert.X509v3CertificateBuilder
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder
import org.bouncycastle.jsse.BCExtendedSSLSession
import org.bouncycastle.jsse.BCSSLSocket
import org.bouncycastle.jsse.provider.BouncyCastleJsseProvider
import org.bouncycastle.openssl.PEMKeyPair
import org.bouncycastle.openssl.PEMParser
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder
import java.io.*
import java.math.BigInteger
import java.net.Socket
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions
import java.security.*
import java.security.cert.*
import java.util.*
import javax.net.ssl.*

const val ALPN_PEER = "mmq-peer/1"
const val ALPN_HTTP = "http/1.1"

const val NODE_URI_PREFIX = "urn:monstermq:node:"
const val NODE_ID_PLACEHOLDER = "{NodeId}"
const val MIN_SECRET_BYTES = 16

const val EKU_SERVER_AUTH = "1.3.6.1.5.5.7.3.1"
const val EKU_CLIENT_AUTH = "1.3.6.1.5.5.7.3.2"
const val EKU_ANY = "2.5.29.37.0"

// Sentinel exceptions matching Go tlsutil errors
open class PeerTlsException(message: String, cause: Throwable? = null) : Exception(message, cause)
class NoCertificateException(message: String = "tlsutil: no peer certificate") : PeerTlsException(message)
class UntrustedCertificateException(message: String = "tlsutil: certificate not trusted", cause: Throwable? = null) : PeerTlsException(message, cause)
class KeyUsageException(message: String = "tlsutil: certificate lacks the required extended key usage", cause: Throwable? = null) : PeerTlsException(message, cause)
class PinMismatchException(message: String = "tlsutil: certificate matches no pin") : PeerTlsException(message)
class IdentityMismatchException(message: String = "tlsutil: certificate identity mismatch") : PeerTlsException(message)
class NoTrustException(message: String = "tlsutil: no truststore, pin or shared secret to authenticate the peer") : PeerTlsException(message)

enum class ClientAuth {
    NONE, REQUEST, REQUIRED;

    companion object {
        fun parse(s: String?): ClientAuth {
            val v = (s ?: "").trim().uppercase()
            if (v.isEmpty()) return NONE
            return when (v) {
                "NONE" -> NONE
                "REQUEST" -> REQUEST
                "REQUIRED" -> REQUIRED
                else -> throw IllegalArgumentException("tlsutil: unknown ClientAuth \"$s\" (NONE, REQUEST, REQUIRED)")
            }
        }
    }
}

enum class IdentityFallback {
    NONE, DNS, CN;

    companion object {
        fun parse(s: String?): IdentityFallback {
            val v = (s ?: "").trim().uppercase()
            if (v.isEmpty()) return NONE
            return when (v) {
                "NONE" -> NONE
                "DNS" -> DNS
                "CN" -> CN
                else -> throw IllegalArgumentException("tlsutil: unknown IdentityFallback \"$s\" (NONE, DNS, CN)")
            }
        }
    }
}

fun nodeURI(nodeId: String): String = NODE_URI_PREFIX + nodeId

fun expandPath(path: String, nodeId: String): String =
    path.replace(NODE_ID_PLACEHOLDER, nodeId)

class Pin(val bytes: ByteArray) {
    init {
        require(bytes.size == 32) { "pin must be 32 bytes (SHA-256)" }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Pin) return false
        return bytes.contentEquals(other.bytes)
    }

    override fun hashCode(): Int = bytes.contentHashCode()

    override fun toString(): String {
        val sb = StringBuilder(64)
        for (b in bytes) {
            val v = b.toInt() and 0xFF
            if (v < 0x10) sb.append('0')
            sb.append(v.toString(16))
        }
        return sb.toString()
    }
}

fun parsePin(s: String): Pin {
    val clean = s.trim().replace(":", "").replace(" ", "")
    if (clean.length != 64) {
        throw IllegalArgumentException("tlsutil: pin \"$s\": want 64 hex digits")
    }
    val bytes = ByteArray(32)
    for (i in 0 until 32) {
        val hi = Character.digit(clean[i * 2], 16)
        val lo = Character.digit(clean[i * 2 + 1], 16)
        if (hi < 0 || lo < 0) {
            throw IllegalArgumentException("tlsutil: pin \"$s\": invalid hex digit")
        }
        bytes[i] = ((hi shl 4) or lo).toByte()
    }
    return Pin(bytes)
}

fun parsePins(list: List<String>?): List<Pin> {
    if (list.isNullOrEmpty()) return emptyList()
    return list.map { parsePin(it) }
}

fun sha256(data: ByteArray): ByteArray =
    MessageDigest.getInstance("SHA-256").digest(data)

fun spkiPin(c: X509Certificate): Pin = Pin(sha256(c.publicKey.encoded))

fun certPin(c: X509Certificate): Pin = Pin(sha256(c.encoded))

fun spkiFingerprint(c: X509Certificate): String = spkiPin(c).toString()

fun matchPins(c: X509Certificate, pins: List<Pin>): Boolean {
    val spki = spkiPin(c)
    val whole = certPin(c)
    for (p in pins) {
        if (p == spki || p == whole) return true
    }
    return false
}

fun decodeSecret(s: String): ByteArray {
    val clean = s.trim()
    val decoders = listOf(
        Base64.getDecoder(),
        Base64.getUrlDecoder()
    )
    var decoded: ByteArray? = null
    for (dec in decoders) {
        try {
            decoded = dec.decode(clean)
            break
        } catch (_: IllegalArgumentException) {
            // try next
        }
    }
    if (decoded == null) {
        // try padding if unpadded
        val padded = when (clean.length % 4) {
            2 -> "$clean=="
            3 -> "$clean="
            else -> clean
        }
        for (dec in decoders) {
            try {
                decoded = dec.decode(padded)
                break
            } catch (_: IllegalArgumentException) {
            }
        }
    }
    if (decoded == null) {
        throw IllegalArgumentException("tlsutil: shared secret is not base64")
    }
    if (decoded.size < MIN_SECRET_BYTES) {
        throw IllegalArgumentException("tlsutil: shared secret has ${decoded.size} bytes, need at least $MIN_SECRET_BYTES")
    }
    return decoded
}

fun decodeSecrets(list: List<String>?): List<ByteArray> {
    if (list.isNullOrEmpty()) return emptyList()
    return list.mapIndexed { idx, s ->
        try {
            decodeSecret(s)
        } catch (e: Exception) {
            throw IllegalArgumentException("tlsutil: shared secret at index $idx: ${e.message}", e)
        }
    }
}

data class PeerIdentity(
    val nodeId: String = "",
    val certificateIdentity: String = "",
    val pins: List<Pin> = emptyList()
)

data class TrustConfig(
    val roots: List<X509Certificate>? = null,
    val fallback: IdentityFallback = IdentityFallback.NONE
) {
    fun hasRoots(): Boolean = !roots.isNullOrEmpty()

    fun verify(chain: Array<X509Certificate>?, peer: PeerIdentity, usageOid: String) {
        if (chain.isNullOrEmpty()) {
            throw NoCertificateException()
        }
        verifyTrust(chain, peer, usageOid)
        matchIdentity(chain[0], peer)
    }

    fun findPeer(chain: Array<X509Certificate>?, peers: List<PeerIdentity>, usageOid: String): PeerIdentity {
        if (chain.isNullOrEmpty()) {
            throw NoCertificateException()
        }
        var trustErr: Exception? = null
        for (p in peers) {
            try {
                matchIdentity(chain[0], p)
            } catch (_: IdentityMismatchException) {
                continue
            }
            try {
                verifyTrust(chain, p, usageOid)
                return p
            } catch (e: Exception) {
                if (trustErr == null) trustErr = e
            }
        }
        if (trustErr != null) throw trustErr
        throw IdentityMismatchException("tlsutil: certificate identity mismatch: ${describe(chain[0])} matches no configured peer")
    }

    fun verifyTrust(chain: Array<X509Certificate>, p: PeerIdentity, usageOid: String) {
        val leaf = chain[0]
        if (p.pins.isNotEmpty()) {
            if (!matchPins(leaf, p.pins)) {
                throw PinMismatchException("tlsutil: certificate matches no pin: spki ${spkiFingerprint(leaf)}")
            }
            if (!hasUsage(leaf, usageOid)) {
                throw KeyUsageException()
            }
            return
        }

        if (!hasRoots()) {
            throw UntrustedCertificateException()
        }

        val rootList = roots!!
        // Check validity period
        try {
            for (c in chain) {
                c.checkValidity()
            }
        } catch (e: CertificateExpiredException) {
            throw UntrustedCertificateException("tlsutil: certificate not trusted (expired)", e)
        } catch (e: CertificateNotYetValidException) {
            throw UntrustedCertificateException("tlsutil: certificate not trusted (not yet valid)", e)
        }

        // If leaf itself is in roots and chain.size == 1
        if (chain.size == 1 && rootList.any { it.encoded.contentEquals(leaf.encoded) }) {
            if (!hasUsage(leaf, usageOid)) {
                throw KeyUsageException()
            }
            return
        }

        // Build cert path validation
        try {
            val cf = CertificateFactory.getInstance("X.509")
            // Intermediates + leaf
            // CertPathValidator expects path from target to most-trusted CA (excluding the anchor)
            val pathList = mutableListOf<X509Certificate>()
            var foundAnchor = false
            for (c in chain) {
                if (rootList.any { it.encoded.contentEquals(c.encoded) }) {
                    foundAnchor = true
                    break
                }
                pathList.add(c)
            }

            val anchors = rootList.map { TrustAnchor(it, null) }.toSet()
            val params = PKIXParameters(anchors)
            params.isRevocationEnabled = false

            if (pathList.isEmpty() && foundAnchor) {
                // Leaf itself was the anchor
                if (!hasUsage(leaf, usageOid)) throw KeyUsageException()
                return
            }

            val certPath = cf.generateCertPath(pathList)
            val validator = CertPathValidator.getInstance("PKIX")
            validator.validate(certPath, params)
        } catch (e: KeyUsageException) {
            throw e
        } catch (e: Exception) {
            throw UntrustedCertificateException("tlsutil: certificate not trusted: ${e.message}", e)
        }

        if (!hasUsage(leaf, usageOid)) {
            throw KeyUsageException()
        }
    }

    fun matchIdentity(leaf: X509Certificate, p: PeerIdentity) {
        val uris = extractURIs(leaf)
        val dnsNames = extractDNSNames(leaf)

        if (p.certificateIdentity.isNotEmpty()) {
            for (u in uris) {
                if (u == p.certificateIdentity) return
            }
            for (d in dnsNames) {
                if (d.equals(p.certificateIdentity, ignoreCase = true)) return
            }
            throw IdentityMismatchException("tlsutil: certificate identity mismatch: want ${p.certificateIdentity}, have ${describe(leaf)}")
        }

        if (p.nodeId.isEmpty()) {
            throw IdentityMismatchException("tlsutil: certificate identity mismatch: no NodeId to bind ${describe(leaf)} to")
        }

        val want = nodeURI(p.nodeId)
        var hasNodeURI = false
        for (u in uris) {
            if (u.equals(want, ignoreCase = true)) return
            if (u.length >= NODE_URI_PREFIX.length && u.substring(0, NODE_URI_PREFIX.length).equals(NODE_URI_PREFIX, ignoreCase = true)) {
                hasNodeURI = true
            }
        }

        if (!hasNodeURI) {
            when (fallback) {
                IdentityFallback.DNS -> {
                    for (d in dnsNames) {
                        if (d.equals(p.nodeId, ignoreCase = true)) return
                    }
                }
                IdentityFallback.CN -> {
                    val cn = extractCN(leaf)
                    if (cn.equals(p.nodeId, ignoreCase = true)) return
                }
                IdentityFallback.NONE -> {}
            }
        }

        throw IdentityMismatchException("tlsutil: certificate identity mismatch: want $want, have ${describe(leaf)}")
    }
}

fun hasUsage(c: X509Certificate, usageOid: String): Boolean {
    val usages = c.extendedKeyUsage ?: return true
    if (usages.isEmpty()) return true
    return usages.contains(usageOid) || usages.contains(EKU_ANY)
}

fun extractURIs(c: X509Certificate): List<String> {
    val res = mutableListOf<String>()
    val sans = c.subjectAlternativeNames ?: return res
    for (san in sans) {
        val type = san[0] as? Int ?: continue
        if (type == 6) { // URI
            val str = san[1] as? String ?: continue
            res.add(str)
        }
    }
    return res
}

fun extractDNSNames(c: X509Certificate): List<String> {
    val res = mutableListOf<String>()
    val sans = c.subjectAlternativeNames ?: return res
    for (san in sans) {
        val type = san[0] as? Int ?: continue
        if (type == 2) { // DNS
            val str = san[1] as? String ?: continue
            res.add(str)
        }
    }
    return res
}

fun extractCN(c: X509Certificate): String {
    val name = c.subjectX500Principal.name
    // Extract CN=...
    val parts = name.split(",")
    for (p in parts) {
        val kv = p.trim().split("=", limit = 2)
        if (kv.size == 2 && kv[0].trim().equals("CN", ignoreCase = true)) {
            return kv[1].trim()
        }
    }
    return ""
}

fun describe(c: X509Certificate): String {
    val sb = StringBuilder()
    sb.append("CN=").append(extractCN(c))
    for (u in extractURIs(c)) {
        sb.append(" URI:").append(u)
    }
    for (d in extractDNSNames(c)) {
        sb.append(" DNS:").append(d)
    }
    return sb.toString()
}

fun loadKeyPair(certPath: String, keyPath: String): Pair<Array<X509Certificate>, PrivateKey> {
    if (certPath.isEmpty() || keyPath.isEmpty()) {
        throw IllegalArgumentException("tlsutil: certificate and key paths must both be set")
    }

    val certs = mutableListOf<X509Certificate>()
    FileReader(certPath).use { reader ->
        val cf = CertificateFactory.getInstance("X.509")
        val parsed = cf.generateCertificates(FileInputStream(certPath))
        for (c in parsed) {
            if (c is X509Certificate) certs.add(c)
        }
    }
    if (certs.isEmpty()) {
        throw IllegalArgumentException("tlsutil: parse certificate $certPath: no certificates found")
    }

    val key = loadPrivateKey(keyPath)

    return Pair(certs.toTypedArray(), key)
}

fun loadPrivateKey(keyPath: String): PrivateKey {
    FileReader(keyPath).use { reader ->
        val parser = PEMParser(reader)
        var obj = parser.readObject()
        var pk: PrivateKey? = null
        val converter = JcaPEMKeyConverter()
        while (obj != null) {
            when (obj) {
                is PEMKeyPair -> {
                    pk = converter.getPrivateKey(obj.privateKeyInfo)
                    break
                }
                is org.bouncycastle.asn1.pkcs.PrivateKeyInfo -> {
                    pk = converter.getPrivateKey(obj)
                    break
                }
                is org.bouncycastle.openssl.PEMEncryptedKeyPair -> {
                    throw IllegalArgumentException("tlsutil: encrypted private key not supported in $keyPath")
                }
            }
            obj = parser.readObject()
        }
        return pk ?: throw IllegalArgumentException("tlsutil: no PEM private key in $keyPath")
    }
}

// Recomputes the public half of a key pair from its private key (EC: Q = d*G, RSA: CRT fields).
fun derivePublicKey(privateKey: PrivateKey): PublicKey = when (privateKey) {
    is java.security.interfaces.ECPrivateKey -> {
        val spec = org.bouncycastle.jcajce.provider.asymmetric.util.EC5Util.convertSpec(privateKey.params)
        val q = spec.g.multiply(privateKey.s).normalize()
        val w = java.security.spec.ECPoint(q.affineXCoord.toBigInteger(), q.affineYCoord.toBigInteger())
        KeyFactory.getInstance("EC").generatePublic(java.security.spec.ECPublicKeySpec(w, privateKey.params))
    }
    is java.security.interfaces.RSAPrivateCrtKey ->
        KeyFactory.getInstance("RSA").generatePublic(java.security.spec.RSAPublicKeySpec(privateKey.modulus, privateKey.publicExponent))
    else -> throw IllegalStateException("tlsutil: cannot derive public key from ${privateKey.algorithm} private key")
}

fun loadCertPool(path: String, storeType: String = "PEM", password: String = ""): List<X509Certificate> {
    if (path.isEmpty()) {
        return emptyList()
    }
    val type = storeType.trim().uppercase()
    val res = mutableListOf<X509Certificate>()
    val file = File(path)
    if (!file.exists()) {
        throw FileNotFoundException("tlsutil: read truststore $path: file not found")
    }

    when (type) {
        "", "PEM" -> {
            FileInputStream(file).use { fis ->
                val cf = CertificateFactory.getInstance("X.509")
                val certs = cf.generateCertificates(fis)
                for (c in certs) {
                    if (c is X509Certificate) res.add(c)
                }
            }
            if (res.isEmpty()) {
                throw IllegalArgumentException("tlsutil: no PEM certificates in truststore $path")
            }
        }
        "PKCS12", "PFX", "P12" -> {
            val ks = KeyStore.getInstance("PKCS12")
            FileInputStream(file).use { fis ->
                ks.load(fis, password.toCharArray())
            }
            for (alias in ks.aliases()) {
                val cert = ks.getCertificate(alias)
                if (cert is X509Certificate) {
                    res.add(cert)
                }
            }
            if (res.isEmpty()) {
                throw IllegalArgumentException("tlsutil: no certificates in pkcs12 truststore $path")
            }
        }
        else -> throw IllegalArgumentException("tlsutil: unknown truststore type \"$storeType\" (PEM, PKCS12)")
    }
    return res
}

fun ensurePeerCertificate(certPath: String, keyPath: String, nodeId: String): Pair<String, Boolean> {
    if (certPath.isEmpty() || keyPath.isEmpty()) {
        throw IllegalArgumentException("tlsutil: certificate and key paths must both be set")
    }
    if (nodeId.isEmpty()) {
        throw IllegalArgumentException("tlsutil: NodeId must not be empty")
    }

    val certFile = File(certPath)
    val keyFile = File(keyPath)
    val certExists = certFile.exists()
    val keyExists = keyFile.exists()

    if (certExists && keyExists) {
        val (chain, _) = loadKeyPair(certPath, keyPath)
        return Pair(spkiFingerprint(chain[0]), false)
    }
    if (certExists) {
        throw IllegalStateException("tlsutil: certificate $certPath exists but key $keyPath is missing; refusing to overwrite it")
    }

    val keyPair: KeyPair
    if (keyExists) {
        val pk = loadPrivateKey(keyPath)
        keyPair = KeyPair(derivePublicKey(pk), pk)
    } else {
        val kpg = KeyPairGenerator.getInstance("EC")
        kpg.initialize(256, SecureRandom())
        keyPair = kpg.generateKeyPair()

        // Write private key PEM atomically with mode 0600
        val sw = StringWriter()
        sw.write("-----BEGIN PRIVATE KEY-----\n")
        sw.write(Base64.getMimeEncoder(64, "\n".toByteArray()).encodeToString(keyPair.private.encoded))
        sw.write("\n-----END PRIVATE KEY-----\n")
        writeFileAtomic(keyPath, sw.toString().toByteArray(Charsets.UTF_8), setOf(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE))
    }

    // Generate self-signed cert
    val now = System.currentTimeMillis()
    val notBefore = Date(now - 3600_000L) // 1 hour ago
    val cal = Calendar.getInstance()
    cal.time = Date(now)
    cal.add(Calendar.YEAR, 10)
    val notAfter = cal.time

    val serial = BigInteger(128, SecureRandom())
    val subject = X500Name("CN=$nodeId, O=MonsterMQ")
    val builder = JcaX509v3CertificateBuilder(
        subject,
        serial,
        notBefore,
        notAfter,
        subject,
        keyPair.public
    )

    // KeyUsage digitalSignature (and keyEncipherment if RSA)
    val ku = if (keyPair.public.algorithm.equals("RSA", ignoreCase = true)) {
        KeyUsage(KeyUsage.digitalSignature or KeyUsage.keyEncipherment)
    } else {
        KeyUsage(KeyUsage.digitalSignature)
    }
    builder.addExtension(Extension.keyUsage, true, ku)

    // ExtKeyUsage ServerAuth + ClientAuth
    val eku = ExtendedKeyUsage(arrayOf(KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth))
    builder.addExtension(Extension.extendedKeyUsage, false, eku)

    // BasicConstraints false
    builder.addExtension(Extension.basicConstraints, true, BasicConstraints(false))

    // URI SAN urn:monstermq:node:<nodeId>
    val uri = nodeURI(nodeId)
    val san = GeneralNames(GeneralName(GeneralName.uniformResourceIdentifier, uri))
    builder.addExtension(Extension.subjectAlternativeName, false, san)

    val sigAlg = if (keyPair.private.algorithm.equals("RSA", ignoreCase = true)) "SHA256withRSA" else "SHA256withECDSA"
    val signer = JcaContentSignerBuilder(sigAlg).build(keyPair.private)
    val holder = builder.build(signer)
    val cert = JcaX509CertificateConverter().getCertificate(holder)

    // Write certificate PEM atomically with mode 0644
    val sw = StringWriter()
    sw.write("-----BEGIN CERTIFICATE-----\n")
    sw.write(Base64.getMimeEncoder(64, "\n".toByteArray()).encodeToString(cert.encoded))
    sw.write("\n-----END CERTIFICATE-----\n")
    writeFileAtomic(
        certPath,
        sw.toString().toByteArray(Charsets.UTF_8),
        setOf(
            PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.GROUP_READ,
            PosixFilePermission.OTHERS_READ
        )
    )

    return Pair(spkiFingerprint(cert), true)
}

fun writeFileAtomic(pathStr: String, data: ByteArray, perms: Set<PosixFilePermission>) {
    val path = Paths.get(pathStr)
    val dir = path.parent ?: Paths.get(".")
    Files.createDirectories(dir)
    val tmp = Files.createTempFile(dir, ".${path.fileName}.tmp", "")
    try {
        try {
            Files.setPosixFilePermissions(tmp, perms)
        } catch (_: UnsupportedOperationException) {
            // Non-posix (Windows)
        }
        Files.write(tmp, data)
        try {
            Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } catch (_: Exception) {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING)
        }
    } finally {
        Files.deleteIfExists(tmp)
    }
}

// Custom KeyManager for single cert chain / key
class SingleCertKeyManager(
    private val chain: Array<X509Certificate>,
    private val privateKey: PrivateKey
) : X509ExtendedKeyManager() {
    override fun getClientAliases(keyType: String?, issuers: Array<out Principal>?): Array<String> = arrayOf("peer")
    override fun chooseClientAlias(keyType: Array<out String>?, issuers: Array<out Principal>?, socket: Socket?): String = "peer"
    override fun getServerAliases(keyType: String?, issuers: Array<out Principal>?): Array<String> = arrayOf("peer")
    override fun chooseServerAlias(keyType: String?, issuers: Array<out Principal>?, socket: Socket?): String = "peer"
    override fun getCertificateChain(alias: String?): Array<X509Certificate> = chain
    override fun getPrivateKey(alias: String?): PrivateKey = privateKey
}

data class ServerTlsOptions(
    val certChain: Array<X509Certificate>,
    val privateKey: PrivateKey,
    val trust: TrustConfig = TrustConfig(),
    val clientAuth: ClientAuth = ClientAuth.NONE,
    val peers: List<PeerIdentity> = emptyList(),
    val sharedSecret: Boolean = false
)

data class ClientTlsOptions(
    val certChain: Array<X509Certificate>? = null,
    val privateKey: PrivateKey? = null,
    val trust: TrustConfig = TrustConfig(),
    val peer: PeerIdentity = PeerIdentity(),
    val serverName: String = "",
    val sharedSecret: Boolean = false,
    val insecureSkipVerify: Boolean = false,
    val nextProtos: List<String> = listOf(ALPN_PEER)
)

fun createServerSSLContext(opts: ServerTlsOptions): SSLContext {
    val auth = opts.clientAuth
    val anyPins = opts.peers.any { it.pins.isNotEmpty() }
    val allPinned = opts.peers.isNotEmpty() && opts.peers.all { it.pins.isNotEmpty() }

    if (auth != ClientAuth.NONE && !opts.trust.hasRoots() && !allPinned) {
        throw IllegalArgumentException("tlsutil: ClientAuth $auth needs a truststore or pins for every peer")
    }

    val trustManager = object : X509ExtendedTrustManager() {
        override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?, socket: Socket?) {
            verifyClient(chain)
        }

        override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?, engine: SSLEngine?) {
            verifyClient(chain)
        }

        override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?) {
            verifyClient(chain)
        }

        override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?, socket: Socket?) {}
        override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?, engine: SSLEngine?) {}
        override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?) {}

        override fun getAcceptedIssuers(): Array<X509Certificate> {
            if (anyPins || auth == ClientAuth.NONE || !opts.trust.hasRoots()) {
                return emptyArray()
            }
            return opts.trust.roots!!.toTypedArray()
        }

        private fun verifyClient(chain: Array<out X509Certificate>?) {
            if (chain.isNullOrEmpty()) {
                if (auth == ClientAuth.REQUIRED) {
                    throw CertificateException("tlsutil: no peer certificate")
                }
                return
            }
            @Suppress("UNCHECKED_CAST")
            val xChain = chain as Array<X509Certificate>
            try {
                opts.trust.findPeer(xChain, opts.peers, EKU_CLIENT_AUTH)
            } catch (e: Exception) {
                throw CertificateException(e.message, e)
            }
        }
    }

    val keyManager = SingleCertKeyManager(opts.certChain, opts.privateKey)
    val sslContext = SSLContext.getInstance("TLS", BouncyCastleJsseProvider())
    sslContext.init(arrayOf(keyManager), arrayOf(trustManager), SecureRandom())
    return sslContext
}

fun createClientSSLContext(opts: ClientTlsOptions): SSLContext {
    if (opts.peer.nodeId.isEmpty() && opts.peer.certificateIdentity.isEmpty() && !opts.insecureSkipVerify) {
        throw IllegalArgumentException("tlsutil: dialer needs the source NodeId")
    }
    if (!opts.insecureSkipVerify && !opts.sharedSecret && opts.peer.pins.isEmpty() && !opts.trust.hasRoots()) {
        throw NoTrustException()
    }

    val trustManager = object : X509ExtendedTrustManager() {
        override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?, socket: Socket?) {
            verifyServer(chain)
        }

        override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?, engine: SSLEngine?) {
            verifyServer(chain)
        }

        override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?) {
            verifyServer(chain)
        }

        override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?, socket: Socket?) {}
        override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?, engine: SSLEngine?) {}
        override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?) {}

        override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()

        private fun verifyServer(chain: Array<out X509Certificate>?) {
            if (opts.insecureSkipVerify) return
            if (chain.isNullOrEmpty()) {
                throw CertificateException("tlsutil: no peer certificate")
            }
            @Suppress("UNCHECKED_CAST")
            val xChain = chain as Array<X509Certificate>
            val optional = opts.sharedSecret && opts.peer.pins.isEmpty()
            try {
                opts.trust.verify(xChain, opts.peer, EKU_SERVER_AUTH)
            } catch (e: Exception) {
                if (optional) {
                    return
                }
                throw CertificateException(e.message, e)
            }
        }
    }

    val keyManagers: Array<KeyManager>? = if (opts.certChain != null && opts.privateKey != null) {
        arrayOf(SingleCertKeyManager(opts.certChain, opts.privateKey))
    } else {
        null
    }

    val sslContext = SSLContext.getInstance("TLS", BouncyCastleJsseProvider())
    sslContext.init(keyManagers, arrayOf(trustManager), SecureRandom())
    return sslContext
}

private val socketEkmMap = java.util.concurrent.ConcurrentHashMap<Socket, java.util.concurrent.atomic.AtomicReference<ByteArray>>()

fun attachKeyingMaterialHook(
    socket: SSLSocket,
    label: String = "monstermq-peer/1",
    context: ByteArray = ByteArray(0),
    length: Int = 32
): java.util.concurrent.atomic.AtomicReference<ByteArray> {
    val existing = socketEkmMap[socket]
    if (existing != null) return existing

    val result = java.util.concurrent.atomic.AtomicReference<ByteArray>()
    socketEkmMap[socket] = result
    try {
        var cls: Class<*>? = socket.javaClass
        var field: java.lang.reflect.Field? = null
        while (cls != null && field == null) {
            try {
                field = cls.getDeclaredField("listeners")
            } catch (_: NoSuchFieldException) {
                cls = cls.superclass
            }
        }
        if (field != null) {
            field.isAccessible = true
            @Suppress("UNCHECKED_CAST")
            val origMap = field.get(socket) as? MutableMap<Any, Any> ?: java.util.HashMap()
            val proxyMap = object : AbstractMap<Any, Any>() {
                override val entries: MutableSet<MutableMap.MutableEntry<Any, Any>>
                    get() {
                        export()
                        return origMap.entries
                    }
                override fun isEmpty(): Boolean {
                    export()
                    return origMap.isEmpty()
                }
                private fun export() {
                    if (result.get() == null && socket is BCSSLSocket) {
                        val conn = socket.connection
                        if (conn != null) {
                            try {
                                val method = conn.javaClass.getMethod("exportKeyingMaterial", String::class.java, ByteArray::class.java, Int::class.javaPrimitiveType)
                                method.isAccessible = true
                                val bytes = method.invoke(conn, label, context, length) as ByteArray
                                result.set(bytes)
                            } catch (_: Exception) {}
                        }
                    }
                }
                override fun put(key: Any, value: Any): Any? = origMap.put(key, value)
            }
            field.set(socket, proxyMap)
            socket.addHandshakeCompletedListener { }
        }
    } catch (_: Exception) {}
    return result
}

fun wrapServerSocket(
    sslContext: SSLContext,
    rawSocket: Socket,
    consumed: ByteArray,
    clientAuth: ClientAuth,
    sharedSecret: Boolean = false
): SSLSocket {
    val sslSocket = sslContext.socketFactory.createSocket(rawSocket, ByteArrayInputStream(consumed), true) as SSLSocket
    sslSocket.useClientMode = false
    when (clientAuth) {
        ClientAuth.REQUIRED -> sslSocket.needClientAuth = true
        ClientAuth.REQUEST -> sslSocket.wantClientAuth = true
        ClientAuth.NONE -> {
            sslSocket.needClientAuth = false
            sslSocket.wantClientAuth = false
        }
    }

    val params = sslSocket.sslParameters
    params.applicationProtocols = arrayOf(ALPN_PEER, ALPN_HTTP)
    params.protocols = if (sharedSecret) arrayOf("TLSv1.3") else arrayOf("TLSv1.3", "TLSv1.2")
    sslSocket.sslParameters = params

    if (sharedSecret) {
        attachKeyingMaterialHook(sslSocket)
    }
    return sslSocket
}

fun wrapClientSocket(
    sslContext: SSLContext,
    rawSocket: Socket,
    host: String,
    port: Int,
    sharedSecret: Boolean = false,
    nextProtos: List<String> = listOf(ALPN_PEER)
): SSLSocket {
    val sslSocket = sslContext.socketFactory.createSocket(rawSocket, host, port, true) as SSLSocket
    sslSocket.useClientMode = true

    val params = sslSocket.sslParameters
    params.applicationProtocols = nextProtos.toTypedArray()
    params.protocols = if (sharedSecret) arrayOf("TLSv1.3") else arrayOf("TLSv1.3", "TLSv1.2")
    sslSocket.sslParameters = params

    if (sharedSecret) {
        attachKeyingMaterialHook(sslSocket)
    }
    return sslSocket
}

fun exportKeyingMaterial(
    socket: SSLSocket,
    label: String = "monstermq-peer/1",
    context: ByteArray = ByteArray(0),
    length: Int = 32
): ByteArray {
    val cached = socketEkmMap[socket]?.get()
    if (cached != null) {
        return cached
    }
    if (socket is BCSSLSocket) {
        val conn = socket.connection
        if (conn != null) {
            try {
                val method = conn.javaClass.getMethod("exportKeyingMaterial", String::class.java, ByteArray::class.java, Int::class.javaPrimitiveType)
                method.isAccessible = true
                return method.invoke(conn, label, context, length) as ByteArray
            } catch (_: Exception) {}
        }
        val bcSession = socket.bcSession
        if (bcSession != null) {
            try {
                return bcSession.exportKeyingMaterialData(label, context, length)
            } catch (_: UnsupportedOperationException) {}
        }
    }
    // Fallback if not BCSSLSocket directly, check session
    val session = socket.session
    if (session is BCExtendedSSLSession) {
        try {
            return session.exportKeyingMaterialData(label, context, length)
        } catch (_: UnsupportedOperationException) {}
    }
    // JDK 25 ExtendedSSLSession reflection
    try {
        val method = session.javaClass.getMethod("exportKeyingMaterial", String::class.java, ByteArray::class.java, Int::class.javaPrimitiveType)
        return method.invoke(session, label, context, length) as ByteArray
    } catch (_: Exception) {}

    throw UnsupportedOperationException("Socket session does not support exportKeyingMaterial: ${session?.javaClass?.name}")
}

class PeerTls(
    val config: at.rocworks.peerlink.config.PeerLinkTLSConfig,
    val nodeId: String,
    val peers: List<PeerIdentity> = emptyList()
) {
    private val keyPairAndCert = if (config.enabled) {
        val certPath = expandPath(config.effectiveCertPath(), nodeId)
        val keyPath = expandPath(config.effectiveKeyPath(), nodeId)
        if (config.autoGenerate) {
            ensurePeerCertificate(certPath, keyPath, nodeId)
        }
        loadKeyPair(certPath, keyPath)
    } else null

    private val trustConfig = if (config.enabled && config.trustStorePath.isNotEmpty()) {
        val tsPath = expandPath(config.trustStorePath, nodeId)
        val roots = loadCertPool(tsPath, config.effectiveTrustStoreType(), config.trustStorePassword)
        val fb = IdentityFallback.parse(config.effectiveIdentityFallback())
        TrustConfig(roots, fb)
    } else TrustConfig(fallback = IdentityFallback.parse(config.effectiveIdentityFallback()))

    private val serverSslContext: SSLContext? by lazy {
        if (keyPairAndCert == null) null
        else {
            val ca = ClientAuth.parse(config.clientAuth.name)
            createServerSSLContext(
                ServerTlsOptions(
                    certChain = keyPairAndCert.first,
                    privateKey = keyPairAndCert.second,
                    trust = trustConfig,
                    clientAuth = ca,
                    peers = peers
                )
            )
        }
    }

    fun wrapServerSocket(rawSocket: Socket, firstByte: Byte, sharedSecret: Boolean = false): SSLSocket {
        val ctx = serverSslContext ?: throw IllegalStateException("Server SSLContext not initialized")
        val ca = ClientAuth.parse(config.clientAuth.name)
        return at.rocworks.peerlink.tls.wrapServerSocket(ctx, rawSocket, byteArrayOf(firstByte), ca, sharedSecret)
    }

    fun wrapClientSocket(
        rawSocket: Socket,
        host: String,
        port: Int,
        insecureSkipVerify: Boolean,
        sharedSecret: Boolean,
        peerId: PeerIdentity = PeerIdentity(nodeId = host)
    ): SSLSocket {
        val clientOpts = ClientTlsOptions(
            certChain = keyPairAndCert?.first,
            privateKey = keyPairAndCert?.second,
            trust = trustConfig,
            peer = peerId,
            serverName = host,
            sharedSecret = sharedSecret,
            insecureSkipVerify = insecureSkipVerify
        )
        val clientCtx = createClientSSLContext(clientOpts)
        return at.rocworks.peerlink.tls.wrapClientSocket(clientCtx, rawSocket, host, port, sharedSecret)
    }

    fun exportKeyingMaterial(socket: SSLSocket): ByteArray {
        return at.rocworks.peerlink.tls.exportKeyingMaterial(socket)
    }

    fun verify(chain: Array<java.security.cert.X509Certificate>?, peer: PeerIdentity): Boolean {
        return try {
            @Suppress("UNCHECKED_CAST")
            trustConfig.verify(chain as Array<X509Certificate>?, peer, EKU_CLIENT_AUTH)
            true
        } catch (_: Exception) {
            false
        }
    }

    companion object {
        fun decodeSecret(s: String): ByteArray = at.rocworks.peerlink.tls.decodeSecret(s)
    }
}
