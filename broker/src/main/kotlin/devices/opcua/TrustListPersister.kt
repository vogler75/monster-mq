package at.rocworks.devices.opcua

import at.rocworks.Utils
import org.eclipse.milo.opcua.stack.core.security.MemoryTrustListManager
import java.io.ByteArrayInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate

/**
 * Loads/persists a client trust list across the on-disk `trusted/certs` and
 * `issuers/certs` directories that we used with Milo 0.6.16's `DefaultTrustListManager`.
 * Milo 1.x replaced that filesystem-aware default with a memory-backed `MemoryTrustListManager`,
 * so we read PEM/DER files into memory at startup and write back any newly auto-accepted
 * certificate so it persists across restarts.
 */
class TrustListPersister(
    private val trustedCertsDir: Path,
    private val issuersCertsDir: Path
) {
    private val logger = Utils.getLogger(this::class.java)

    fun loadInto(manager: MemoryTrustListManager) {
        manager.trustedCertificates = readCerts(trustedCertsDir)
        manager.issuerCertificates = readCerts(issuersCertsDir)
    }

    fun persistTrusted(cert: X509Certificate) {
        try {
            Files.createDirectories(trustedCertsDir)
            val name = "${certThumbprint(cert)}.der"
            val target = trustedCertsDir.resolve(name)
            if (!Files.exists(target)) Files.write(target, cert.encoded)
        } catch (e: Exception) {
            logger.warning("Failed to persist trusted certificate: ${e.message}")
        }
    }

    private fun readCerts(dir: Path): MutableList<X509Certificate> {
        val out = mutableListOf<X509Certificate>()
        if (!Files.exists(dir)) return out
        val cf = CertificateFactory.getInstance("X.509")
        Files.list(dir).use { paths ->
            paths.filter { Files.isRegularFile(it) }
                .forEach { p ->
                    try {
                        val bytes = Files.readAllBytes(p)
                        val cert = cf.generateCertificate(ByteArrayInputStream(bytes)) as X509Certificate
                        out.add(cert)
                    } catch (e: Exception) {
                        logger.warning("Skipping unreadable certificate ${p.fileName}: ${e.message}")
                    }
                }
        }
        return out
    }

    private fun certThumbprint(cert: X509Certificate): String {
        val sha1 = MessageDigest.getInstance("SHA-1").digest(cert.encoded)
        return sha1.joinToString("") { "%02x".format(it) }
    }
}
