package at.rocworks.devices.opcuaserver

import at.rocworks.Utils
import java.io.ByteArrayInputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.MessageDigest
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.time.Instant

/**
 * Represents a certificate found in the file system
 */
data class OpcUaServerCertificate(
    val serverName: String,
    val fingerprint: String,
    val subject: String,
    val issuer: String,
    val validFrom: String,
    val validTo: String,
    val trusted: Boolean,
    val filePath: String,
    val firstSeen: String
)

/**
 * Scanner for OPC UA server certificates in trusted and untrusted directories
 */
class OpcUaServerCertificateScanner {
    private val logger = Utils.getLogger(this::class.java)

    /**
     * Scan certificates for a specific OPC UA server
     */
    fun scanCertificates(serverName: String, securityDir: String): List<OpcUaServerCertificate> {
        val certificates = mutableListOf<OpcUaServerCertificate>()
        val baseDir = Paths.get(securityDir)

        // Scan trusted certificates in trusted-{serverName}/trusted/certs/
        val trustedDir = baseDir.resolve("trusted-$serverName").resolve("trusted").resolve("certs")
        if (Files.exists(trustedDir)) {
            certificates.addAll(scanDirectory(trustedDir, serverName, trusted = true))
        }

        // Scan untrusted/rejected certificates in trusted-{serverName}/rejected/certs/
        val rejectedDir = baseDir.resolve("trusted-$serverName").resolve("rejected").resolve("certs")
        if (Files.exists(rejectedDir)) {
            certificates.addAll(scanDirectory(rejectedDir, serverName, trusted = false))
        }

        return certificates
    }

    /**
     * Scan all certificates for all OPC UA servers
     */
    fun scanAllCertificates(securityDir: String): Map<String, List<OpcUaServerCertificate>> {
        val results = mutableMapOf<String, List<OpcUaServerCertificate>>()
        val baseDir = Paths.get(securityDir)

        if (!Files.exists(baseDir)) {
            return results
        }

        try {
            // Find all trusted-* directories
            val serverNames = mutableSetOf<String>()

            Files.list(baseDir).use { paths ->
                paths.filter { Files.isDirectory(it) }
                    .forEach { dir ->
                        val dirName = dir.fileName.toString()
                        if (dirName.startsWith("trusted-")) {
                            serverNames.add(dirName.substring("trusted-".length))
                        }
                    }
            }

            // Scan certificates for each server
            serverNames.forEach { serverName ->
                results[serverName] = scanCertificates(serverName, securityDir)
            }

        } catch (e: Exception) {
            logger.warning("Error scanning certificate directories: ${e.message}")
        }

        return results
    }

    private fun scanDirectory(directory: Path, serverName: String, trusted: Boolean): List<OpcUaServerCertificate> {
        val certificates = mutableListOf<OpcUaServerCertificate>()

        try {
            if (!Files.exists(directory)) {
                return certificates
            }

            Files.list(directory).use { paths ->
                paths.filter { Files.isRegularFile(it) }
                    .filter { it.fileName.toString().endsWith(".crt") || it.fileName.toString().endsWith(".pem") || it.fileName.toString().endsWith(".der") }
                    .forEach { certFile ->
                        try {
                            val certificate = parseCertificate(certFile, serverName, trusted)
                            if (certificate != null) {
                                certificates.add(certificate)
                            }
                        } catch (e: Exception) {
                            logger.warning("Error parsing certificate ${certFile.fileName}: ${e.message}")
                        }
                    }
            }
        } catch (e: Exception) {
            logger.warning("Error scanning directory $directory: ${e.message}")
        }

        return certificates
    }

    private fun parseCertificate(certFile: Path, serverName: String, trusted: Boolean): OpcUaServerCertificate? {
        try {
            val certBytes = Files.readAllBytes(certFile)
            val certFactory = CertificateFactory.getInstance("X.509")

            // Try to parse as X.509 certificate
            val x509Cert = certFactory.generateCertificate(ByteArrayInputStream(certBytes)) as X509Certificate

            // Generate fingerprint (SHA-1)
            val digest = MessageDigest.getInstance("SHA-1")
            val fingerprint = digest.digest(x509Cert.encoded)
                .joinToString(":") { "%02X".format(it) }

            // Get certificate details
            val subject = x509Cert.subjectDN.toString()
            val issuer = x509Cert.issuerDN.toString()
            val validFrom = x509Cert.notBefore.toInstant().toString()
            val validTo = x509Cert.notAfter.toInstant().toString()

            // Get file creation time as firstSeen
            val firstSeen = try {
                Files.getLastModifiedTime(certFile).toInstant().toString()
            } catch (e: Exception) {
                Instant.now().toString()
            }

            return OpcUaServerCertificate(
                serverName = serverName,
                fingerprint = fingerprint,
                subject = subject,
                issuer = issuer,
                validFrom = validFrom,
                validTo = validTo,
                trusted = trusted,
                filePath = certFile.toString(),
                firstSeen = firstSeen
            )

        } catch (e: Exception) {
            logger.warning("Failed to parse certificate ${certFile.fileName}: ${e.message}")
            return null
        }
    }
}