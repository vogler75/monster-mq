package at.rocworks.devices.opcuaserver

import at.rocworks.Utils
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

/**
 * Result of certificate management operations
 */
data class CertificateManagementResult(
    val success: Boolean,
    val message: String,
    val affectedCertificates: List<OpcUaServerCertificate>
)

/**
 * Manages certificate trust operations for OPC UA servers
 */
class OpcUaServerCertificateManager {
    private val logger = Utils.getLogger(this::class.java)
    private val scanner = OpcUaServerCertificateScanner()

    /**
     * Trust certificates (move from rejected to trusted directory)
     */
    fun trustCertificates(serverName: String, securityDir: String, fingerprints: List<String>): CertificateManagementResult {
        val certificates = scanner.scanCertificates(serverName, securityDir)
        val untrustedCerts = certificates.filter { !it.trusted && fingerprints.contains(it.fingerprint) }

        if (untrustedCerts.isEmpty()) {
            return CertificateManagementResult(
                success = false,
                message = "No untrusted certificates found with the specified fingerprints",
                affectedCertificates = emptyList()
            )
        }

        val trustedDir = Paths.get(securityDir).resolve("trusted-$serverName").resolve("trusted").resolve("certs")
        // Eclipse Milo places rejected certificates in rejected/ directly, not in rejected/certs/
        val rejectedDir = Paths.get(securityDir).resolve("trusted-$serverName").resolve("rejected")

        try {
            // Ensure trusted directory exists
            Files.createDirectories(trustedDir)

            val movedCertificates = mutableListOf<OpcUaServerCertificate>()

            untrustedCerts.forEach { cert ->
                val sourceFile = Paths.get(cert.filePath)
                val targetFile = trustedDir.resolve(sourceFile.fileName)

                if (Files.exists(sourceFile)) {
                    // Move certificate file
                    Files.move(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING)

                    // Update certificate object with new trust status and path
                    val trustedCert = cert.copy(
                        trusted = true,
                        filePath = targetFile.toString()
                    )
                    movedCertificates.add(trustedCert)

                    logger.info("Moved certificate ${cert.fingerprint} to trusted directory for server $serverName")
                } else {
                    logger.warning("Certificate file not found: ${cert.filePath}")
                }
            }

            return CertificateManagementResult(
                success = true,
                message = "Successfully trusted ${movedCertificates.size} certificate(s)",
                affectedCertificates = movedCertificates
            )

        } catch (e: Exception) {
            logger.severe("Error trusting certificates: ${e.message}")
            return CertificateManagementResult(
                success = false,
                message = "Error trusting certificates: ${e.message}",
                affectedCertificates = emptyList()
            )
        }
    }


    /**
     * Delete certificates completely (from both trusted and untrusted directories)
     */
    fun deleteCertificates(serverName: String, securityDir: String, fingerprints: List<String>): CertificateManagementResult {
        val certificates = scanner.scanCertificates(serverName, securityDir)
        val certsToDelete = certificates.filter { fingerprints.contains(it.fingerprint) }

        if (certsToDelete.isEmpty()) {
            return CertificateManagementResult(
                success = false,
                message = "No certificates found with the specified fingerprints",
                affectedCertificates = emptyList()
            )
        }

        try {
            val deletedCertificates = mutableListOf<OpcUaServerCertificate>()

            certsToDelete.forEach { cert ->
                val certFile = Paths.get(cert.filePath)

                if (Files.exists(certFile)) {
                    // Delete certificate file
                    Files.delete(certFile)
                    deletedCertificates.add(cert)

                    val status = if (cert.trusted) "trusted" else "untrusted"
                    logger.info("Deleted $status certificate ${cert.fingerprint} for server $serverName")
                } else {
                    logger.warning("Certificate file not found: ${cert.filePath}")
                }
            }

            return CertificateManagementResult(
                success = true,
                message = "Successfully deleted ${deletedCertificates.size} certificate(s) from both trusted and untrusted directories",
                affectedCertificates = deletedCertificates
            )

        } catch (e: Exception) {
            logger.severe("Error deleting certificates: ${e.message}")
            return CertificateManagementResult(
                success = false,
                message = "Error deleting certificates: ${e.message}",
                affectedCertificates = emptyList()
            )
        }
    }

    /**
     * Get certificates for a specific server
     */
    fun getCertificates(serverName: String, securityDir: String, trustedFilter: Boolean? = null): List<OpcUaServerCertificate> {
        val allCertificates = scanner.scanCertificates(serverName, securityDir)

        return when (trustedFilter) {
            true -> allCertificates.filter { it.trusted }
            false -> allCertificates.filter { !it.trusted }
            null -> allCertificates
        }
    }

    /**
     * Ensure certificate directories exist for a server
     */
    fun ensureDirectoriesExist(serverName: String, securityDir: String) {
        try {
            val baseDir = Paths.get(securityDir)
            val trustedDir = baseDir.resolve("trusted-$serverName").resolve("trusted").resolve("certs")
            // Eclipse Milo places rejected certificates in rejected/ directly
            val rejectedDir = baseDir.resolve("trusted-$serverName").resolve("rejected")
            // Also create the certs subdirectory for compatibility
            val rejectedCertsDir = rejectedDir.resolve("certs")

            Files.createDirectories(trustedDir)
            Files.createDirectories(rejectedDir)
            Files.createDirectories(rejectedCertsDir)

            logger.info("Ensured certificate directories exist for server $serverName")
        } catch (e: Exception) {
            logger.warning("Error creating certificate directories for server $serverName: ${e.message}")
        }
    }
}