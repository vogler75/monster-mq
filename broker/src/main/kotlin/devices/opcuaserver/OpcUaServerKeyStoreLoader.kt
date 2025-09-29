package at.rocworks.devices.opcuaserver

import at.rocworks.Utils
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateBuilder
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateGenerator
import java.net.InetAddress
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.*
import java.security.cert.X509Certificate
import java.util.regex.Pattern

/**
 * Handles loading and generation of certificates for OPC UA Server
 */
class OpcUaServerKeyStoreLoader(
    private val config: OpcUaServerConfig
) {
    private val logger = Utils.getLogger(this::class.java)

    companion object {
        private val IP_ADDR_PATTERN = Pattern.compile(
            "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$"
        )
        private const val SERVER_ALIAS = "opcua-server"
    }

    var serverCertificate: X509Certificate? = null
        private set
    var serverCertificateChain: Array<X509Certificate>? = null
        private set
    var serverKeyPair: KeyPair? = null
        private set

    @Throws(Exception::class)
    fun load(): OpcUaServerKeyStoreLoader {
        val securityDir = getSecurityDir()
        Files.createDirectories(securityDir)
        if (!Files.exists(securityDir)) {
            throw Exception("Unable to create security dir: $securityDir")
        }

        return loadKeyStore(securityDir)
    }

    private fun getSecurityDir(): Path {
        return Paths.get(config.security.certificateDir)
    }

    @Throws(Exception::class)
    private fun loadKeyStore(baseDir: Path): OpcUaServerKeyStoreLoader {
        val keyStore = KeyStore.getInstance("PKCS12")
        // Use server-specific certificate file name
        val certificateFileName = "monstermq-opcua-server-${config.name.replace(Regex("[^a-zA-Z0-9-]"), "_")}.pfx"
        val serverKeyStore = baseDir.resolve(certificateFileName)
        val password = config.security.keystorePassword.toCharArray()

        logger.info("Loading Server KeyStore at $serverKeyStore")

        if (!Files.exists(serverKeyStore)) {
            if (!config.security.createSelfSigned) {
                throw Exception("Certificate file not found and auto-creation is disabled: $serverKeyStore")
            }

            logger.info("Creating new self-signed certificate for OPC UA Server '${config.name}'...")
            keyStore.load(null, password)

            val keyPair = SelfSignedCertificateGenerator.generateRsaKeyPair(2048)

            val builder = SelfSignedCertificateBuilder(keyPair)
                .setCommonName("MonsterMQ OPC UA Server - ${config.name}")
                .setApplicationUri("urn:MonsterMQ:OpcUaServer:${config.name}")
                .setOrganization("MonsterMQ")
                .setOrganizationalUnit("OPC UA Server")
                .setLocalityName("Vienna")
                .setCountryCode("AT")
                .addDnsName("localhost")
                .addIpAddress("127.0.0.1")

            // Add all local hostnames and IP addresses
            val localHost = InetAddress.getLocalHost()
            builder.addDnsName(localHost.hostName)
            builder.addIpAddress(localHost.hostAddress)

            // Get all network interfaces
            try {
                val interfaces = java.net.NetworkInterface.getNetworkInterfaces()
                while (interfaces.hasMoreElements()) {
                    val networkInterface = interfaces.nextElement()
                    if (networkInterface.isUp && !networkInterface.isLoopback) {
                        val addresses = networkInterface.inetAddresses
                        while (addresses.hasMoreElements()) {
                            val address = addresses.nextElement()
                            val hostAddress = address.hostAddress
                            // Skip IPv6 link-local addresses
                            if (!hostAddress.contains("%")) {
                                if (IP_ADDR_PATTERN.matcher(hostAddress).matches()) {
                                    logger.info("Adding IP to certificate: $hostAddress")
                                    builder.addIpAddress(hostAddress)
                                }
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.warning("Error getting network interfaces: ${e.message}")
            }

            val certificate = builder.build()

            keyStore.setKeyEntry(SERVER_ALIAS, keyPair.private, password, arrayOf(certificate))
            Files.newOutputStream(serverKeyStore).use { out ->
                keyStore.store(out, password)
            }
            logger.info("Self-signed certificate created successfully for OPC UA Server '${config.name}'")
        } else {
            logger.info("Loading existing certificate for OPC UA Server '${config.name}'...")
            Files.newInputStream(serverKeyStore).use { inputStream ->
                keyStore.load(inputStream, password)
            }
        }

        val serverPrivateKey = keyStore.getKey(SERVER_ALIAS, password)
        if (serverPrivateKey is PrivateKey) {
            serverCertificate = keyStore.getCertificate(SERVER_ALIAS) as X509Certificate

            serverCertificateChain = keyStore.getCertificateChain(SERVER_ALIAS)
                ?.map { it as X509Certificate }
                ?.toTypedArray() ?: arrayOf(serverCertificate!!)

            val serverPublicKey = serverCertificate!!.publicKey
            serverKeyPair = KeyPair(serverPublicKey, serverPrivateKey)
        } else {
            throw Exception("Failed to load private key from keystore")
        }

        logger.info("Certificate loaded successfully for OPC UA Server '${config.name}'.")
        return this
    }
}