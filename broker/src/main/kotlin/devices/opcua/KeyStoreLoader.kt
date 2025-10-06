package at.rocworks.devices.opcua

import at.rocworks.Utils
import at.rocworks.stores.devices.CertificateConfig
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateBuilder
import org.eclipse.milo.opcua.stack.core.util.SelfSignedCertificateGenerator
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.*
import java.security.cert.X509Certificate
import java.util.regex.Pattern

class KeyStoreLoader(
    private val config: CertificateConfig,
    private val deviceIdentifier: String? = null
) {
    private val logger = Utils.getLogger(this::class.java)

    companion object {
        private val IP_ADDR_PATTERN = Pattern.compile(
            "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$"
        )

        private const val CLIENT_ALIAS = "client-ai"
    }

    var clientCertificate: X509Certificate? = null
        private set
    var clientCertificateChain: Array<X509Certificate>? = null
        private set
    var clientKeyPair: KeyPair? = null
        private set

    @Throws(Exception::class)
    fun load(): KeyStoreLoader {
        val securityDir = getSecurityDir()
        Files.createDirectories(securityDir)
        if (!Files.exists(securityDir)) {
            throw Exception("Unable to create security dir: $securityDir")
        }

        return loadKeyStore(securityDir)
    }

    private fun getSecurityDir(): Path {
        return Paths.get(config.securityDir)
    }

    @Throws(Exception::class)
    private fun loadKeyStore(baseDir: Path): KeyStoreLoader {
        val keyStore = KeyStore.getInstance("PKCS12")
        // Use device-specific certificate file name to avoid conflicts
        val certificateFileName = if (deviceIdentifier != null) {
            "monstermq-opcua-client-${deviceIdentifier.replace(Regex("[^a-zA-Z0-9-]"), "_")}.pfx"
        } else {
            "monstermq-opcua-client.pfx"
        }
        val serverKeyStore = baseDir.resolve(certificateFileName)
        val password = config.keystorePassword.toCharArray()

        logger.info("Loading Client KeyStore at $serverKeyStore")

        if (!Files.exists(serverKeyStore)) {
            if (!config.createSelfSigned) {
                throw Exception("Certificate file not found and auto-creation is disabled: $serverKeyStore")
            }

            logger.info("Create new self-signed certificate...")
            keyStore.load(null, password)

            val keyPair = SelfSignedCertificateGenerator.generateRsaKeyPair(2048)

            val builder = SelfSignedCertificateBuilder(keyPair)
                .setCommonName(config.applicationName)
                .setApplicationUri(config.applicationUri)
                .setOrganization(config.organization)
                .setOrganizationalUnit(config.organizationalUnit)
                .setLocalityName(config.localityName)
                .setCountryCode(config.countryCode)
                .addDnsName("localhost")
                .addIpAddress("127.0.0.1")

            // Get as many hostnames and IP addresses as we can listed in the certificate.
            for (hostname in HostnameUtil.getHostnames("0.0.0.0")) {
                if (IP_ADDR_PATTERN.matcher(hostname).matches()) {
                    logger.info("Adding IP: $hostname")
                    builder.addIpAddress(hostname)
                } else {
                    logger.info("Adding DNS: $hostname")
                    builder.addDnsName(hostname)
                }
            }

            val certificate = builder.build()

            keyStore.setKeyEntry(CLIENT_ALIAS, keyPair.private, password, arrayOf(certificate))
            Files.newOutputStream(serverKeyStore).use { out ->
                keyStore.store(out, password)
            }
        } else {
            logger.info("Load existing certificate...")
            Files.newInputStream(serverKeyStore).use { inputStream ->
                keyStore.load(inputStream, password)
            }
        }

        val serverPrivateKey = keyStore.getKey(CLIENT_ALIAS, password)
        if (serverPrivateKey is PrivateKey) {
            clientCertificate = keyStore.getCertificate(CLIENT_ALIAS) as X509Certificate

            clientCertificateChain = keyStore.getCertificateChain(CLIENT_ALIAS)
                .map { it as X509Certificate }
                .toTypedArray()

            val serverPublicKey = clientCertificate!!.publicKey
            clientKeyPair = KeyPair(serverPublicKey, serverPrivateKey)
        }

        logger.info("Certificate loaded successfully.")
        return this
    }
}