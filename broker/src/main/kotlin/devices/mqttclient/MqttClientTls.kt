package at.rocworks.devices.mqttclient

import at.rocworks.stores.devices.MqttClientConnectionConfig
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.openssl.PEMEncryptedKeyPair
import org.bouncycastle.openssl.PEMKeyPair
import org.bouncycastle.openssl.PEMParser
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo
import java.io.File
import java.io.FileInputStream
import java.io.FileReader
import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.Socket
import java.net.SocketAddress
import java.net.SocketOption
import java.nio.channels.SocketChannel
import java.security.KeyStore
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.Signature
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.function.BiFunction
import javax.net.ssl.HandshakeCompletedListener
import javax.net.ssl.KeyManager
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SNIHostName
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLParameters
import javax.net.ssl.SSLSession
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509ExtendedTrustManager

class MqttClientTlsException(message: String, cause: Throwable? = null) : Exception(message, cause)

/**
 * Builds the SSLSocketFactory for the MQTT client bridge: custom CA trust, client certificate
 * (mutual TLS), ALPN and SNI.
 */
object MqttClientTls {
    private val bcProvider by lazy { BouncyCastleProvider() }

    /**
     * Returns null when the default JVM TLS setup applies (verification on, no tls* options).
     */
    fun buildSocketFactory(config: MqttClientConnectionConfig): SSLSocketFactory? {
        if (config.sslVerifyCertificate && !config.hasTlsOptions()) return null

        val serverName = config.tlsServerName?.let {
            try {
                SNIHostName(it)
            } catch (e: IllegalArgumentException) {
                throw MqttClientTlsException("tlsServerName '$it' is not a valid host name: ${e.message}")
            }
        }

        val sslContext = SSLContext.getInstance("TLS")
        sslContext.init(loadKeyManagers(config), loadTrustManagers(config), SecureRandom())

        return ConfiguredSslSocketFactory(
            sslContext.socketFactory,
            SocketSettings(
                verify = config.sslVerifyCertificate,
                serverName = serverName,
                alpnProtocols = config.tlsAlpnProtocols?.toTypedArray()
            )
        )
    }

    private fun loadTrustManagers(config: MqttClientConnectionConfig): Array<TrustManager>? {
        if (!config.sslVerifyCertificate) return arrayOf(TrustAllManager)
        val caPath = config.tlsCaCertPath ?: return null

        val certs = readCertificates(caPath, "tlsCaCertPath")
        val trustStore = KeyStore.getInstance("PKCS12").apply { load(null, null) }
        certs.forEachIndexed { i, cert -> trustStore.setCertificateEntry("ca-$i", cert) }
        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
        tmf.init(trustStore)
        return tmf.trustManagers
    }

    private fun loadKeyManagers(config: MqttClientConnectionConfig): Array<KeyManager>? {
        val certPath = config.tlsClientCertPath ?: return null
        val password = config.tlsClientKeyPassword?.toCharArray() ?: CharArray(0)

        val keyStore = when (config.tlsClientKeyFormat) {
            MqttClientConnectionConfig.TLS_KEY_FORMAT_PKCS12 -> loadPkcs12(certPath, password)
            else -> {
                val keyPath = config.tlsClientKeyPath
                    ?: throw MqttClientTlsException("tlsClientKeyPath is required when tlsClientCertPath is set")
                val chain = readCertificates(certPath, "tlsClientCertPath")
                val key = readPrivateKey(keyPath, config.tlsClientKeyPassword)
                checkKeyMatchesCertificate(key, chain.first(), certPath, keyPath)
                KeyStore.getInstance("PKCS12").apply {
                    load(null, null)
                    setKeyEntry("client", key, password, chain.toTypedArray())
                }
            }
        }
        val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
        kmf.init(keyStore, password)
        return kmf.keyManagers
    }

    private fun readableFile(path: String, field: String): File {
        val file = File(path)
        if (!file.isFile) throw MqttClientTlsException("$field '$path': file not found")
        if (!file.canRead()) throw MqttClientTlsException("$field '$path': file is not readable")
        return file
    }

    private fun readCertificates(path: String, field: String): List<X509Certificate> {
        val file = readableFile(path, field)
        val certs = try {
            FileInputStream(file).use { input ->
                CertificateFactory.getInstance("X.509").generateCertificates(input).map { it as X509Certificate }
            }
        } catch (e: Exception) {
            throw MqttClientTlsException("$field '$path': cannot parse certificates: ${e.message}", e)
        }
        if (certs.isEmpty()) throw MqttClientTlsException("$field '$path': no certificates found")
        return certs
    }

    private fun loadPkcs12(path: String, password: CharArray): KeyStore {
        val file = readableFile(path, "tlsClientCertPath")
        val keyStore = try {
            KeyStore.getInstance("PKCS12").apply { FileInputStream(file).use { load(it, password) } }
        } catch (e: Exception) {
            throw MqttClientTlsException(
                "tlsClientCertPath '$path': cannot open PKCS12 bundle (wrong tlsClientKeyPassword?): ${e.message}", e
            )
        }
        if (keyStore.aliases().toList().none { keyStore.isKeyEntry(it) }) {
            throw MqttClientTlsException("tlsClientCertPath '$path': PKCS12 bundle contains no private key")
        }
        return keyStore
    }

    /**
     * Reads PKCS#1, SEC1 (EC), PKCS#8 and encrypted (legacy OpenSSL or PKCS#8) PEM private keys.
     */
    private fun readPrivateKey(path: String, password: String?): PrivateKey {
        val file = readableFile(path, "tlsClientKeyPath")
        val converter = JcaPEMKeyConverter()
        val pemObjects = try {
            PEMParser(FileReader(file)).use { parser -> generateSequence { parser.readObject() }.toList() }
        } catch (e: Exception) {
            throw MqttClientTlsException("tlsClientKeyPath '$path': cannot parse PEM file: ${e.message}", e)
        }

        fun requirePassword(): CharArray = password?.toCharArray()
            ?: throw MqttClientTlsException("tlsClientKeyPath '$path': private key is encrypted but no tlsClientKeyPassword is set")

        for (obj in pemObjects) {
            try {
                return when (obj) {
                    is PEMKeyPair -> converter.getPrivateKey(obj.privateKeyInfo)
                    is PrivateKeyInfo -> converter.getPrivateKey(obj)
                    is PEMEncryptedKeyPair -> converter.getPrivateKey(
                        obj.decryptKeyPair(JcePEMDecryptorProviderBuilder().setProvider(bcProvider).build(requirePassword()))
                            .privateKeyInfo
                    )
                    is PKCS8EncryptedPrivateKeyInfo -> converter.getPrivateKey(
                        obj.decryptPrivateKeyInfo(
                            JceOpenSSLPKCS8DecryptorProviderBuilder().setProvider(bcProvider).build(requirePassword())
                        )
                    )
                    else -> continue
                }
            } catch (e: MqttClientTlsException) {
                throw e
            } catch (e: Exception) {
                throw MqttClientTlsException(
                    "tlsClientKeyPath '$path': cannot decrypt or read private key (wrong tlsClientKeyPassword?): ${e.message}", e
                )
            }
        }
        throw MqttClientTlsException("tlsClientKeyPath '$path': no PEM private key found")
    }

    /**
     * A mismatched key only surfaces as an opaque handshake failure, so check it up front.
     */
    private fun checkKeyMatchesCertificate(key: PrivateKey, cert: X509Certificate, certPath: String, keyPath: String) {
        val algorithm = when (key.algorithm) {
            "RSA" -> "SHA256withRSA"
            "EC" -> "SHA256withECDSA"
            "Ed25519", "EdDSA" -> "Ed25519"
            else -> return
        }
        val data = ByteArray(32).also { SecureRandom().nextBytes(it) }
        val matches = try {
            val signature = Signature.getInstance(algorithm).run { initSign(key); update(data); sign() }
            Signature.getInstance(algorithm).run { initVerify(cert.publicKey); update(data); verify(signature) }
        } catch (e: Exception) {
            false
        }
        if (!matches) {
            throw MqttClientTlsException("tlsClientKeyPath '$keyPath' does not match the certificate in tlsClientCertPath '$certPath'")
        }
    }

    private object TrustAllManager : X509ExtendedTrustManager() {
        override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String) {}
        override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String) {}
        override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String, socket: Socket) {}
        override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String, socket: Socket) {}
        override fun checkClientTrusted(chain: Array<X509Certificate>, authType: String, engine: javax.net.ssl.SSLEngine) {}
        override fun checkServerTrusted(chain: Array<X509Certificate>, authType: String, engine: javax.net.ssl.SSLEngine) {}
        override fun getAcceptedIssuers(): Array<X509Certificate> = arrayOf()
    }

    internal class SocketSettings(
        val verify: Boolean,
        val serverName: SNIHostName?,
        val alpnProtocols: Array<String>?
    ) {
        fun apply(params: SSLParameters): SSLParameters {
            params.endpointIdentificationAlgorithm = if (verify) "HTTPS" else null
            serverName?.let { params.serverNames = listOf(it) }
            alpnProtocols?.let { params.applicationProtocols = it }
            return params
        }
    }

    internal class ConfiguredSslSocketFactory(
        private val delegate: SSLSocketFactory,
        private val settings: SocketSettings
    ) : SSLSocketFactory() {
        override fun getDefaultCipherSuites(): Array<String> = delegate.defaultCipherSuites
        override fun getSupportedCipherSuites(): Array<String> = delegate.supportedCipherSuites

        private fun wrap(socket: Socket): Socket =
            if (socket is SSLSocket) ConfiguredSslSocket(socket, settings) else socket

        override fun createSocket(): Socket = wrap(delegate.createSocket())
        override fun createSocket(s: Socket?, host: String?, port: Int, autoClose: Boolean): Socket =
            wrap(delegate.createSocket(s, host, port, autoClose))
        override fun createSocket(host: String?, port: Int): Socket = wrap(delegate.createSocket(host, port))
        override fun createSocket(host: String?, port: Int, localHost: InetAddress?, localPort: Int): Socket =
            wrap(delegate.createSocket(host, port, localHost, localPort))
        override fun createSocket(host: InetAddress?, port: Int): Socket = wrap(delegate.createSocket(host, port))
        override fun createSocket(address: InetAddress?, port: Int, localAddress: InetAddress?, localPort: Int): Socket =
            wrap(delegate.createSocket(address, port, localAddress, localPort))
    }

    /**
     * Paho's SSLNetworkModule calls setSSLParameters() with fresh SSLParameters objects before the
     * handshake, which resets ALPN and replaces SNI with the URL host. This wrapper re-applies our
     * settings on every call. Everything else is delegated.
     */
    internal class ConfiguredSslSocket(
        private val delegate: SSLSocket,
        private val settings: SocketSettings
    ) : SSLSocket() {
        init {
            delegate.sslParameters = settings.apply(delegate.sslParameters)
        }

        override fun setSSLParameters(params: SSLParameters) {
            delegate.sslParameters = settings.apply(params)
        }
        override fun getSSLParameters(): SSLParameters = delegate.sslParameters

        // SSLSocket
        override fun getSupportedCipherSuites(): Array<String> = delegate.supportedCipherSuites
        override fun getEnabledCipherSuites(): Array<String> = delegate.enabledCipherSuites
        override fun setEnabledCipherSuites(suites: Array<String>) { delegate.enabledCipherSuites = suites }
        override fun getSupportedProtocols(): Array<String> = delegate.supportedProtocols
        override fun getEnabledProtocols(): Array<String> = delegate.enabledProtocols
        override fun setEnabledProtocols(protocols: Array<String>) { delegate.enabledProtocols = protocols }
        override fun getSession(): SSLSession = delegate.session
        override fun getHandshakeSession(): SSLSession? = delegate.handshakeSession
        override fun addHandshakeCompletedListener(listener: HandshakeCompletedListener) = delegate.addHandshakeCompletedListener(listener)
        override fun removeHandshakeCompletedListener(listener: HandshakeCompletedListener) = delegate.removeHandshakeCompletedListener(listener)
        override fun startHandshake() = delegate.startHandshake()
        override fun setUseClientMode(mode: Boolean) { delegate.useClientMode = mode }
        override fun getUseClientMode(): Boolean = delegate.useClientMode
        override fun setNeedClientAuth(need: Boolean) { delegate.needClientAuth = need }
        override fun getNeedClientAuth(): Boolean = delegate.needClientAuth
        override fun setWantClientAuth(want: Boolean) { delegate.wantClientAuth = want }
        override fun getWantClientAuth(): Boolean = delegate.wantClientAuth
        override fun setEnableSessionCreation(flag: Boolean) { delegate.enableSessionCreation = flag }
        override fun getEnableSessionCreation(): Boolean = delegate.enableSessionCreation
        override fun getApplicationProtocol(): String? = delegate.applicationProtocol
        override fun getHandshakeApplicationProtocol(): String? = delegate.handshakeApplicationProtocol
        override fun setHandshakeApplicationProtocolSelector(selector: BiFunction<SSLSocket, MutableList<String>, String>?) {
            delegate.handshakeApplicationProtocolSelector = selector
        }
        override fun getHandshakeApplicationProtocolSelector(): BiFunction<SSLSocket, MutableList<String>, String>? =
            delegate.handshakeApplicationProtocolSelector

        // Socket
        override fun connect(endpoint: SocketAddress) = delegate.connect(endpoint)
        override fun connect(endpoint: SocketAddress, timeout: Int) = delegate.connect(endpoint, timeout)
        override fun bind(bindpoint: SocketAddress?) = delegate.bind(bindpoint)
        override fun getInetAddress(): InetAddress? = delegate.inetAddress
        override fun getLocalAddress(): InetAddress = delegate.localAddress
        override fun getPort(): Int = delegate.port
        override fun getLocalPort(): Int = delegate.localPort
        override fun getRemoteSocketAddress(): SocketAddress? = delegate.remoteSocketAddress
        override fun getLocalSocketAddress(): SocketAddress? = delegate.localSocketAddress
        override fun getChannel(): SocketChannel? = delegate.channel
        override fun getInputStream(): InputStream = delegate.inputStream
        override fun getOutputStream(): OutputStream = delegate.outputStream
        override fun setTcpNoDelay(on: Boolean) { delegate.tcpNoDelay = on }
        override fun getTcpNoDelay(): Boolean = delegate.tcpNoDelay
        override fun setSoLinger(on: Boolean, linger: Int) = delegate.setSoLinger(on, linger)
        override fun getSoLinger(): Int = delegate.soLinger
        override fun sendUrgentData(data: Int) = delegate.sendUrgentData(data)
        override fun setOOBInline(on: Boolean) { delegate.oobInline = on }
        override fun getOOBInline(): Boolean = delegate.oobInline
        override fun setSoTimeout(timeout: Int) { delegate.soTimeout = timeout }
        override fun getSoTimeout(): Int = delegate.soTimeout
        override fun setSendBufferSize(size: Int) { delegate.sendBufferSize = size }
        override fun getSendBufferSize(): Int = delegate.sendBufferSize
        override fun setReceiveBufferSize(size: Int) { delegate.receiveBufferSize = size }
        override fun getReceiveBufferSize(): Int = delegate.receiveBufferSize
        override fun setKeepAlive(on: Boolean) { delegate.keepAlive = on }
        override fun getKeepAlive(): Boolean = delegate.keepAlive
        override fun setTrafficClass(tc: Int) { delegate.trafficClass = tc }
        override fun getTrafficClass(): Int = delegate.trafficClass
        override fun setReuseAddress(on: Boolean) { delegate.reuseAddress = on }
        override fun getReuseAddress(): Boolean = delegate.reuseAddress
        override fun close() = delegate.close()
        override fun shutdownInput() = delegate.shutdownInput()
        override fun shutdownOutput() = delegate.shutdownOutput()
        override fun isConnected(): Boolean = delegate.isConnected
        override fun isBound(): Boolean = delegate.isBound
        override fun isClosed(): Boolean = delegate.isClosed
        override fun isInputShutdown(): Boolean = delegate.isInputShutdown
        override fun isOutputShutdown(): Boolean = delegate.isOutputShutdown
        override fun setPerformancePreferences(connectionTime: Int, latency: Int, bandwidth: Int) =
            delegate.setPerformancePreferences(connectionTime, latency, bandwidth)
        override fun <T : Any?> setOption(name: SocketOption<T>, value: T): Socket { delegate.setOption(name, value); return this }
        override fun <T : Any?> getOption(name: SocketOption<T>): T = delegate.getOption(name)
        override fun supportedOptions(): MutableSet<SocketOption<*>> = delegate.supportedOptions()
        override fun toString(): String = delegate.toString()
    }
}
