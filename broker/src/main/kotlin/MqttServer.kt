package at.rocworks

import at.rocworks.auth.UserManager
import at.rocworks.handlers.SessionHandler
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.http.ClientAuth
import io.vertx.core.net.JksOptions
import io.vertx.core.net.KeyCertOptions
import io.vertx.core.net.PemKeyCertOptions
import io.vertx.core.net.PemTrustOptions
import io.vertx.core.net.PfxOptions
import io.vertx.core.net.TrustOptions

import io.vertx.mqtt.MqttServer
import io.vertx.mqtt.MqttServerOptions

class MqttServer(
    private val port: Int,
    private val ssl: Boolean,
    private val useWebSocket: Boolean,
    private val maxMessageSize: Int,
    private val tcpNoDelay: Boolean = true,
    private val receiveBufferSize: Int = 512 * 1024,
    private val sendBufferSize: Int = 512 * 1024,
    private val sessionHandler: SessionHandler,
    private val userManager: UserManager,
    private val keyStorePath: String = "server-keystore.jks",
    private val keyStorePassword: String = "password",
    private val keyStoreType: String = "JKS",
    // Private key file path, only used when keyStoreType == "PEM" (keyStorePath is then the cert/fullchain PEM path)
    private val keyPath: String = "",
    // Mutual TLS (client certificate verification). Defaults preserve prior behavior exactly: no client auth.
    private val clientAuth: String = "NONE",
    private val trustStorePath: String = "",
    private val trustStorePassword: String = "",
    private val trustStoreType: String = "JKS",
    // When true, a verified client certificate's Common Name is used as the client's
    // authenticated identity instead of a username/password. Defaults to false.
    private val useIdentityAsUsername: Boolean = false,
    // When true, a certificate Common Name without an existing user account gets one created
    // automatically with default, non-admin permissions. Defaults to false.
    private val autoCreateUser: Boolean = false
) : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private val options = MqttServerOptions().let { it ->
        it.isSsl = ssl
        it.keyCertOptions = buildKeyCertOptions(keyStorePath, keyStorePassword, keyStoreType, keyPath)
        if (clientAuth.uppercase() != "NONE") {
            if (trustStorePath.isNotBlank()) {
                it.trustOptions = buildTrustOptions(trustStorePath, trustStorePassword, trustStoreType)
                it.setClientAuth(ClientAuth.valueOf(clientAuth.uppercase()))
            } else {
                logger.warning("SSL ClientAuth is set to '$clientAuth' but no TrustStorePath is configured, so client certificates won't be checked.")
            }
        }
        it.isUseWebSocket = this.useWebSocket
        it.maxMessageSize = this.maxMessageSize
        it.isTcpNoDelay = this.tcpNoDelay      // Disable Nagle's algorithm - send packets immediately, don't coalesce
        it.receiveBufferSize = this.receiveBufferSize  // Receive buffer for burst traffic
        it.sendBufferSize = this.sendBufferSize         // Send buffer for burst traffic

        it
    }


    override fun start(startPromise: Promise<Void>) {
        val mqttServer: MqttServer = MqttServer.create(vertx, options)

        mqttServer.exceptionHandler { err ->
            val msg = err.message ?: ""
            val localizedMsg = err.localizedMessage ?: msg
            val isWrongMessageType = msg.contains("Wrong message type", ignoreCase = true)
            val isProtocolOrSslError = err is java.io.IOException ||
                    err.javaClass.name.contains("DecoderException") ||
                    err.javaClass.name.contains("SSL") ||
                    err.javaClass.name.contains("MqttUnacceptableProtocolVersionException") ||
                    err.javaClass.name.contains("MqttIdentifierRejectedException") ||
                    msg.contains("SSLHandshakeException", ignoreCase = true) ||
                    msg.contains("DecoderException", ignoreCase = true) ||
                    msg.contains("UnacceptableProtocolVersionException", ignoreCase = true) ||
                    msg.contains("connection reset", ignoreCase = true) ||
                    msg.contains("broken pipe", ignoreCase = true) ||
                    msg.contains("connection timed out", ignoreCase = true)

            if (isWrongMessageType) {
                logger.warning("MQTT WebSocket port received a standard HTTP request instead of a WebSocket handshake/MQTT traffic (e.g., a client or health check hitting the wrong port, or trying to access GraphQL/REST APIs on the MQTT WebSocket port). Message details: $localizedMsg")
            } else if (isProtocolOrSslError) {
                logger.warning("MQTT Server connection warning (possible wrong protocol, SSL issue or abrupt disconnect): $localizedMsg")
            } else {
                logger.severe("MQTT Server error: $msg [${Utils.getCurrentFunctionName()}]")
            }
        }

        mqttServer.endpointHandler { endpoint ->
            MqttClient.deployEndpoint(vertx, endpoint, sessionHandler, userManager, useIdentityAsUsername, autoCreateUser)
        }

        mqttServer.listen(port)
            .onSuccess { server ->
                val tlsLabel = when {
                    !ssl -> "   "
                    clientAuth.uppercase() != "NONE" && trustStorePath.isNotBlank() -> "mTLS"
                    else -> "TLS"
                }
                logger.info("MQTT Server is listening on port [${server.actualPort()}] [${if (useWebSocket) "WS " else "TCP"}][$tlsLabel]")
                startPromise.complete()
            }
            .onFailure { error ->
                logger.severe("Error starting MQTT Server: ${error.message} [${Utils.getCurrentFunctionName()}]")
                startPromise.fail(error)
            }
    }

    companion object {
        fun buildKeyCertOptions(path: String, password: String, type: String, keyPath: String = ""): KeyCertOptions =
            when (type.uppercase()) {
                "PKCS12", "PFX", "P12" -> PfxOptions().setPath(path).setPassword(password)
                "PEM" -> PemKeyCertOptions().setCertPath(path).setKeyPath(keyPath.ifBlank { path })
                else -> JksOptions().setPath(path).setPassword(password)
            }

        fun buildTrustOptions(path: String, password: String, type: String): TrustOptions =
            when (type.uppercase()) {
                "PKCS12", "PFX", "P12" -> PfxOptions().setPath(path).setPassword(password)
                "PEM" -> PemTrustOptions().addCertPath(path)
                else -> JksOptions().setPath(path).setPassword(password)
            }
    }
}
