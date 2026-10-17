package at.rocworks.devices.mqttclient

import java.io.InputStream
import java.io.OutputStream
import java.net.InetAddress
import java.net.Socket
import java.net.SocketAddress
import java.net.SocketOption
import java.nio.channels.SocketChannel
import java.util.function.BiFunction
import javax.net.SocketFactory
import javax.net.ssl.HandshakeCompletedListener
import javax.net.ssl.SSLParameters
import javax.net.ssl.SSLSession
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory

/**
 * Checks if the first bytes received from a server represent an MQTT 3.1.1 CONNACK:
 * fixed header 0x20, remaining length 2. An MQTT 5 CONNACK always carries a
 * properties length varint, so its remaining length is at least 3.
 */
fun isV3Connack(head: ByteArray): Boolean {
    return head.size >= 2 && head[0] == 0x20.toByte() && head[1] == 0x02.toByte()
}

/**
 * Checks whether an exception indicates that the remote broker does not support MQTT 5.
 */
fun isUnsupportedProtocolVersion(exception: Throwable?): Boolean {
    if (exception == null) return false
    if (exception is org.eclipse.paho.mqttv5.common.MqttException) {
        val code = exception.reasonCode
        if (code == org.eclipse.paho.mqttv5.common.packet.MqttReturnCode.RETURN_CODE_UNSUPPORTED_PROTOCOL_VERSION ||
            code == org.eclipse.paho.mqttv5.common.MqttException.REASON_CODE_UNSUPPORTED_PROTOCOL_VERSION ||
            code == 1 // MQTT 3.1.1 return code 1: Connection Refused, unacceptable protocol version
        ) {
            return true
        }
    }
    val msg = exception.message?.lowercase() ?: ""
    if (msg.contains("unsupported protocol") || msg.contains("unacceptable protocol") || msg.contains("not support mqtt 5")) {
        return true
    }
    val cause = exception.cause
    if (cause != null && cause !== exception) {
        return isUnsupportedProtocolVersion(cause)
    }
    return false
}

class SniffingInputStream(
    private val delegate: InputStream,
    private val onHead: (ByteArray) -> Unit
) : InputStream() {
    private val head = ByteArray(2)
    private var headLen = 0
    private var notified = false

    override fun read(): Int {
        val b = delegate.read()
        if (b != -1 && headLen < 2) {
            head[headLen++] = b.toByte()
            checkHead()
        }
        return b
    }

    override fun read(b: ByteArray, off: Int, len: Int): Int {
        val n = delegate.read(b, off, len)
        if (n > 0 && headLen < 2) {
            val toCopy = minOf(n, 2 - headLen)
            System.arraycopy(b, off, head, headLen, toCopy)
            headLen += toCopy
            checkHead()
        }
        return n
    }

    private fun checkHead() {
        if (headLen >= 2 && !notified) {
            notified = true
            onHead(head)
        }
    }

    override fun close() = delegate.close()
    override fun available() = delegate.available()
    override fun skip(n: Long) = delegate.skip(n)
}

open class SniffingSocket(
    private val delegate: Socket,
    private val onHead: (ByteArray) -> Unit
) : Socket() {
    private var sniffingStream: SniffingInputStream? = null

    override fun getInputStream(): InputStream {
        return sniffingStream ?: SniffingInputStream(delegate.inputStream, onHead).also {
            sniffingStream = it
        }
    }

    override fun getOutputStream(): OutputStream = delegate.outputStream
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

class SniffingSslSocket(
    private val delegate: SSLSocket,
    private val onHead: (ByteArray) -> Unit
) : SSLSocket() {
    private var sniffingStream: SniffingInputStream? = null

    override fun getInputStream(): InputStream {
        return sniffingStream ?: SniffingInputStream(delegate.inputStream, onHead).also {
            sniffingStream = it
        }
    }

    override fun getOutputStream(): OutputStream = delegate.outputStream
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

    // SSLSocket specific methods
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
    override fun getSSLParameters(): SSLParameters = delegate.sslParameters
    override fun setSSLParameters(params: SSLParameters) { delegate.sslParameters = params }
    override fun getApplicationProtocol(): String? = delegate.applicationProtocol
    override fun getHandshakeApplicationProtocol(): String? = delegate.handshakeApplicationProtocol
    override fun setHandshakeApplicationProtocolSelector(selector: BiFunction<SSLSocket, MutableList<String>, String>?) {
        delegate.handshakeApplicationProtocolSelector = selector
    }
    override fun getHandshakeApplicationProtocolSelector(): BiFunction<SSLSocket, MutableList<String>, String>? =
        delegate.handshakeApplicationProtocolSelector
}

class SniffingSocketFactory(
    private val delegate: SocketFactory,
    private val onHead: (ByteArray) -> Unit
) : SocketFactory() {
    private fun wrap(socket: Socket): Socket =
        if (socket is SSLSocket) SniffingSslSocket(socket, onHead) else SniffingSocket(socket, onHead)

    override fun createSocket(): Socket = wrap(delegate.createSocket())
    override fun createSocket(host: String, port: Int): Socket = wrap(delegate.createSocket(host, port))
    override fun createSocket(host: String, port: Int, localHost: InetAddress, localPort: Int): Socket =
        wrap(delegate.createSocket(host, port, localHost, localPort))
    override fun createSocket(host: InetAddress, port: Int): Socket = wrap(delegate.createSocket(host, port))
    override fun createSocket(address: InetAddress, port: Int, localAddress: InetAddress, localPort: Int): Socket =
        wrap(delegate.createSocket(address, port, localAddress, localPort))
}

class SniffingSslSocketFactory(
    private val delegate: SSLSocketFactory,
    private val onHead: (ByteArray) -> Unit
) : SSLSocketFactory() {
    override fun getDefaultCipherSuites(): Array<String> = delegate.defaultCipherSuites
    override fun getSupportedCipherSuites(): Array<String> = delegate.supportedCipherSuites

    private fun wrap(socket: Socket): Socket =
        if (socket is SSLSocket) SniffingSslSocket(socket, onHead) else SniffingSocket(socket, onHead)

    override fun createSocket(): Socket = wrap(delegate.createSocket())
    override fun createSocket(s: Socket, host: String, port: Int, autoClose: Boolean): Socket =
        wrap(delegate.createSocket(s, host, port, autoClose))
    override fun createSocket(host: String, port: Int): Socket = wrap(delegate.createSocket(host, port))
    override fun createSocket(host: String, port: Int, localHost: InetAddress, localPort: Int): Socket =
        wrap(delegate.createSocket(host, port, localHost, localPort))
    override fun createSocket(host: InetAddress, port: Int): Socket = wrap(delegate.createSocket(host, port))
    override fun createSocket(address: InetAddress, port: Int, localAddress: InetAddress, localPort: Int): Socket =
        wrap(delegate.createSocket(address, port, localAddress, localPort))
}
