package at.rocworks.peerlink.wire

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.MessageDigest
import java.security.SecureRandom
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

const val ExporterLabel = "monstermq-peer/1"
const val ExporterLen = 32

private const val macLabelConsumer = "mmq-peer/1 C"
private const val macLabelSource = "mmq-peer/1 S"

private val secureRandom = SecureRandom()

// AppendLP (mac.go: lines 27-31)
fun appendLP(dst: ByteArray, x: ByteArray): ByteArray {
    val len = minOf(x.size, 0xFFFF)
    val res = ByteArray(dst.size + 2 + len)
    System.arraycopy(dst, 0, res, 0, dst.size)
    val bb = ByteBuffer.wrap(res, dst.size, 2).order(ByteOrder.LITTLE_ENDIAN)
    bb.putShort(len.toShort())
    System.arraycopy(x, 0, res, dst.size + 2, len)
    return res
}

// AppendLPString (mac.go: lines 33-38)
fun appendLPString(dst: ByteArray, s: String): ByteArray {
    val b = s.toByteArray(Charsets.UTF_8)
    return appendLP(dst, b)
}

// ConsumerMACInput (mac.go: lines 40-49)
fun consumerMACInput(
    nonceS: ByteArray,
    nonceC: ByteArray,
    consumerID: String,
    sourceID: String,
    exporter: ByteArray
): ByteArray {
    var b = ByteArray(0)
    b = appendLPString(b, macLabelConsumer)
    b = appendLP(b, nonceS)
    b = appendLP(b, nonceC)
    b = appendLPString(b, consumerID)
    b = appendLPString(b, sourceID)
    return appendLP(b, exporter)
}

// SourceMACInput (mac.go: lines 51-60)
fun sourceMACInput(
    nonceC: ByteArray,
    nonceS: ByteArray,
    sourceID: String,
    consumerID: String,
    exporter: ByteArray
): ByteArray {
    var b = ByteArray(0)
    b = appendLPString(b, macLabelSource)
    b = appendLP(b, nonceC)
    b = appendLP(b, nonceS)
    b = appendLPString(b, sourceID)
    b = appendLPString(b, consumerID)
    return appendLP(b, exporter)
}

// MAC (mac.go: lines 62-69)
fun mac(secret: ByteArray, input: ByteArray): ByteArray {
    val hmac = Mac.getInstance("HmacSHA256")
    hmac.init(SecretKeySpec(secret, "HmacSHA256"))
    return hmac.doFinal(input)
}

// MatchMAC (mac.go: lines 71-81)
fun matchMAC(secrets: List<ByteArray>, input: ByteArray, expectedMac: ByteArray): Int {
    for ((index, secret) in secrets.withIndex()) {
        val m = mac(secret, input)
        if (MessageDigest.isEqual(m, expectedMac)) {
            return index
        }
    }
    return -1
}

// NewNonce (mac.go: lines 83-88)
fun newNonce(): ByteArray {
    val n = ByteArray(NonceLen)
    secureRandom.nextBytes(n)
    return n
}
