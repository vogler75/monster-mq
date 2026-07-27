import at.rocworks.devices.opcuaserver.OpcUaServerNetworkIdentity
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test
import java.net.InetAddress
import java.net.UnknownHostException

class OpcUaServerNetworkIdentityTest {
    @Test
    fun `configured hostname takes precedence`() {
        val resolution = OpcUaServerNetworkIdentity.resolve(
            configuredHostname = "opcua.example.com",
            localHostnameProvider = { error("must not be called") },
            localAddressesProvider = { error("must not be called") }
        )

        assertEquals("opcua.example.com", resolution.hostname)
        assertNull(resolution.autoDetectionError)
    }

    @Test
    fun `resolved local hostname is used`() {
        val resolution = OpcUaServerNetworkIdentity.resolve(
            configuredHostname = null,
            localHostnameProvider = { "monster" },
            localAddressesProvider = { emptyList() }
        )

        assertEquals("monster", resolution.hostname)
        assertNull(resolution.autoDetectionError)
    }

    @Test
    fun `non-loopback address is used when local hostname cannot be resolved`() {
        val resolution = OpcUaServerNetworkIdentity.resolve(
            configuredHostname = null,
            localHostnameProvider = { throw UnknownHostException("monster") },
            localAddressesProvider = {
                listOf(
                    InetAddress.getByAddress(byteArrayOf(10, 20, 30, 40)),
                    InetAddress.getByAddress(byteArrayOf(127, 0, 0, 1))
                )
            }
        )

        assertEquals("10.20.30.40", resolution.hostname)
        assertNotNull(resolution.autoDetectionError)
    }

    @Test
    fun `localhost is used when no network identity is available`() {
        val resolution = OpcUaServerNetworkIdentity.resolve(
            configuredHostname = null,
            localHostnameProvider = { throw UnknownHostException("monster") },
            localAddressesProvider = { emptyList() }
        )

        assertEquals("localhost", resolution.hostname)
        assertNotNull(resolution.autoDetectionError)
    }
}
