package at.rocworks.devices.opcua

import at.rocworks.Utils
import java.net.Inet4Address
import java.net.InetAddress
import java.net.NetworkInterface
import java.net.SocketException
import java.net.UnknownHostException
import java.util.Collections
import java.util.logging.Logger

object HostnameUtil {
    private val logger = Utils.getLogger(this::class.java)

    /**
     * @return the local hostname, if possible. Failure results in "localhost".
     */
    fun getHostname(): String {
        return try {
            InetAddress.getLocalHost().hostName
        } catch (e: UnknownHostException) {
            "localhost"
        }
    }

    /**
     * Given an address resolve it to as many unique addresses or hostnames as can be found.
     *
     * @param address the address to resolve.
     * @return the addresses and hostnames that were resolved from [address].
     */
    fun getHostnames(address: String): Set<String> {
        return getHostnames(address, true)
    }

    /**
     * Given an address resolve it to as many unique addresses or hostnames as can be found.
     *
     * @param address         the address to resolve.
     * @param includeLoopback if `true` loopback addresses will be included in the returned set.
     * @return the addresses and hostnames that were resolved from [address].
     */
    fun getHostnames(address: String, includeLoopback: Boolean): Set<String> {
        val hostnames = mutableSetOf<String>()

        try {
            val inetAddress = InetAddress.getByName(address)

            if (inetAddress.isAnyLocalAddress) {
                try {
                    val nis = NetworkInterface.getNetworkInterfaces()

                    for (ni in Collections.list(nis)) {
                        Collections.list(ni.inetAddresses).forEach { ia ->
                            if (ia is Inet4Address) {
                                if (includeLoopback || !ia.isLoopbackAddress) {
                                    hostnames.add(ia.hostName)
                                    hostnames.add(ia.hostAddress)
                                    hostnames.add(ia.canonicalHostName)
                                }
                            }
                        }
                    }
                } catch (e: SocketException) {
                    logger.warning("Failed to NetworkInterfaces for bind address: $address ${e.message}")
                }
            } else {
                if (includeLoopback || !inetAddress.isLoopbackAddress) {
                    hostnames.add(inetAddress.hostName)
                    hostnames.add(inetAddress.hostAddress)
                    hostnames.add(inetAddress.canonicalHostName)
                }
            }
        } catch (e: UnknownHostException) {
            logger.warning("Failed to get InetAddress for bind address: $address ${e.message}")
        }

        return hostnames
    }
}