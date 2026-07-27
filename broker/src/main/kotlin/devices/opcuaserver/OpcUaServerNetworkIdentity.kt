package at.rocworks.devices.opcuaserver

import java.net.Inet4Address
import java.net.InetAddress
import java.net.NetworkInterface
import java.util.Collections

/**
 * Resolves the hostname advertised by OPC UA server endpoints without requiring
 * the operating-system hostname to be present in DNS or /etc/hosts.
 */
internal object OpcUaServerNetworkIdentity {
    data class Resolution(
        val hostname: String,
        val autoDetectionError: String? = null
    )

    fun resolve(configuredHostname: String?): Resolution {
        return resolve(
            configuredHostname = configuredHostname,
            localHostnameProvider = { InetAddress.getLocalHost().hostName },
            localAddressesProvider = ::getLocalAddresses
        )
    }

    internal fun resolve(
        configuredHostname: String?,
        localHostnameProvider: () -> String,
        localAddressesProvider: () -> List<InetAddress>
    ): Resolution {
        configuredHostname?.trim()?.takeIf { it.isNotEmpty() }?.let {
            return Resolution(it)
        }

        return try {
            val hostname = localHostnameProvider().trim()
            if (hostname.isNotEmpty()) {
                Resolution(hostname)
            } else {
                Resolution(selectFallbackAddress(localAddressesProvider()) ?: "localhost")
            }
        } catch (e: Exception) {
            Resolution(
                hostname = selectFallbackAddress(localAddressesProvider()) ?: "localhost",
                autoDetectionError = e.message ?: e.javaClass.simpleName
            )
        }
    }

    fun getLocalAddresses(): List<InetAddress> {
        return try {
            Collections.list(NetworkInterface.getNetworkInterfaces())
                .asSequence()
                .filter { networkInterface ->
                    runCatching { networkInterface.isUp && !networkInterface.isLoopback }
                        .getOrDefault(false)
                }
                .flatMap { Collections.list(it.inetAddresses).asSequence() }
                .filterNot { it.isAnyLocalAddress || it.isLoopbackAddress }
                .filterNot { it.hostAddress.contains("%") }
                .distinctBy { it.hostAddress }
                .toList()
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun selectFallbackAddress(addresses: List<InetAddress>): String? {
        return addresses
            .sortedWith(
                compareByDescending<InetAddress> { it.isSiteLocalAddress }
                    .thenByDescending { it is Inet4Address }
                    .thenBy { it.hostAddress }
            )
            .firstOrNull()
            ?.hostAddress
    }
}
