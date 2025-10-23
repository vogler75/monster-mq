package at.rocworks.data

import java.io.Serializable

/**
 * Bulk message wrapper for delivering multiple BrokerMessages to a local client in a single send.
 * Used when BulkMessaging is enabled to reduce eventBus overhead.
 *
 * @param messages List of BrokerMessages to deliver in bulk
 */
data class BulkClientMessage(
    val messages: List<BrokerMessage>
) : Serializable

/**
 * Bulk message wrapper for delivering multiple BrokerMessages to a remote node in a single send.
 * Used when BulkMessaging is enabled to reduce inter-node communication overhead.
 *
 * @param messages List of BrokerMessages to deliver in bulk to the remote node
 */
data class BulkNodeMessage(
    val messages: List<BrokerMessage>
) : Serializable
