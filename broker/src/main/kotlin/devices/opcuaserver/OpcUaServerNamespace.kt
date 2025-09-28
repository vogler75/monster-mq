package at.rocworks.devices.opcuaserver

import org.eclipse.milo.opcua.sdk.server.Lifecycle
import org.eclipse.milo.opcua.sdk.server.LifecycleManager
import org.eclipse.milo.opcua.sdk.server.OpcUaServer
import org.eclipse.milo.opcua.sdk.server.api.AddressSpaceComposite
import org.eclipse.milo.opcua.sdk.server.api.Namespace
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort

/**
 * MonsterMQ OPC UA Namespace following the gateway pattern
 */
class OpcUaServerNamespace(server: OpcUaServer, namespaceUri: String) : AddressSpaceComposite(server), Namespace, Lifecycle {

    private val lifecycleManager = LifecycleManager()
    private val namespaceIndex: UShort = server.namespaceTable.addUri(namespaceUri)

    init {
        lifecycleManager.addLifecycle(object : Lifecycle {
            override fun startup() {
                server.addressSpaceManager.register(this@OpcUaServerNamespace)
            }

            override fun shutdown() {
                server.addressSpaceManager.unregister(this@OpcUaServerNamespace)
            }
        })
    }

    override fun startup() {
        lifecycleManager.startup()
    }

    override fun shutdown() {
        lifecycleManager.shutdown()
    }

    override fun getNamespaceUri(): String {
        return namespaceUri
    }

    override fun getNamespaceIndex(): UShort {
        return namespaceIndex
    }
}