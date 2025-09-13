package at.rocworks.handlers

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import com.hazelcast.cluster.MembershipEvent
import com.hazelcast.cluster.MembershipListener
import com.hazelcast.map.IMap
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
// VertxInternal removed in Vert.x 5 - using alternative approaches
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.util.concurrent.Callable
import kotlin.system.exitProcess


class HealthHandler(
    private val sessionHandler: SessionHandler
): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private var clusterManager: HazelcastClusterManager? = null
    private var clusterDataMap: IMap<String, String>? = null

    private var periodicId: Long = 0

    companion object {
        const val CLUSTER_MAP = "cluster-config"
        const val LEADER_KEY = "cluster-leader"
        const val BIRTH_KEY = "cluster-init"
    }

    override fun start(startPromise: Promise<Void>) {
        if (Monster.isClustered()) {
            clusterHealthCheck(startPromise)
        } else {
            singleInstanceCheck(startPromise)
        }
    }

    private fun singleInstanceCheck(startPromise: Promise<Void>) {
        vertx.executeBlocking(Callable {
            sessionHandler.purgeSessions()
            sessionHandler.purgeQueuedMessages()
        }).onComplete {
            logger.info("Purged sessions and queued messages")
            vertx.setPeriodic(60_000 * 10) {
                vertx.executeBlocking(Callable { sessionHandler.purgeQueuedMessages() })
            }
            startPromise.complete()
        }
    }

    private fun clusterHealthCheck(startPromise: Promise<Void>) {
        val clusterManager = Monster.getClusterManager()
        if (clusterManager != null) {
            this.clusterManager = clusterManager
            val instance = clusterManager.hazelcastInstance
            val cluster = instance.cluster

            logger.info("Cluster local: ${cluster.localMember.address} members: ${cluster.members.joinToString(", ") { it.address.toString() }}")

            instance.getMap<String, String>(CLUSTER_MAP).let { map ->
                clusterDataMap = map
                tryToBecomeLeader(map)
            }

            cluster.addMembershipListener(object: MembershipListener {
                override fun memberAdded(event: MembershipEvent) {
                    memberAddedHandler(event)
                }
                override fun memberRemoved(event: MembershipEvent) {
                    memberRemovedHandler(event)
                }
            })

            startPromise.complete()
        } else {
            startPromise.fail("Cluster manager is not Hazelcast")
        }
    }

    private fun memberAddedHandler(event: MembershipEvent) {
        val nodeId = event.member.uuid.toString()
        logger.info("Cluster member added: $nodeId")
    }

    private fun memberRemovedHandler(event: MembershipEvent) {
        val nodeId = event.member.uuid.toString()
        logger.info("Cluster member removed: $nodeId")
        if (nodeId == Monster.getClusterNodeId(vertx)) {
            logger.warning("We got removed from the cluster!")
            exitProcess(-1) // TODO: handle this properly
        } else {
            clusterDataMap?.let { map ->
                if (map.remove(LEADER_KEY, nodeId)) { // remove leader if it was the removed node
                    tryToBecomeLeader(map)
                }
                if (areWeTheLeader(map)) {
                    logger.info("We are the leader - handling dead node [$nodeId] session cleanup")
                    handleDeadNodeCleanup(nodeId)
                }
            }
        }
    }

    private fun tryToBecomeLeader(map: IMap<String, String>) {
        val nodeId = Monster.getClusterNodeId(vertx) // clusterManager!!.hazelcastInstance.cluster.localMember.uuid.toString()
        val birth = map.putIfAbsent(BIRTH_KEY, System.currentTimeMillis().toString())
        val leader = map.putIfAbsent(LEADER_KEY, nodeId)
        if (leader == null) weBecameTheLeader(birth==null)
    }

    private fun weBecameTheLeader(first: Boolean) {
        logger.info("We are the ${if (first) "first " else ""}leader now.")
        if (first) {
            sessionHandler.purgeSessions()
            sessionHandler.purgeQueuedMessages()
        }
        if (periodicId == 0L) {
            periodicId = vertx.setPeriodic(600_000) { // every 10 minutes
                vertx.executeBlocking(Callable {
                    sessionHandler.purgeQueuedMessages()
                })
            }
        }
    }

    private fun areWeTheLeader(map: IMap<String, String>): Boolean {
        val nodeId = Monster.getClusterNodeId(vertx)
        val leaderId = map[LEADER_KEY]
        logger.info("Checking if we are the leader: $nodeId == $leaderId")
        return nodeId == map[LEADER_KEY]
    }

    private fun handleDeadNodeCleanup(deadNodeId: String) {
        logger.info("Processing dead node [$deadNodeId] - cleaning up sessions")

        sessionHandler.iterateNodeClients(deadNodeId) { clientId, cleanSession, will ->
            logger.fine("Processing client [$clientId] from dead node [$deadNodeId], cleanSession=[$cleanSession]")

            // Publish last will message if present
            if (will.topicName.isNotEmpty()) {
                logger.info("Publishing last will for client [$clientId]: [${will.topicName}]")
                sessionHandler.publishMessage(will)
            }

            if (cleanSession) {
                // Clean sessions: Remove from database completely
                logger.info("Removing clean session client [$clientId] from dead node [$deadNodeId]")
                sessionHandler.delClient(clientId)
            } else {
                // Persistent sessions: Mark as offline in database (connected=false)
                logger.info("Marking persistent session client [$clientId] as offline from dead node [$deadNodeId]")
                sessionHandler.pauseClient(clientId)

                // Also propagate CLIENT_DISCONNECTED event to update client mappings
                val mappingEvent = at.rocworks.data.ClientNodeMapping(
                    clientId,
                    deadNodeId,
                    at.rocworks.data.ClientNodeMapping.EventType.NODE_FAILURE
                )
                vertx.eventBus().publish("${at.rocworks.Const.GLOBAL_CLIENT_TABLE_NAMESPACE}/M", mappingEvent)
            }
        }

        // Also send a general node failure event to clean up any remaining mappings
        val nodeFailureEvent = at.rocworks.data.ClientNodeMapping(
            "", // empty clientId for node-wide event
            deadNodeId,
            at.rocworks.data.ClientNodeMapping.EventType.NODE_FAILURE
        )
        vertx.eventBus().publish("${at.rocworks.Const.GLOBAL_CLIENT_TABLE_NAMESPACE}/M", nodeFailureEvent)

        logger.info("Completed dead node [$deadNodeId] cleanup")
    }
}