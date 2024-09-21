package at.rocworks.handlers

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import com.hazelcast.cluster.MembershipEvent
import com.hazelcast.cluster.MembershipListener
import com.hazelcast.map.IMap
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.impl.VertxInternal
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.util.concurrent.Callable
import kotlin.system.exitProcess


class HealthHandler(
    private val sessionHandler: SessionHandler,
    private val eventHandler: EventHandler
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
        val clusterManager = (vertx as VertxInternal).clusterManager
        if (clusterManager is HazelcastClusterManager) {
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
        if (nodeId == Utils.getClusterNodeId(vertx)) {
            logger.warning("We got removed from the cluster!")
            exitProcess(-1) // TODO: handle this properly
        } else {
            clusterDataMap?.let { map ->
                if (map.remove(LEADER_KEY, nodeId)) { // remove leader if it was the removed node
                    tryToBecomeLeader(map)
                }
                if (areWeTheLeader(map)) {
                    logger.info("We are still the leader")
                    sessionHandler.iterateNodeClients(nodeId) { clientId, cleanSession, will ->
                        eventHandler.publishMessage(will)
                        if (cleanSession)
                            sessionHandler.delClient(clientId)
                        else
                            sessionHandler.pauseClient(clientId)
                    }
                }
            }
        }
    }

    private fun tryToBecomeLeader(map: IMap<String, String>) {
        val nodeId = Utils.getClusterNodeId(vertx) // clusterManager!!.hazelcastInstance.cluster.localMember.uuid.toString()
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
        val nodeId = Utils.getClusterNodeId(vertx)
        val leaderId = map[LEADER_KEY]
        logger.info("Checking if we are the leader: $nodeId == $leaderId")
        return nodeId == map[LEADER_KEY]
    }
}