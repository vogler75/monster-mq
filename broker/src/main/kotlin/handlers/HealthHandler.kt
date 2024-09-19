package at.rocworks.handlers

import at.rocworks.Monster
import at.rocworks.Utils
import com.hazelcast.cluster.MembershipEvent
import com.hazelcast.cluster.MembershipListener
import com.hazelcast.map.IMap
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.impl.VertxInternal
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.util.concurrent.Callable
import kotlin.system.exitProcess


class HealthHandler(private val sessionHandler: SessionHandler): AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)

    private var clusterManager: HazelcastClusterManager? = null
    private var leadershipMap: IMap<String, String>? = null

    fun ourMemberUuid() = clusterManager?.hazelcastInstance?.cluster?.localMember?.uuid.toString()

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

            leadershipMap = instance.getMap(CLUSTER_MAP);
            tryToBecomeLeader()

            cluster.addMembershipListener(object: MembershipListener {
                override fun memberAdded(event: MembershipEvent) {
                    val nodeId = event.member.uuid.toString()
                    logger.info("Cluster member added: $nodeId")
                }
                override fun memberRemoved(event: MembershipEvent) {
                    val nodeId = event.member.uuid.toString()
                    logger.info("Cluster member removed: $nodeId")
                    if (nodeId == ourMemberUuid()) {
                        logger.warning("We are removed from the cluster!")
                        exitProcess(-1) // TODO: handle this properly
                    } else {
                        leadershipMap?.let { map ->
                            map.remove(LEADER_KEY, nodeId)
                            tryToBecomeLeader()
                        }
                    }
                }
            })

            startPromise.complete()
        } else {
            startPromise.fail("Cluster manager is not Hazelcast")
        }
    }

    private fun tryToBecomeLeader() {
        leadershipMap?.let { map ->
            val nodeId = clusterManager!!.hazelcastInstance.cluster.localMember.uuid.toString()
            val birth = map[BIRTH_KEY]
            map.putIfAbsent(LEADER_KEY, nodeId)
            val currentLeader = map[LEADER_KEY]
            logger.info("Current leader [$currentLeader], we are [$nodeId]")
            if (nodeId == currentLeader) {
                map[BIRTH_KEY] = System.currentTimeMillis().toString()
                weAreLeader(birth==null)
            }
        }
    }

    private fun weAreLeader(first: Boolean) {
        logger.info("We are the ${if (first) "initial " else ""}leader now.")
        if (first) {
            sessionHandler.purgeSessions()
            sessionHandler.purgeQueuedMessages()
        }
        vertx.setPeriodic(60_000) {
            vertx.executeBlocking(Callable { sessionHandler.purgeQueuedMessages() })
        }
    }
}