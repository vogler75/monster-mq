package at.rocworks.handlers

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import com.hazelcast.cluster.MembershipEvent
import com.hazelcast.cluster.MembershipListener
import com.hazelcast.map.IMap
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.Vertx
// VertxInternal removed in Vert.x 5 - using alternative approaches
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
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

        @Volatile
        private var cachedLeaderNodeId: String? = null

        /**
         * Gets the nodeId of the cluster leader.
         * In a single-node setup (non-clustered), returns the local node ID.
         * In a clustered setup, returns the leader nodeId from the cluster-config map.
         * Returns null if no leader is found or error occurs.
         */
        fun getLeaderNodeId(vertx: Vertx): String? {
            if (!Monster.isClustered()) {
                return Monster.getClusterNodeId(vertx)
            }
            return cachedLeaderNodeId
        }

        /**
         * Checks if the current node is the cluster leader.
         * Convenience function that compares the current node ID with the leader nodeId.
         */
        fun isLeader(vertx: Vertx): Boolean {
            val leaderNodeId = getLeaderNodeId(vertx)
            val currentNodeId = Monster.getClusterNodeId(vertx)
            val result = leaderNodeId == currentNodeId
            Utils.getLogger(HealthHandler::class.java).fine("isLeader check - currentNodeId: $currentNodeId, leaderNodeId: $leaderNodeId, result: $result")
            return result
        }
    }

    override fun start(startPromise: Promise<Void>) {
        if (Monster.isClustered()) {
            clusterHealthCheck(startPromise)
        } else {
            singleInstanceCheck(startPromise)
        }
    }

    private fun singleInstanceCheck(startPromise: Promise<Void>) {
        sessionHandler.purgeSessions().compose {
            sessionHandler.purgeQueuedMessages()
        }.onComplete {
            logger.fine("Purged sessions and queued messages")
            vertx.setPeriodic(60_000 * 10) {
                sessionHandler.purgeQueuedMessages()
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

            vertx.executeBlocking(java.util.concurrent.Callable<Void> {
                try {
                    val map = instance.getMap<String, String>(CLUSTER_MAP)
                    clusterDataMap = map

                    // Read current leader from map and cache it
                    cachedLeaderNodeId = map[LEADER_KEY]

                    // Register listener to keep cachedLeaderNodeId in sync
                    map.addEntryListener(object : com.hazelcast.core.EntryListener<String, String> {
                        override fun entryAdded(event: com.hazelcast.core.EntryEvent<String, String>) {
                            if (event.key == LEADER_KEY) {
                                cachedLeaderNodeId = event.value
                                logger.info("Cluster leader added: $cachedLeaderNodeId")
                            }
                        }
                        override fun entryUpdated(event: com.hazelcast.core.EntryEvent<String, String>) {
                            if (event.key == LEADER_KEY) {
                                cachedLeaderNodeId = event.value
                                logger.info("Cluster leader updated: $cachedLeaderNodeId")
                            }
                        }
                        override fun entryRemoved(event: com.hazelcast.core.EntryEvent<String, String>) {
                            if (event.key == LEADER_KEY) {
                                cachedLeaderNodeId = null
                                logger.info("Cluster leader removed")
                            }
                        }
                        override fun entryEvicted(event: com.hazelcast.core.EntryEvent<String, String>) {
                            if (event.key == LEADER_KEY) {
                                cachedLeaderNodeId = null
                                logger.info("Cluster leader evicted")
                            }
                        }
                        override fun entryExpired(event: com.hazelcast.core.EntryEvent<String, String>) {
                            if (event.key == LEADER_KEY) {
                                cachedLeaderNodeId = null
                                logger.info("Cluster leader expired")
                            }
                        }
                        override fun mapCleared(event: com.hazelcast.map.MapEvent) {
                            cachedLeaderNodeId = null
                            logger.info("Cluster map cleared")
                        }
                        override fun mapEvicted(event: com.hazelcast.map.MapEvent) {
                            cachedLeaderNodeId = null
                            logger.info("Cluster map evicted")
                        }
                    }, true)

                    // Try to become leader
                    tryToBecomeLeader(map)
                } catch (e: Exception) {
                    logger.severe("Failed during cluster health check initialization: ${e.message}")
                }
                null
            }).onComplete { result ->
                if (result.succeeded()) {
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
                    startPromise.fail(result.cause())
                }
            }
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
                vertx.executeBlocking(java.util.concurrent.Callable<Void> {
                    try {
                        if (map.remove(LEADER_KEY, nodeId)) { // remove leader if it was the removed node
                            tryToBecomeLeader(map)
                        }
                        if (areWeTheLeader(map)) {
                            logger.info("We are the leader - handling dead node [$nodeId] session cleanup")
                            vertx.runOnContext {
                                handleDeadNodeCleanup(nodeId)
                            }
                        }
                    } catch (e: Exception) {
                        logger.severe("Error handling member removal in Hazelcast: ${e.message}")
                    }
                    null
                })
            }
        }
    }

    private fun tryToBecomeLeader(map: IMap<String, String>) {
        val nodeId = Monster.getClusterNodeId(vertx) // clusterManager!!.hazelcastInstance.cluster.localMember.uuid.toString()
        logger.info("Attempting to become leader. Current nodeId: $nodeId")
        val birth = map.putIfAbsent(BIRTH_KEY, System.currentTimeMillis().toString())
        val leader = map.putIfAbsent(LEADER_KEY, nodeId)
        val currentLeader = map.get(LEADER_KEY)
        logger.info("Leader election result - Previous leader value: $leader, Current nodeId: $nodeId, Map value after putIfAbsent: $currentLeader")
        cachedLeaderNodeId = currentLeader
        if (leader == null) {
            logger.info("Successfully became leader!")
            vertx.runOnContext {
                weBecameTheLeader(birth == null)
            }
        } else {
            logger.info("Another node is already the leader: $leader")
        }
    }

    private fun weBecameTheLeader(first: Boolean) {
        logger.info("We are the ${if (first) "first " else ""}leader now.")
        if (first) {
            sessionHandler.purgeSessions().compose {
                sessionHandler.purgeQueuedMessages()
            }
        }
        if (periodicId == 0L) {
            periodicId = vertx.setPeriodic(600_000) { // every 10 minutes
                sessionHandler.purgeQueuedMessages()
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
            logger.fine { "Processing client [$clientId] from dead node [$deadNodeId], cleanSession=[$cleanSession]" }

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

                // Client mapping will be cleaned up by handleNodeFailure call below
            }
        }

        // Clean up all client-node mappings for the failed node
        sessionHandler.handleNodeFailure(deadNodeId)

        logger.info("Completed dead node [$deadNodeId] cleanup")
    }
}