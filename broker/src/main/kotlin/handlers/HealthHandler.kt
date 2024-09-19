package at.rocworks.handlers

import at.rocworks.Monster
import at.rocworks.Utils
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import java.util.concurrent.Callable

class HealthHandler(private val sessionHandler: SessionHandler): AbstractVerticle() {
    val logger = Utils.getLogger(this::class.java)

    override fun start(startPromise: Promise<Void>) {
        if (Monster.isNotClustered()) {
            vertx.executeBlocking(Callable {
                sessionHandler.purgeSessions()
                sessionHandler.purgeQueuedMessages()
            }).onComplete {
                logger.info("Purged sessions and queued messages")
                startPromise.complete()

                vertx.setPeriodic(60_000*10) {
                    vertx.executeBlocking(Callable { sessionHandler.purgeQueuedMessages() })
                }
            }
        }
    }
}