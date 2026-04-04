package at.rocworks

import at.rocworks.auth.UserManager
import at.rocworks.handlers.SessionHandler
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.net.NetServerOptions

class NatsServer(
    private val port: Int,
    private val sessionHandler: SessionHandler,
    private val userManager: UserManager
) : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)


    override fun start(startPromise: Promise<Void>) {
        val options = NetServerOptions()
            .setTcpNoDelay(true)

        val server = vertx.createNetServer(options)

        server.exceptionHandler {
            logger.severe("NATS Server error: ${it.message} [${Utils.getCurrentFunctionName()}]")
        }

        server.connectHandler { socket ->
            NatsClient.deploy(vertx, socket, sessionHandler, userManager)
        }

        server.listen(port)
            .onSuccess { s ->
                logger.info("NATS Server is listening on port [${s.actualPort()}]")
                startPromise.complete()
            }
            .onFailure { error ->
                logger.severe("Error starting NATS Server: ${error.message} [${Utils.getCurrentFunctionName()}]")
                startPromise.fail(error)
            }
    }
}
