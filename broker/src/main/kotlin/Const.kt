package at.rocworks

import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.shareddata.AsyncMap
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.util.logging.Level
import java.util.logging.LogManager

object Const {
    const val GLOBAL_DISTRIBUTOR_NAMESPACE = "D"
    const val GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE = "S"

    const val CLIENT_NAMESPACE = "C"

    const val TOPIC_KEY = "Topic"
    const val CLIENT_KEY = "Client"
    const val COMMAND_KEY = "Command"

    const val COMMAND_STATUS = "Status"

    var DEBUG_LEVEL: Level = Level.INFO
}