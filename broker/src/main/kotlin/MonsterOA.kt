package at.rocworks

import at.rocworks.oa4j.base.JManager
import at.rocworks.logging.JManagerLogHandler
import java.util.logging.Logger
import kotlin.system.exitProcess

/**
 * MonsterOA - WinCC OA integration entry point for MonsterMQ
 *
 * This class handles the initialization and shutdown of MonsterMQ when running
 * as a WinCC OA manager. It manages the JManager lifecycle and passes filtered
 * arguments to the Monster class.
 *
 * Usage:
 *   WCCOAJavaManager -proj TestProj -num 3 -class at/rocworks/MonsterOA -- -config monster-config.yaml
 *
 * Arguments before "--" are for the WinCC OA manager
 * Arguments after "--" are passed to MonsterMQ
 */

fun main(args: Array<String>) {
    try {
        MonsterOA(args)
    } catch (e: Exception) {
        System.err.println("MonsterOA: Failed to initialize: ${e.message}")
        e.printStackTrace()
        exitProcess(1)
    }
}

class MonsterOA(args: Array<String>) {
    private val logger: Logger = Utils.getLogger(this::class.java)
    private var manager: JManager? = null
    private var monster: Monster? = null

    init {
        try {
            instance = this

            // Step 1: Install JManager log handler and remove all default handlers
            // This redirects all logs to JManager's log system instead of console/files
            val rootLogger = Logger.getLogger("")

            // Remove all default handlers to prevent duplicate logging
            rootLogger.handlers.toList().forEach { handler ->
                rootLogger.removeHandler(handler)
            }

            // Install JManager log handler
            JManagerLogHandler.install()

            // Step 2: Initialize JManager with WinCC OA
            manager = JManager()
            manager!!.init(args).start()

            logger.fine("MonsterOA: Starting with ${args.size} arguments ["+args.joinToString("][")+"]")

            // Step 3: Initialize Monster with filtered arguments
            logger.fine("MonsterOA: Initializing Monster...")
            monster = Monster(args)
            logger.fine("MonsterOA: Monster initialized successfully")

        } catch (e: Exception) {
            logger.severe("MonsterOA: Initialization failed: ${e.message}")
            e.printStackTrace()
            throw e
        }
    }

    companion object {
        private var instance: MonsterOA? = null

        /**
         * Get the singleton instance (if initialized)
         */
        fun getInstance(): MonsterOA? = instance

        /**
         * Extract arguments that come after "--" in the command line.
         * These are the application-specific arguments for MonsterMQ.
         */
    }
}
