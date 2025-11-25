package at.rocworks.logging

import at.rocworks.oa4j.base.JManager
import at.rocworks.oa4j.jni.ErrCode
import at.rocworks.oa4j.jni.ErrPrio
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord

/**
 * JManagerLogHandler - Redirects Java logging to JManager's log system
 *
 * When MonsterMQ is running as a WinCC OA manager via MonsterOA,
 * this handler redirects all logs to the JManager log system instead of
 * writing to separate log files.
 *
 * Log files are written to:
 * - Linux: WCCOAjava<num>.0.log
 * - Windows: WCCOAjava<num>.log
 */
class JManagerLogHandler : Handler() {

    companion object {
        private var instance: JManagerLogHandler? = null

        /**
         * Install the JManager log handler
         */
        fun install(): JManagerLogHandler {
            if (instance == null) {
                instance = JManagerLogHandler()

                // Add to root logger to capture all logging
                val rootLogger = java.util.logging.Logger.getLogger("")
                rootLogger.addHandler(instance!!)

                // Set level to capture all logs (individual loggers control their own levels)
                instance!!.level = Level.ALL
            }
            return instance!!
        }

        /**
         * Remove the JManager log handler
         */
        fun uninstall() {
            instance?.let { handler ->
                val rootLogger = java.util.logging.Logger.getLogger("")
                rootLogger.removeHandler(handler)
                handler.close()
                instance = null
            }
        }
    }

    override fun publish(record: LogRecord) {
        try {
            // Convert Java logging level to JManager priority
            val priority = when {
                record.level.intValue() >= Level.SEVERE.intValue() -> ErrPrio.PRIO_SEVERE
                record.level.intValue() >= Level.WARNING.intValue() -> ErrPrio.PRIO_WARNING
                else -> ErrPrio.PRIO_INFO
            }

            // Format the log message
            val loggerName = record.loggerName ?: "Unknown"
            val message = "${record.message}"

            // Log through JManager
            JManager.log(priority, ErrCode.NOERR, "[$loggerName] $message")
        } catch (e: Exception) {
            // Silently ignore errors to prevent logging loops
        }
    }

    override fun flush() {
        // No-op for JManager logging
    }

    override fun close() {
        flush()
    }
}
