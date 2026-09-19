package at.rocworks.devices.script

import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentLinkedDeque

/**
 * Thread-safe ring buffer storing recent log lines for GraphQL inspection.
 */
class ScriptCircularLogBuffer(private val capacity: Int = 100) {

    private val deque = ConcurrentLinkedDeque<String>()
    private val timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneOffset.UTC)

    fun add(message: String) {
        val timestamp = timeFormatter.format(Instant.now())
        val line = "[$timestamp] $message"
        deque.addLast(line)
        while (deque.size > capacity) {
            deque.pollFirst()
        }
    }

    fun getLogs(): List<String> = deque.toList()

    fun clear() {
        deque.clear()
    }
}
