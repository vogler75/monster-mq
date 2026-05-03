package at.rocworks.stores.sqlite

import java.io.File

object SQLiteDatabasePath {
    const val SESSION_MEMORY = "file:monstermq-session?mode=memory&cache=shared"
    const val QUEUE_MEMORY = "file:monstermq-queue?mode=memory&cache=shared"

    fun isMemory(path: String): Boolean =
        path == ":memory:" || (path.startsWith("file:") && path.contains("mode=memory"))

    fun connectionPath(path: String): String =
        if (isMemory(path)) path else File(path).absolutePath

    fun jdbcUrl(path: String): String = "jdbc:sqlite:${connectionPath(path)}"
}
