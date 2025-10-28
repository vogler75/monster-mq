package at.rocworks.flowengine

/**
 * Singleton holder for the JdbcManager instance
 */
object JdbcManagerHolder {
    private val instance: JdbcManager = JdbcManager()

    fun getInstance(): JdbcManager {
        return instance
    }
}
