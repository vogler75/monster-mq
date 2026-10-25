package at.rocworks

import java.util.logging.Logger

object Version {
    private val logger: Logger = Utils.getLogger(Version::class.java)
    private var cachedVersion: String? = null

    /**
     * Get the MonsterMQ version from the version.txt file in resources.
     * version.txt is committed and kept in sync with the top-level version.txt by make.py release/dev,
     * so a plain Maven build from a fresh clone has it. The build scripts additionally write
     * git-sha.txt (gitignored); when present, the commit is appended as build metadata.
     * @return Version string (e.g., "1.6.5+9945c5a", or "1.6.5" without git-sha.txt) or "unknown"
     */
    fun getVersion(): String {
        if (cachedVersion != null) {
            return cachedVersion!!
        }

        try {
            val version = readResource("version.txt")?.substringBefore('+')
            if (!version.isNullOrEmpty()) {
                val sha = readResource("git-sha.txt")
                val full = if (sha.isNullOrEmpty()) version else "$version+$sha"
                cachedVersion = full
                logger.info("MonsterMQ version: $full")
                return full
            }
        } catch (e: Exception) {
            logger.warning("Failed to read version from resources: ${e.message}")
        }

        // Fallback to unknown version
        val unknownVersion = "unknown"
        cachedVersion = unknownVersion
        logger.warning("Version information not available, using: $unknownVersion")
        return unknownVersion
    }

    private fun readResource(name: String): String? =
        Version::class.java.classLoader.getResourceAsStream(name)?.use { stream ->
            stream.bufferedReader().use { it.readText().trim() }
        }
}
