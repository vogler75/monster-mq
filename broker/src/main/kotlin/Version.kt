package at.rocworks

import java.util.logging.Logger

object Version {
    private val logger: Logger = Utils.getLogger(Version::class.java)
    private var cachedVersion: String? = null

    /**
     * Get the MonsterMQ version from the version.txt file in resources.
     * This file is copied by the release.sh script during the release process.
     * @return Version string (e.g., "1.6.5+9945c5a") or "unknown" if not available
     */
    fun getVersion(): String {
        if (cachedVersion != null) {
            return cachedVersion!!
        }

        try {
            val versionStream = Version::class.java.classLoader.getResourceAsStream("version.txt")
            if (versionStream != null) {
                versionStream.use { stream ->
                    val version = stream.bufferedReader().use { it.readText().trim() }
                    if (version.isNotEmpty()) {
                        cachedVersion = version
                        logger.info("MonsterMQ version: $version")
                        return version
                    }
                }
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
}