package at.rocworks.stores

import at.rocworks.stores.postgres.ConfigStorePostgres
import io.vertx.core.json.JsonObject

object ConfigStoreFactory {
    fun createConfigStore(config: JsonObject, storeType: String?): IConfigStore? {
        return when (storeType?.uppercase()) {
            "POSTGRES" -> {
                val postgresConfig = config.getJsonObject("Postgres")
                if (postgresConfig != null) {
                    val url = postgresConfig.getString("Url")
                    val user = postgresConfig.getString("User")
                    val pass = postgresConfig.getString("Pass")
                    if (url != null && user != null && pass != null) {
                        ConfigStorePostgres(url, user, pass)
                    } else {
                        null
                    }
                } else {
                    null
                }
            }
            else -> null
        }
    }
}