package at.rocworks.stores.postgres

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.AclRule
import at.rocworks.data.User
import at.rocworks.stores.StoreType
import at.rocworks.stores.IUserStore
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import org.mindrot.jbcrypt.BCrypt
import java.sql.SQLException
import java.util.concurrent.Callable

class UserStorePostgres(
    private val url: String,
    private val username: String,
    private val password: String,
    private val vertx: Vertx,
    private val schema: String? = null
): IUserStore {
    private val logger = Utils.getLogger(this::class.java)

    private val usersTableName = "users"
    private val usersAclTableName = "usersacl"

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): StoreType = StoreType.POSTGRES

    private var connection: java.sql.Connection? = null

    private fun rollbackQuietly() {
        try { connection?.rollback() } catch (_: SQLException) { }
    }

    private fun logSqlException(operation: String, e: SQLException) {
        logger.warning("$operation failed: ${e.message} sqlState=${e.sqlState} errorCode=${e.errorCode} [${Utils.getCurrentFunctionName()}]")
    }

    private fun createTablesSync(connection: java.sql.Connection): Boolean {
        return try {
            // Set PostgreSQL schema if specified
            if (!schema.isNullOrBlank()) {
                connection.createStatement().use { stmt ->
                    stmt.execute("SET search_path TO \"$schema\", public")
                }
            }

            val createTableSQL = listOf("""
                CREATE TABLE IF NOT EXISTS $usersTableName (
                    username VARCHAR(255) PRIMARY KEY,
                    password_hash VARCHAR(255) NOT NULL,
                    enabled BOOLEAN DEFAULT true,
                    can_subscribe BOOLEAN DEFAULT true,
                    can_publish BOOLEAN DEFAULT true,
                    is_admin BOOLEAN DEFAULT false,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """.trimIndent(), """
                CREATE TABLE IF NOT EXISTS $usersAclTableName (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255) REFERENCES $usersTableName(username) ON DELETE CASCADE,
                    topic_pattern VARCHAR(1024) NOT NULL,
                    can_subscribe BOOLEAN DEFAULT false,
                    can_publish BOOLEAN DEFAULT false,
                    priority INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """.trimIndent())

            val createIndexesSQL = listOf(
                "CREATE INDEX IF NOT EXISTS ${usersAclTableName}_username_idx ON $usersAclTableName (username)",
                "CREATE INDEX IF NOT EXISTS ${usersAclTableName}_priority_idx ON $usersAclTableName (priority)"
            )

            connection.createStatement().use { stmt ->
                createTableSQL.forEach(stmt::executeUpdate)
                createIndexesSQL.forEach(stmt::executeUpdate)
            }
            connection.commit()
            logger.info("PostgreSQL user management tables created")
            true
        } catch (e: java.sql.SQLException) {
            rollbackQuietly()
            logger.severe("Error creating PostgreSQL user management tables: ${e.message}")
            false
        }
    }

    override fun init(): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            try {
                connection = java.sql.DriverManager.getConnection(url, username, password)
                connection?.autoCommit = false
                createTablesSync(connection!!)
                logger.info("PostgreSQL user management store initialized")
                true
            } catch (e: Exception) {
                logger.severe("Failed to initialize PostgreSQL user management store: ${e.message}")
                throw e
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Failed to initialize user management store: ${result.cause()?.message}")
                promise.complete(false)
            }
        }

        return promise.future()
    }

    override fun createTables(): Future<Boolean> {
        return init()
    }

    override fun close(): Future<Void> {
        val promise = Promise.promise<Void>()

        vertx.executeBlocking(Callable {
            try {
                connection?.close()
            } catch (e: java.sql.SQLException) {
                logger.warning("Error closing PostgreSQL connection: ${e.message}")
            }
            null
        }).onComplete {
            promise.complete()
        }

        return promise.future()
    }

    override fun createUser(user: User): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            val sql = "INSERT INTO $usersTableName (username, password_hash, enabled, can_subscribe, can_publish, is_admin) VALUES (?, ?, ?, ?, ?, ?)"
            try {
                connection?.let { connection ->
                    connection.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, user.username)
                        stmt.setString(2, user.passwordHash)
                        stmt.setBoolean(3, user.enabled)
                        stmt.setBoolean(4, user.canSubscribe)
                        stmt.setBoolean(5, user.canPublish)
                        stmt.setBoolean(6, user.isAdmin)
                        val result = stmt.executeUpdate() > 0
                        connection.commit()
                        result
                    }
                } ?: false
            } catch (e: SQLException) {
                rollbackQuietly()
                logSqlException("Error creating user [${user.username}]", e)
                false
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in createUser: ${result.cause()?.message}")
                promise.complete(false)
            }
        }

        return promise.future()
    }

    override fun updateUser(user: User): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            val sql = "UPDATE $usersTableName SET password_hash = ?, enabled = ?, can_subscribe = ?, can_publish = ?, is_admin = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?"
            try {
                connection?.let { connection ->
                    connection.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, user.passwordHash)
                        stmt.setBoolean(2, user.enabled)
                        stmt.setBoolean(3, user.canSubscribe)
                        stmt.setBoolean(4, user.canPublish)
                        stmt.setBoolean(5, user.isAdmin)
                        stmt.setString(6, user.username)
                        val result = stmt.executeUpdate() > 0
                        connection.commit()
                        result
                    }
                } ?: false
            } catch (e: SQLException) {
                rollbackQuietly()
                logSqlException("Error updating user [${user.username}]", e)
                false
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in updateUser: ${result.cause()?.message}")
                promise.complete(false)
            }
        }

        return promise.future()
    }

    override fun deleteUser(username: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            val sql = "DELETE FROM $usersTableName WHERE username = ?"
            try {
                connection?.let { connection ->
                    connection.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, username)
                        val result = stmt.executeUpdate() > 0
                        connection.commit()
                        result
                    }
                } ?: false
            } catch (e: SQLException) {
                rollbackQuietly()
                logSqlException("Error deleting user [$username]", e)
                false
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in deleteUser: ${result.cause()?.message}")
                promise.complete(false)
            }
        }

        return promise.future()
    }

    override fun getUser(username: String): Future<User?> {
        val promise = Promise.promise<User?>()

        vertx.executeBlocking(Callable {
            val sql = "SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM $usersTableName WHERE username = ?"
            try {
                connection?.let { connection ->
                    connection.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, username)
                        val rs = stmt.executeQuery()
                        if (rs.next()) {
                            User(
                                username = rs.getString("username"),
                                passwordHash = rs.getString("password_hash"),
                                enabled = rs.getBoolean("enabled"),
                                canSubscribe = rs.getBoolean("can_subscribe"),
                                canPublish = rs.getBoolean("can_publish"),
                                isAdmin = rs.getBoolean("is_admin"),
                                createdAt = rs.getTimestamp("created_at")?.toLocalDateTime(),
                                updatedAt = rs.getTimestamp("updated_at")?.toLocalDateTime()
                            )
                        } else null
                    }
                }
            } catch (e: SQLException) {
                rollbackQuietly()
                logSqlException("Error getting user [$username]", e)
                null
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in getUser: ${result.cause()?.message}")
                promise.complete(null)
            }
        }

        return promise.future()
    }

    override fun getAllUsers(): Future<List<User>> {
        val promise = Promise.promise<List<User>>()

        vertx.executeBlocking(Callable {
            val sql = "SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM $usersTableName ORDER BY username"
            try {
                connection?.let { connection ->
                    connection.prepareStatement(sql).use { stmt ->
                        val rs = stmt.executeQuery()
                        val users = mutableListOf<User>()
                        while (rs.next()) {
                            users.add(User(
                                username = rs.getString("username"),
                                passwordHash = rs.getString("password_hash"),
                                enabled = rs.getBoolean("enabled"),
                                canSubscribe = rs.getBoolean("can_subscribe"),
                                canPublish = rs.getBoolean("can_publish"),
                                isAdmin = rs.getBoolean("is_admin"),
                                createdAt = rs.getTimestamp("created_at")?.toLocalDateTime(),
                                updatedAt = rs.getTimestamp("updated_at")?.toLocalDateTime()
                            ))
                        }
                        users
                    }
                } ?: emptyList()
            } catch (e: SQLException) {
                rollbackQuietly()
                logSqlException("Error getting all users", e)
                emptyList()
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in getAllUsers: ${result.cause()?.message}")
                promise.complete(emptyList())
            }
        }

        return promise.future()
    }

    override fun validateCredentials(username: String, password: String): Future<User?> {
        val promise = Promise.promise<User?>()

        getUser(username).onComplete { userResult ->
            if (userResult.succeeded()) {
                val user = userResult.result()
                if (user != null && user.enabled && BCrypt.checkpw(password, user.passwordHash)) {
                    promise.complete(user)
                } else {
                    promise.complete(null)
                }
            } else {
                logger.severe("Error in validateCredentials: ${userResult.cause()?.message}")
                promise.complete(null)
            }
        }

        return promise.future()
    }

    override fun createAclRule(rule: AclRule): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            val sql = "INSERT INTO $usersAclTableName (username, topic_pattern, can_subscribe, can_publish, priority) VALUES (?, ?, ?, ?, ?)"
            try {
                connection?.let { connection ->
                    connection.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, rule.username)
                        stmt.setString(2, rule.topicPattern)
                        stmt.setBoolean(3, rule.canSubscribe)
                        stmt.setBoolean(4, rule.canPublish)
                        stmt.setInt(5, rule.priority)
                        val result = stmt.executeUpdate() > 0
                        connection.commit()
                        result
                    }
                } ?: false
            } catch (e: SQLException) {
                rollbackQuietly()
                logSqlException("Error creating ACL rule for user [${rule.username}]", e)
                false
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in createAclRule: ${result.cause()?.message}")
                promise.complete(false)
            }
        }

        return promise.future()
    }

    override fun updateAclRule(rule: AclRule): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            val sql = "UPDATE $usersAclTableName SET username = ?, topic_pattern = ?, can_subscribe = ?, can_publish = ?, priority = ? WHERE id = ?"
            try {
                val idInt = rule.id.toIntOrNull()
                if (idInt == null) {
                    logger.warning("updateAclRule invalid id [${rule.id}]")
                    return@Callable false
                }
                connection?.let { connection ->
                    connection.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, rule.username)
                        stmt.setString(2, rule.topicPattern)
                        stmt.setBoolean(3, rule.canSubscribe)
                        stmt.setBoolean(4, rule.canPublish)
                        stmt.setInt(5, rule.priority)
                        stmt.setInt(6, idInt)
                        val result = stmt.executeUpdate() > 0
                        connection.commit()
                        result
                    }
                } ?: false
            } catch (e: SQLException) {
                rollbackQuietly()
                logSqlException("Error updating ACL rule [${rule.id}]", e)
                false
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in updateAclRule: ${result.cause()?.message}")
                promise.complete(false)
            }
        }

        return promise.future()
    }

    override fun deleteAclRule(id: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            val sql = "DELETE FROM $usersAclTableName WHERE id = ?"
            try {
                val idInt = id.toIntOrNull()
                if (idInt == null) {
                    logger.warning("deleteAclRule invalid id [$id]")
                    return@Callable false
                }
                connection?.let { connection ->
                    connection.prepareStatement(sql).use { stmt ->
                        stmt.setInt(1, idInt)
                        val result = stmt.executeUpdate() > 0
                        connection.commit()
                        result
                    }
                } ?: false
            } catch (e: SQLException) {
                rollbackQuietly()
                logSqlException("Error deleting ACL rule [$id]", e)
                false
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in deleteAclRule: ${result.cause()?.message}")
                promise.complete(false)
            }
        }

        return promise.future()
    }

    override fun getAclRule(id: String): Future<AclRule?> {
        val promise = Promise.promise<AclRule?>()

        vertx.executeBlocking(Callable {
            val sql = "SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM $usersAclTableName WHERE id = ?"
            try {
                val idInt = id.toIntOrNull()
                if (idInt == null) {
                    logger.warning("getAclRule invalid id [$id]")
                    return@Callable null
                }
                connection?.let { connection ->
                    connection.prepareStatement(sql).use { stmt ->
                        stmt.setInt(1, idInt)
                        val rs = stmt.executeQuery()
                        if (rs.next()) {
                            AclRule(
                                id = rs.getString("id"),
                                username = rs.getString("username"),
                                topicPattern = rs.getString("topic_pattern"),
                                canSubscribe = rs.getBoolean("can_subscribe"),
                                canPublish = rs.getBoolean("can_publish"),
                                priority = rs.getInt("priority"),
                                createdAt = rs.getTimestamp("created_at")?.toLocalDateTime()
                            )
                        } else null
                    }
                }
            } catch (e: SQLException) {
                rollbackQuietly()
                logSqlException("Error getting ACL rule [$id]", e)
                null
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in getAclRule: ${result.cause()?.message}")
                promise.complete(null)
            }
        }

        return promise.future()
    }

    override fun getUserAclRules(username: String): Future<List<AclRule>> {
        val promise = Promise.promise<List<AclRule>>()

        vertx.executeBlocking(Callable {
            val sql = "SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM $usersAclTableName WHERE username = ? ORDER BY priority DESC"
            try {
                connection?.let { connection ->
                    connection.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, username)
                        val rs = stmt.executeQuery()
                        val rules = mutableListOf<AclRule>()
                        while (rs.next()) {
                            rules.add(AclRule(
                                id = rs.getString("id"),
                                username = rs.getString("username"),
                                topicPattern = rs.getString("topic_pattern"),
                                canSubscribe = rs.getBoolean("can_subscribe"),
                                canPublish = rs.getBoolean("can_publish"),
                                priority = rs.getInt("priority"),
                                createdAt = rs.getTimestamp("created_at")?.toLocalDateTime()
                            ))
                        }
                        rules
                    }
                } ?: emptyList()
            } catch (e: SQLException) {
                rollbackQuietly()
                logSqlException("Error getting ACL rules for user [$username]", e)
                emptyList()
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in getUserAclRules: ${result.cause()?.message}")
                promise.complete(emptyList())
            }
        }

        return promise.future()
    }

    override fun getAllAclRules(): Future<List<AclRule>> {
        val promise = Promise.promise<List<AclRule>>()

        vertx.executeBlocking(Callable {
            val sql = "SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM $usersAclTableName ORDER BY priority DESC"
            try {
                connection?.let { connection ->
                    connection.prepareStatement(sql).use { stmt ->
                        val rs = stmt.executeQuery()
                        val rules = mutableListOf<AclRule>()
                        while (rs.next()) {
                            rules.add(AclRule(
                                id = rs.getString("id"),
                                username = rs.getString("username"),
                                topicPattern = rs.getString("topic_pattern"),
                                canSubscribe = rs.getBoolean("can_subscribe"),
                                canPublish = rs.getBoolean("can_publish"),
                                priority = rs.getInt("priority"),
                                createdAt = rs.getTimestamp("created_at")?.toLocalDateTime()
                            ))
                        }
                        rules
                    }
                } ?: emptyList()
            } catch (e: SQLException) {
                rollbackQuietly()
                logSqlException("Error getting all ACL rules", e)
                emptyList()
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in getAllAclRules: ${result.cause()?.message}")
                promise.complete(emptyList())
            }
        }

        return promise.future()
    }

    override fun loadAllUsersAndAcls(): Future<Pair<List<User>, List<AclRule>>> {
        val promise = Promise.promise<Pair<List<User>, List<AclRule>>>()

        getAllUsers().compose { users ->
            getAllAclRules().map { acls ->
                Pair(users, acls)
            }
        }.onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in loadAllUsersAndAcls: ${result.cause()?.message}")
                promise.complete(Pair(emptyList(), emptyList()))
            }
        }

        return promise.future()
    }
}