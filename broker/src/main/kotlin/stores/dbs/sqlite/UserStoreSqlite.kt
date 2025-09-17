package at.rocworks.stores.sqlite

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
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.concurrent.Callable

class UserStoreSqlite(
    private val path: String,
    private val vertx: Vertx
): IUserStore {
    private val logger = Utils.getLogger(this::class.java)

    private val usersTableName = "users"
    private val usersAclTableName = "usersacl"

    private var connection: Connection? = null

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): StoreType = StoreType.SQLITE

    private fun createTablesSync(): Boolean {
        return try {
            connection?.let { conn ->
                val createTableSQL = listOf("""
                CREATE TABLE IF NOT EXISTS $usersTableName (
                    username TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL,
                    enabled BOOLEAN DEFAULT 1,
                    can_subscribe BOOLEAN DEFAULT 1,
                    can_publish BOOLEAN DEFAULT 1,
                    is_admin BOOLEAN DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """.trimIndent(), """
                CREATE TABLE IF NOT EXISTS $usersAclTableName (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT REFERENCES $usersTableName(username) ON DELETE CASCADE,
                    topic_pattern TEXT NOT NULL,
                    can_subscribe BOOLEAN DEFAULT 0,
                    can_publish BOOLEAN DEFAULT 0,
                    priority INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """.trimIndent())

                val createIndexesSQL = listOf(
                    "CREATE INDEX IF NOT EXISTS ${usersAclTableName}_username_idx ON $usersAclTableName (username)",
                    "CREATE INDEX IF NOT EXISTS ${usersAclTableName}_priority_idx ON $usersAclTableName (priority)"
                )

                conn.createStatement().use { stmt ->
                    createTableSQL.forEach(stmt::executeUpdate)
                    createIndexesSQL.forEach(stmt::executeUpdate)
                }
                conn.commit()
                logger.info("User management tables created [${Utils.getCurrentFunctionName()}]")
                true
            } ?: false
        } catch (e: SQLException) {
            logger.severe("Error creating user management tables: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override fun init(): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            try {
                connection = DriverManager.getConnection("jdbc:sqlite:$path")
                connection?.autoCommit = false
                createTablesSync()
                logger.info("SQLite user management store initialized [${Utils.getCurrentFunctionName()}]")
                true
            } catch (e: Exception) {
                logger.severe("Failed to initialize SQLite user management store: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
            } catch (e: SQLException) {
                logger.warning("Error closing SQLite connection: ${e.message}")
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
                connection?.let { conn ->
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, user.username)
                        stmt.setString(2, user.passwordHash)
                        stmt.setBoolean(3, user.enabled)
                        stmt.setBoolean(4, user.canSubscribe)
                        stmt.setBoolean(5, user.canPublish)
                        stmt.setBoolean(6, user.isAdmin)
                        val result = stmt.executeUpdate() > 0
                        conn.commit()
                        result
                    }
                } ?: false
            } catch (e: SQLException) {
                logger.warning("Error creating user [${user.username}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
                connection?.let { conn ->
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, user.passwordHash)
                        stmt.setBoolean(2, user.enabled)
                        stmt.setBoolean(3, user.canSubscribe)
                        stmt.setBoolean(4, user.canPublish)
                        stmt.setBoolean(5, user.isAdmin)
                        stmt.setString(6, user.username)
                        val result = stmt.executeUpdate() > 0
                        conn.commit()
                        result
                    }
                } ?: false
            } catch (e: SQLException) {
                logger.warning("Error updating user [${user.username}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
                connection?.let { conn ->
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, username)
                        val result = stmt.executeUpdate() > 0
                        conn.commit()
                        result
                    }
                } ?: false
            } catch (e: SQLException) {
                logger.warning("Error deleting user [$username]: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
                connection?.let { conn ->
                    conn.prepareStatement(sql).use { stmt ->
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
                                createdAt = rs.getString("created_at")?.let { java.time.LocalDateTime.parse(it.replace(" ", "T")) },
                                updatedAt = rs.getString("updated_at")?.let { java.time.LocalDateTime.parse(it.replace(" ", "T")) }
                            )
                        } else null
                    }
                }
            } catch (e: SQLException) {
                logger.warning("Error getting user [$username]: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
            val sql = "SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM $usersTableName"
            try {
                connection?.let { conn ->
                    conn.prepareStatement(sql).use { stmt ->
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
                                createdAt = rs.getString("created_at")?.let { java.time.LocalDateTime.parse(it.replace(" ", "T")) },
                                updatedAt = rs.getString("updated_at")?.let { java.time.LocalDateTime.parse(it.replace(" ", "T")) }
                            ))
                        }
                        users
                    }
                } ?: emptyList()
            } catch (e: SQLException) {
                logger.warning("Error getting all users: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
                connection?.let { conn ->
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, rule.username)
                        stmt.setString(2, rule.topicPattern)
                        stmt.setBoolean(3, rule.canSubscribe)
                        stmt.setBoolean(4, rule.canPublish)
                        stmt.setInt(5, rule.priority)
                        val result = stmt.executeUpdate() > 0
                        conn.commit()
                        result
                    }
                } ?: false
            } catch (e: SQLException) {
                logger.warning("Error creating ACL rule for user [${rule.username}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
                connection?.let { conn ->
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, rule.username)
                        stmt.setString(2, rule.topicPattern)
                        stmt.setBoolean(3, rule.canSubscribe)
                        stmt.setBoolean(4, rule.canPublish)
                        stmt.setInt(5, rule.priority)
                        stmt.setString(6, rule.id)
                        val result = stmt.executeUpdate() > 0
                        conn.commit()
                        result
                    }
                } ?: false
            } catch (e: SQLException) {
                logger.warning("Error updating ACL rule [${rule.id}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
                connection?.let { conn ->
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, id)
                        val result = stmt.executeUpdate() > 0
                        conn.commit()
                        result
                    }
                } ?: false
            } catch (e: SQLException) {
                logger.warning("Error deleting ACL rule [$id]: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
                connection?.let { conn ->
                    conn.prepareStatement(sql).use { stmt ->
                        stmt.setString(1, id)
                        val rs = stmt.executeQuery()
                        if (rs.next()) {
                            AclRule(
                                id = rs.getString("id"),
                                username = rs.getString("username"),
                                topicPattern = rs.getString("topic_pattern"),
                                canSubscribe = rs.getBoolean("can_subscribe"),
                                canPublish = rs.getBoolean("can_publish"),
                                priority = rs.getInt("priority"),
                                createdAt = rs.getString("created_at")?.let { java.time.LocalDateTime.parse(it.replace(" ", "T")) }
                            )
                        } else null
                    }
                }
            } catch (e: SQLException) {
                logger.warning("Error getting ACL rule [$id]: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
                connection?.let { conn ->
                    conn.prepareStatement(sql).use { stmt ->
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
                                createdAt = rs.getString("created_at")?.let { java.time.LocalDateTime.parse(it.replace(" ", "T")) }
                            ))
                        }
                        rules
                    }
                } ?: emptyList()
            } catch (e: SQLException) {
                logger.warning("Error getting ACL rules for user [$username]: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
                connection?.let { conn ->
                    conn.prepareStatement(sql).use { stmt ->
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
                                createdAt = rs.getString("created_at")?.let { java.time.LocalDateTime.parse(it.replace(" ", "T")) }
                            ))
                        }
                        rules
                    }
                } ?: emptyList()
            } catch (e: SQLException) {
                logger.warning("Error getting all ACL rules: ${e.message} [${Utils.getCurrentFunctionName()}]")
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