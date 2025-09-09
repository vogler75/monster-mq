package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.AclRule
import at.rocworks.data.User
import at.rocworks.stores.AuthStoreType
import at.rocworks.stores.IUserManagementStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import org.mindrot.jbcrypt.BCrypt
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

class UserManagementStoreSqlite(
    private val path: String
): AbstractVerticle(), IUserManagementStore {
    private val logger = Utils.getLogger(this::class.java)
    
    private val usersTableName = "users"
    private val usersAclTableName = "usersacl"
    
    private var connection: Connection? = null

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): AuthStoreType = AuthStoreType.SQLITE

    override fun start(startPromise: Promise<Void>) {
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:$path")
            connection?.autoCommit = false
            createTablesSync()
            logger.info("SQLite user management store initialized [${Utils.getCurrentFunctionName()}]")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to initialize SQLite user management store: ${e.message} [${Utils.getCurrentFunctionName()}]")
            startPromise.fail(e)
        }
    }

    override suspend fun init(): Boolean {
        return try {
            val promise = Promise.promise<Void>()
            start(promise)
            promise.future().await()
            true
        } catch (e: Exception) {
            logger.severe("Failed to initialize user management store: ${e.message}")
            false
        }
    }

    override suspend fun createTables(): Boolean {
        return createTablesSync()
    }

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

    override suspend fun close() {
        try {
            connection?.close()
        } catch (e: SQLException) {
            logger.warning("Error closing SQLite connection: ${e.message}")
        }
    }

    override suspend fun createUser(user: User): Boolean {
        val sql = "INSERT INTO $usersTableName (username, password_hash, enabled, can_subscribe, can_publish, is_admin) VALUES (?, ?, ?, ?, ?, ?)"
        return try {
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
    }

    override suspend fun updateUser(user: User): Boolean {
        val sql = "UPDATE $usersTableName SET password_hash = ?, enabled = ?, can_subscribe = ?, can_publish = ?, is_admin = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?"
        return try {
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
    }

    override suspend fun deleteUser(username: String): Boolean {
        val sql = "DELETE FROM $usersTableName WHERE username = ?"
        return try {
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
    }

    override suspend fun getUser(username: String): User? {
        val sql = "SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM $usersTableName WHERE username = ?"
        return try {
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
    }

    override suspend fun getAllUsers(): List<User> {
        val sql = "SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM $usersTableName"
        return try {
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
    }

    override suspend fun validateCredentials(username: String, password: String): User? {
        val user = getUser(username)
        return if (user != null && user.enabled && BCrypt.checkpw(password, user.passwordHash)) {
            user
        } else null
    }

    override suspend fun createAclRule(rule: AclRule): Boolean {
        val sql = "INSERT INTO $usersAclTableName (username, topic_pattern, can_subscribe, can_publish, priority) VALUES (?, ?, ?, ?, ?)"
        return try {
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
    }

    override suspend fun updateAclRule(rule: AclRule): Boolean {
        val sql = "UPDATE $usersAclTableName SET username = ?, topic_pattern = ?, can_subscribe = ?, can_publish = ?, priority = ? WHERE id = ?"
        return try {
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
    }

    override suspend fun deleteAclRule(id: String): Boolean {
        val sql = "DELETE FROM $usersAclTableName WHERE id = ?"
        return try {
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
    }

    override suspend fun getUserAclRules(username: String): List<AclRule> {
        val sql = "SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM $usersAclTableName WHERE username = ? ORDER BY priority DESC"
        return try {
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
    }

    override suspend fun getAllAclRules(): List<AclRule> {
        val sql = "SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM $usersAclTableName ORDER BY priority DESC"
        return try {
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
    }

    override suspend fun loadAllUsersAndAcls(): Pair<List<User>, List<AclRule>> {
        return Pair(getAllUsers(), getAllAclRules())
    }
}