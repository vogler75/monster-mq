package at.rocworks.stores.cratedb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.AclRule
import at.rocworks.data.User
import at.rocworks.stores.AuthStoreType
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IUserManagementStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import org.mindrot.jbcrypt.BCrypt
import java.sql.Connection
import java.sql.SQLException
import java.time.LocalDateTime
import java.util.*

class UserManagementCrateDb(
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IUserManagementStore {
    private val logger = Utils.getLogger(this::class.java)

    private val usersTableName = "users"
    private val usersAclTableName = "usersacl"

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): AuthStoreType = AuthStoreType.CRATEDB

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.autoCommit = false

                val createTableSQL = listOf("""
                CREATE TABLE IF NOT EXISTS $usersTableName (
                    username TEXT PRIMARY KEY,
                    password_hash TEXT,
                    enabled BOOLEAN,
                    can_subscribe BOOLEAN,
                    can_publish BOOLEAN,
                    is_admin BOOLEAN,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                ) CLUSTERED INTO 2 SHARDS
                """.trimIndent(), """
                CREATE TABLE IF NOT EXISTS $usersAclTableName (
                    id TEXT PRIMARY KEY,
                    username TEXT,
                    topic_pattern TEXT,
                    can_subscribe BOOLEAN,
                    can_publish BOOLEAN,
                    priority INTEGER,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                ) CLUSTERED INTO 2 SHARDS
                """.trimIndent())

                connection.createStatement().use { statement ->
                    createTableSQL.forEach(statement::executeUpdate)
                }
                connection.commit()
                logger.info("CrateDB user management tables are ready [${Utils.getCurrentFunctionName()}]")
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Error creating CrateDB user management tables: ${e.message} [${Utils.getCurrentFunctionName()}]")
                promise.fail(e)
            }
            return promise.future()
        }
    }

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
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
        return init()
    }

    override suspend fun close() {
        // Database connection is managed by the parent class
    }

    override suspend fun createUser(user: User): Boolean {
        val sql = "INSERT INTO $usersTableName (username, password_hash, enabled, can_subscribe, can_publish, is_admin) VALUES (?, ?, ?, ?, ?, ?)"
        return try {
            db.connection?.let { connection ->
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
            logger.warning("Error creating user [${user.username}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override suspend fun updateUser(user: User): Boolean {
        val sql = "UPDATE $usersTableName SET password_hash = ?, enabled = ?, can_subscribe = ?, can_publish = ?, is_admin = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?"
        return try {
            db.connection?.let { connection ->
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
            logger.warning("Error updating user [${user.username}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override suspend fun deleteUser(username: String): Boolean {
        val sql = "DELETE FROM $usersTableName WHERE username = ?"
        return try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { stmt ->
                    stmt.setString(1, username)
                    val result = stmt.executeUpdate() > 0
                    connection.commit()
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
            db.connection?.let { connection ->
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
            logger.warning("Error getting user [$username]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            null
        }
    }

    override suspend fun getAllUsers(): List<User> {
        val sql = "SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM $usersTableName"
        return try {
            db.connection?.let { connection ->
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
        val sql = "INSERT INTO $usersAclTableName (id, username, topic_pattern, can_subscribe, can_publish, priority) VALUES (?, ?, ?, ?, ?, ?)"
        return try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { stmt ->
                    // Generate UUID for CrateDB
                    val id = rule.id.ifEmpty { UUID.randomUUID().toString() }
                    stmt.setString(1, id)
                    stmt.setString(2, rule.username)
                    stmt.setString(3, rule.topicPattern)
                    stmt.setBoolean(4, rule.canSubscribe)
                    stmt.setBoolean(5, rule.canPublish)
                    stmt.setInt(6, rule.priority)
                    val result = stmt.executeUpdate() > 0
                    connection.commit()
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
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { stmt ->
                    stmt.setString(1, rule.username)
                    stmt.setString(2, rule.topicPattern)
                    stmt.setBoolean(3, rule.canSubscribe)
                    stmt.setBoolean(4, rule.canPublish)
                    stmt.setInt(5, rule.priority)
                    stmt.setString(6, rule.id)
                    val result = stmt.executeUpdate() > 0
                    connection.commit()
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
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { stmt ->
                    stmt.setString(1, id)
                    val result = stmt.executeUpdate() > 0
                    connection.commit()
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
            db.connection?.let { connection ->
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
            logger.warning("Error getting ACL rules for user [$username]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    override suspend fun getAllAclRules(): List<AclRule> {
        val sql = "SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM $usersAclTableName ORDER BY priority DESC"
        return try {
            db.connection?.let { connection ->
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
            logger.warning("Error getting all ACL rules: ${e.message} [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    override suspend fun loadAllUsersAndAcls(): Pair<List<User>, List<AclRule>> {
        return Pair(getAllUsers(), getAllAclRules())
    }
}