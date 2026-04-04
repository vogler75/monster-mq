package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.AclRule
import at.rocworks.data.User
import at.rocworks.stores.StoreType
import at.rocworks.stores.IUserStore
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.mindrot.jbcrypt.BCrypt
import java.time.LocalDateTime

class UserStoreSqlite(
    private val path: String,
    private val vertx: Vertx
): IUserStore {
    private val logger = Utils.getLogger(this::class.java)

    private val usersTableName = "users"
    private val usersAclTableName = "usersacl"

    private lateinit var sqliteClient: SQLiteClient


    override fun getType(): StoreType = StoreType.SQLITE

    override fun init(): Future<Boolean> {
        sqliteClient = SQLiteClient(vertx, path)

        val initSql = JsonArray()
            .add("""
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
            """.trimIndent())
            .add("""
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
            .add("CREATE INDEX IF NOT EXISTS ${usersAclTableName}_username_idx ON $usersAclTableName (username)")
            .add("CREATE INDEX IF NOT EXISTS ${usersAclTableName}_priority_idx ON $usersAclTableName (priority)")

        return sqliteClient.initDatabase(initSql).map {
            logger.info("SQLite user management store initialized [${Utils.getCurrentFunctionName()}]")
            true
        }.otherwise { e ->
            logger.severe("Failed to initialize SQLite user management store: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override fun createTables(): Future<Boolean> {
        return init()
    }

    override fun close(): Future<Void> {
        // Connection is owned by SQLiteVerticle — nothing to close here
        return Future.succeededFuture()
    }

    private fun toBool(value: Any?): Boolean {
        return when (value) {
            is Boolean -> value
            is Number -> value.toInt() != 0
            else -> false
        }
    }

    private fun rowToUser(row: JsonObject): User {
        return User(
            username = row.getString("username"),
            passwordHash = row.getString("password_hash"),
            enabled = toBool(row.getValue("enabled")),
            canSubscribe = toBool(row.getValue("can_subscribe")),
            canPublish = toBool(row.getValue("can_publish")),
            isAdmin = toBool(row.getValue("is_admin")),
            createdAt = row.getString("created_at")?.let { LocalDateTime.parse(it.replace(" ", "T")) },
            updatedAt = row.getString("updated_at")?.let { LocalDateTime.parse(it.replace(" ", "T")) }
        )
    }

    private fun rowToAclRule(row: JsonObject): AclRule {
        return AclRule(
            id = row.getValue("id").toString(),
            username = row.getString("username"),
            topicPattern = row.getString("topic_pattern"),
            canSubscribe = toBool(row.getValue("can_subscribe")),
            canPublish = toBool(row.getValue("can_publish")),
            priority = row.getInteger("priority", 0),
            createdAt = row.getString("created_at")?.let { LocalDateTime.parse(it.replace(" ", "T")) }
        )
    }

    override fun createUser(user: User): Future<Boolean> {
        val sql = "INSERT INTO $usersTableName (username, password_hash, enabled, can_subscribe, can_publish, is_admin) VALUES (?, ?, ?, ?, ?, ?)"
        val params = JsonArray()
            .add(user.username)
            .add(user.passwordHash)
            .add(if (user.enabled) 1 else 0)
            .add(if (user.canSubscribe) 1 else 0)
            .add(if (user.canPublish) 1 else 0)
            .add(if (user.isAdmin) 1 else 0)

        return sqliteClient.executeUpdate(sql, params).map { rowsAffected ->
            rowsAffected > 0
        }.otherwise { e ->
            logger.warning("Error creating user [${user.username}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override fun updateUser(user: User): Future<Boolean> {
        val sql = "UPDATE $usersTableName SET password_hash = ?, enabled = ?, can_subscribe = ?, can_publish = ?, is_admin = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?"
        val params = JsonArray()
            .add(user.passwordHash)
            .add(if (user.enabled) 1 else 0)
            .add(if (user.canSubscribe) 1 else 0)
            .add(if (user.canPublish) 1 else 0)
            .add(if (user.isAdmin) 1 else 0)
            .add(user.username)

        return sqliteClient.executeUpdate(sql, params).map { rowsAffected ->
            rowsAffected > 0
        }.otherwise { e ->
            logger.warning("Error updating user [${user.username}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override fun deleteUser(username: String): Future<Boolean> {
        val sql = "DELETE FROM $usersTableName WHERE username = ?"
        val params = JsonArray().add(username)

        return sqliteClient.executeUpdate(sql, params).map { rowsAffected ->
            rowsAffected > 0
        }.otherwise { e ->
            logger.warning("Error deleting user [$username]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override fun getUser(username: String): Future<User?> {
        val sql = "SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM $usersTableName WHERE username = ?"
        val params = JsonArray().add(username)

        return sqliteClient.executeQuery(sql, params).map { results ->
            if (results.size() > 0) {
                rowToUser(results.getJsonObject(0))
            } else {
                null
            }
        }.otherwise { e ->
            logger.warning("Error getting user [$username]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            null
        }
    }

    override fun getAllUsers(): Future<List<User>> {
        val sql = "SELECT username, password_hash, enabled, can_subscribe, can_publish, is_admin, created_at, updated_at FROM $usersTableName ORDER BY username"

        return sqliteClient.executeQuery(sql).map { results ->
            (0 until results.size()).map { i -> rowToUser(results.getJsonObject(i)) }
        }.otherwise { e ->
            logger.warning("Error getting all users: ${e.message} [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    override fun validateCredentials(username: String, password: String): Future<User?> {
        val result: Future<User?> = getUser(username).compose<User?> { user: User? ->
            if (user != null && user.enabled && BCrypt.checkpw(password, user.passwordHash)) {
                Future.succeededFuture(user)
            } else {
                Future.succeededFuture(null)
            }
        }
        return result.recover { e ->
            logger.severe("Error in validateCredentials: ${e.message}")
            Future.succeededFuture(null)
        }
    }

    override fun createAclRule(rule: AclRule): Future<Boolean> {
        val sql = "INSERT INTO $usersAclTableName (username, topic_pattern, can_subscribe, can_publish, priority) VALUES (?, ?, ?, ?, ?)"
        val params = JsonArray()
            .add(rule.username)
            .add(rule.topicPattern)
            .add(if (rule.canSubscribe) 1 else 0)
            .add(if (rule.canPublish) 1 else 0)
            .add(rule.priority)

        return sqliteClient.executeUpdate(sql, params).map { rowsAffected ->
            rowsAffected > 0
        }.otherwise { e ->
            logger.warning("Error creating ACL rule for user [${rule.username}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override fun updateAclRule(rule: AclRule): Future<Boolean> {
        val sql = "UPDATE $usersAclTableName SET username = ?, topic_pattern = ?, can_subscribe = ?, can_publish = ?, priority = ? WHERE id = ?"
        val params = JsonArray()
            .add(rule.username)
            .add(rule.topicPattern)
            .add(if (rule.canSubscribe) 1 else 0)
            .add(if (rule.canPublish) 1 else 0)
            .add(rule.priority)
            .add(rule.id)

        return sqliteClient.executeUpdate(sql, params).map { rowsAffected ->
            rowsAffected > 0
        }.otherwise { e ->
            logger.warning("Error updating ACL rule [${rule.id}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override fun deleteAclRule(id: String): Future<Boolean> {
        val sql = "DELETE FROM $usersAclTableName WHERE id = ?"
        val params = JsonArray().add(id)

        return sqliteClient.executeUpdate(sql, params).map { rowsAffected ->
            rowsAffected > 0
        }.otherwise { e ->
            logger.warning("Error deleting ACL rule [$id]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override fun getAclRule(id: String): Future<AclRule?> {
        val sql = "SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM $usersAclTableName WHERE id = ?"
        val params = JsonArray().add(id)

        return sqliteClient.executeQuery(sql, params).map { results ->
            if (results.size() > 0) {
                rowToAclRule(results.getJsonObject(0))
            } else {
                null
            }
        }.otherwise { e ->
            logger.warning("Error getting ACL rule [$id]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            null
        }
    }

    override fun getUserAclRules(username: String): Future<List<AclRule>> {
        val sql = "SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM $usersAclTableName WHERE username = ? ORDER BY priority DESC"
        val params = JsonArray().add(username)

        return sqliteClient.executeQuery(sql, params).map { results ->
            (0 until results.size()).map { i -> rowToAclRule(results.getJsonObject(i)) }
        }.otherwise { e ->
            logger.warning("Error getting ACL rules for user [$username]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    override fun getAllAclRules(): Future<List<AclRule>> {
        val sql = "SELECT id, username, topic_pattern, can_subscribe, can_publish, priority, created_at FROM $usersAclTableName ORDER BY priority DESC"

        return sqliteClient.executeQuery(sql).map { results ->
            (0 until results.size()).map { i -> rowToAclRule(results.getJsonObject(i)) }
        }.otherwise { e ->
            logger.warning("Error getting all ACL rules: ${e.message} [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    override fun loadAllUsersAndAcls(): Future<Pair<List<User>, List<AclRule>>> {
        return getAllUsers().compose { users ->
            getAllAclRules().map { acls ->
                Pair(users, acls)
            }
        }.otherwise { e ->
            logger.severe("Error in loadAllUsersAndAcls: ${e.message}")
            Pair(emptyList(), emptyList())
        }
    }
}
