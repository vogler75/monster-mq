package at.rocworks.stores.mongodb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.AclRule
import at.rocworks.data.User
import at.rocworks.stores.AuthStoreType
import at.rocworks.stores.IUserManagement
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.ReplaceOptions
import io.vertx.core.Promise
import org.bson.Document
import org.mindrot.jbcrypt.BCrypt
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*

class UserManagementMongoDB(
    private val url: String,
    private val database: String
): IUserManagement {
    private val logger = Utils.getLogger(this::class.java)

    private val usersCollectionName = "users"
    private val usersAclCollectionName = "usersacl"

    private var mongoClient: MongoClient? = null
    private var mongoDatabase: MongoDatabase? = null
    private var usersCollection: MongoCollection<Document>? = null
    private var usersAclCollection: MongoCollection<Document>? = null

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): AuthStoreType = AuthStoreType.MONGODB

    fun start(startPromise: Promise<Void>) {
        try {
            mongoClient = MongoClients.create(url)
            mongoDatabase = mongoClient!!.getDatabase(database)
            usersCollection = mongoDatabase!!.getCollection(usersCollectionName)
            usersAclCollection = mongoDatabase!!.getCollection(usersAclCollectionName)

            // Create indexes
            usersAclCollection!!.createIndex(Indexes.ascending("username"))
            usersAclCollection!!.createIndex(Indexes.descending("priority"))

            logger.info("MongoDB user management store initialized [${Utils.getCurrentFunctionName()}]")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to initialize MongoDB user management store: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
        return init()
    }

    override suspend fun close() {
        try {
            mongoClient?.close()
        } catch (e: Exception) {
            logger.warning("Error closing MongoDB connection: ${e.message}")
        }
    }

    private fun userToDocument(user: User): Document {
        return Document().apply {
            put("_id", user.username)
            put("passwordHash", user.passwordHash)
            put("enabled", user.enabled)
            put("canSubscribe", user.canSubscribe)
            put("canPublish", user.canPublish)
            put("isAdmin", user.isAdmin)
            user.createdAt?.let { put("createdAt", Date.from(it.toInstant(ZoneOffset.UTC))) }
            user.updatedAt?.let { put("updatedAt", Date.from(it.toInstant(ZoneOffset.UTC))) }
        }
    }

    private fun documentToUser(doc: Document): User {
        return User(
            username = doc.getString("_id"),
            passwordHash = doc.getString("passwordHash"),
            enabled = doc.getBoolean("enabled", true),
            canSubscribe = doc.getBoolean("canSubscribe", true),
            canPublish = doc.getBoolean("canPublish", true),
            isAdmin = doc.getBoolean("isAdmin", false),
            createdAt = doc.getDate("createdAt")?.toInstant()?.let { LocalDateTime.ofInstant(it, ZoneOffset.UTC) },
            updatedAt = doc.getDate("updatedAt")?.toInstant()?.let { LocalDateTime.ofInstant(it, ZoneOffset.UTC) }
        )
    }

    private fun aclRuleToDocument(rule: AclRule): Document {
        return Document().apply {
            put("_id", rule.id.ifEmpty { UUID.randomUUID().toString() })
            put("username", rule.username)
            put("topicPattern", rule.topicPattern)
            put("canSubscribe", rule.canSubscribe)
            put("canPublish", rule.canPublish)
            put("priority", rule.priority)
            rule.createdAt?.let { put("createdAt", Date.from(it.toInstant(ZoneOffset.UTC))) }
        }
    }

    private fun documentToAclRule(doc: Document): AclRule {
        return AclRule(
            id = doc.getString("_id"),
            username = doc.getString("username"),
            topicPattern = doc.getString("topicPattern"),
            canSubscribe = doc.getBoolean("canSubscribe", false),
            canPublish = doc.getBoolean("canPublish", false),
            priority = doc.getInteger("priority", 0),
            createdAt = doc.getDate("createdAt")?.toInstant()?.let { LocalDateTime.ofInstant(it, ZoneOffset.UTC) }
        )
    }

    override suspend fun createUser(user: User): Boolean {
        return try {
            usersCollection?.insertOne(userToDocument(user))
            true
        } catch (e: Exception) {
            logger.warning("Error creating user [${user.username}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override suspend fun updateUser(user: User): Boolean {
        return try {
            val updatedUser = user.copy(updatedAt = LocalDateTime.now())
            val result = usersCollection?.replaceOne(
                Filters.eq("_id", user.username),
                userToDocument(updatedUser),
                ReplaceOptions().upsert(false)
            )
            result?.modifiedCount ?: 0 > 0
        } catch (e: Exception) {
            logger.warning("Error updating user [${user.username}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override suspend fun deleteUser(username: String): Boolean {
        return try {
            // Delete user's ACL rules first
            usersAclCollection?.deleteMany(Filters.eq("username", username))
            // Delete user
            val result = usersCollection?.deleteOne(Filters.eq("_id", username))
            result?.deletedCount ?: 0 > 0
        } catch (e: Exception) {
            logger.warning("Error deleting user [$username]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override suspend fun getUser(username: String): User? {
        return try {
            val doc = usersCollection?.find(Filters.eq("_id", username))?.first()
            doc?.let { documentToUser(it) }
        } catch (e: Exception) {
            logger.warning("Error getting user [$username]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            null
        }
    }

    override suspend fun getAllUsers(): List<User> {
        return try {
            usersCollection?.find()?.map { documentToUser(it) }?.toList() ?: emptyList()
        } catch (e: Exception) {
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
        return try {
            val ruleWithId = if (rule.id.isEmpty()) rule.copy(id = UUID.randomUUID().toString()) else rule
            val ruleWithTimestamp = ruleWithId.copy(createdAt = LocalDateTime.now())
            usersAclCollection?.insertOne(aclRuleToDocument(ruleWithTimestamp))
            true
        } catch (e: Exception) {
            logger.warning("Error creating ACL rule for user [${rule.username}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override suspend fun updateAclRule(rule: AclRule): Boolean {
        return try {
            val result = usersAclCollection?.replaceOne(
                Filters.eq("_id", rule.id),
                aclRuleToDocument(rule),
                ReplaceOptions().upsert(false)
            )
            result?.modifiedCount ?: 0 > 0
        } catch (e: Exception) {
            logger.warning("Error updating ACL rule [${rule.id}]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override suspend fun deleteAclRule(id: String): Boolean {
        return try {
            val result = usersAclCollection?.deleteOne(Filters.eq("_id", id))
            result?.deletedCount ?: 0 > 0
        } catch (e: Exception) {
            logger.warning("Error deleting ACL rule [$id]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            false
        }
    }

    override suspend fun getAclRule(id: String): AclRule? {
        return try {
            usersAclCollection?.find(Filters.eq("_id", id))
                ?.first()
                ?.let { documentToAclRule(it) }
        } catch (e: Exception) {
            logger.warning("Error getting ACL rule [$id]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            null
        }
    }

    override suspend fun getUserAclRules(username: String): List<AclRule> {
        return try {
            usersAclCollection?.find(Filters.eq("username", username))
                ?.sort(Document("priority", -1))
                ?.map { documentToAclRule(it) }
                ?.toList() ?: emptyList()
        } catch (e: Exception) {
            logger.warning("Error getting ACL rules for user [$username]: ${e.message} [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    override suspend fun getAllAclRules(): List<AclRule> {
        return try {
            usersAclCollection?.find()
                ?.sort(Document("priority", -1))
                ?.map { documentToAclRule(it) }
                ?.toList() ?: emptyList()
        } catch (e: Exception) {
            logger.warning("Error getting all ACL rules: ${e.message} [${Utils.getCurrentFunctionName()}]")
            emptyList()
        }
    }

    override suspend fun loadAllUsersAndAcls(): Pair<List<User>, List<AclRule>> {
        return Pair(getAllUsers(), getAllAclRules())
    }
}