package at.rocworks.stores.mongodb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.AclRule
import at.rocworks.data.User
import at.rocworks.stores.StoreType
import at.rocworks.stores.IUserStore
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.ReplaceOptions
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import org.bson.Document
import org.mindrot.jbcrypt.BCrypt
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import java.util.concurrent.Callable

class UserStoreMongoDB(
    private val url: String,
    private val database: String,
    private val vertx: Vertx
): IUserStore {
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

    override fun getType(): StoreType = StoreType.MONGODB

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

    override fun init(): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            try {
                mongoClient = MongoClients.create(url)
                mongoDatabase = mongoClient!!.getDatabase(database)
                usersCollection = mongoDatabase!!.getCollection(usersCollectionName)
                usersAclCollection = mongoDatabase!!.getCollection(usersAclCollectionName)

                // Create indexes
                usersAclCollection!!.createIndex(Indexes.ascending("username"))
                usersAclCollection!!.createIndex(Indexes.descending("priority"))

                logger.info("MongoDB user management store initialized [${Utils.getCurrentFunctionName()}]")
                true
            } catch (e: Exception) {
                logger.severe("Failed to initialize MongoDB user management store: ${e.message} [${Utils.getCurrentFunctionName()}]")
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
                mongoClient?.close()
            } catch (e: Exception) {
                logger.warning("Error closing MongoDB connection: ${e.message}")
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
            try {
                usersCollection?.insertOne(userToDocument(user))
                true
            } catch (e: Exception) {
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
            try {
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
            try {
                // Delete user's ACL rules first
                usersAclCollection?.deleteMany(Filters.eq("username", username))
                // Delete user
                val result = usersCollection?.deleteOne(Filters.eq("_id", username))
                result?.deletedCount ?: 0 > 0
            } catch (e: Exception) {
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
            try {
                val doc = usersCollection?.find(Filters.eq("_id", username))?.first()
                doc?.let { documentToUser(it) }
            } catch (e: Exception) {
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
            try {
                usersCollection?.find()?.sort(Document("_id", 1))?.map { documentToUser(it) }?.toList() ?: emptyList()
            } catch (e: Exception) {
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
            try {
                val ruleWithId = if (rule.id.isEmpty()) rule.copy(id = UUID.randomUUID().toString()) else rule
                val ruleWithTimestamp = ruleWithId.copy(createdAt = LocalDateTime.now())
                usersAclCollection?.insertOne(aclRuleToDocument(ruleWithTimestamp))
                true
            } catch (e: Exception) {
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
            try {
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
            try {
                val result = usersAclCollection?.deleteOne(Filters.eq("_id", id))
                result?.deletedCount ?: 0 > 0
            } catch (e: Exception) {
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
            try {
                usersAclCollection?.find(Filters.eq("_id", id))
                    ?.first()
                    ?.let { documentToAclRule(it) }
            } catch (e: Exception) {
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
            try {
                usersAclCollection?.find(Filters.eq("username", username))
                    ?.sort(Document("priority", -1))
                    ?.map { documentToAclRule(it) }
                    ?.toList() ?: emptyList()
            } catch (e: Exception) {
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
            try {
                usersAclCollection?.find()
                    ?.sort(Document("priority", -1))
                    ?.map { documentToAclRule(it) }
                    ?.toList() ?: emptyList()
            } catch (e: Exception) {
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