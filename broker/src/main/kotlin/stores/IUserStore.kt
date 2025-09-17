package at.rocworks.stores

import at.rocworks.data.AclRule
import at.rocworks.data.User
import io.vertx.core.Future

interface IUserStore {
    fun getType(): StoreType

    // User operations
    fun createUser(user: User): Future<Boolean>
    fun updateUser(user: User): Future<Boolean>
    fun deleteUser(username: String): Future<Boolean>
    fun getUser(username: String): Future<User?>
    fun getAllUsers(): Future<List<User>>
    fun validateCredentials(username: String, password: String): Future<User?>

    // ACL operations
    fun createAclRule(rule: AclRule): Future<Boolean>
    fun updateAclRule(rule: AclRule): Future<Boolean>
    fun deleteAclRule(id: String): Future<Boolean>
    fun getAclRule(id: String): Future<AclRule?>
    fun getUserAclRules(username: String): Future<List<AclRule>>
    fun getAllAclRules(): Future<List<AclRule>>

    // Load all data for in-memory cache
    fun loadAllUsersAndAcls(): Future<Pair<List<User>, List<AclRule>>>

    // Initialization
    fun init(): Future<Boolean>
    fun createTables(): Future<Boolean>
    fun close(): Future<Void>
}