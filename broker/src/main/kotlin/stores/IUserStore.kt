package at.rocworks.stores

import at.rocworks.data.AclRule
import at.rocworks.data.User

interface IUserStore {
    fun getType(): AuthStoreType

    // User operations
    suspend fun createUser(user: User): Boolean
    suspend fun updateUser(user: User): Boolean
    suspend fun deleteUser(username: String): Boolean
    suspend fun getUser(username: String): User?
    suspend fun getAllUsers(): List<User>
    suspend fun validateCredentials(username: String, password: String): User?
    
    // ACL operations
    suspend fun createAclRule(rule: AclRule): Boolean
    suspend fun updateAclRule(rule: AclRule): Boolean
    suspend fun deleteAclRule(id: String): Boolean
    suspend fun getAclRule(id: String): AclRule?
    suspend fun getUserAclRules(username: String): List<AclRule>
    suspend fun getAllAclRules(): List<AclRule>
    
    // Load all data for in-memory cache
    suspend fun loadAllUsersAndAcls(): Pair<List<User>, List<AclRule>>
    
    // Initialization
    suspend fun init(): Boolean
    suspend fun createTables(): Boolean
    suspend fun close()
}