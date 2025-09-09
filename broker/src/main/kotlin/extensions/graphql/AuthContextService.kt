package at.rocworks.extensions.graphql

/**
 * Thread-local service to manage authentication context for GraphQL requests
 * This allows resolvers to access the current request's auth context without 
 * complex GraphQL context injection
 */
object AuthContextService {
    private val contextHolder = ThreadLocal<AuthContext?>()
    
    /**
     * Set the auth context for the current thread (request)
     */
    fun setAuthContext(authContext: AuthContext?) {
        contextHolder.set(authContext)
    }
    
    /**
     * Get the auth context for the current thread (request)
     */
    fun getAuthContext(): AuthContext? {
        return contextHolder.get()
    }
    
    /**
     * Clear the auth context for the current thread
     * Should be called at the end of each request to prevent memory leaks
     */
    fun clearAuthContext() {
        contextHolder.remove()
    }
}