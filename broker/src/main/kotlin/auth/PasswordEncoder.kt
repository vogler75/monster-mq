package auth

import org.mindrot.jbcrypt.BCrypt

object PasswordEncoder {
    
    private const val BCRYPT_SALT_ROUNDS = 12
    
    /**
     * Hash a password using BCrypt
     * @param password The plain text password to hash
     * @return The hashed password
     */
    fun hash(password: String): String {
        return BCrypt.hashpw(password, BCrypt.gensalt(BCRYPT_SALT_ROUNDS))
    }
    
    /**
     * Verify a password against a hash
     * @param password The plain text password to verify
     * @param hash The hash to verify against
     * @return True if the password matches the hash, false otherwise
     */
    fun verify(password: String, hash: String): Boolean {
        return try {
            BCrypt.checkpw(password, hash)
        } catch (e: Exception) {
            false
        }
    }
    
    /**
     * Check if a password meets minimum security requirements
     * @param password The password to validate
     * @return True if password meets requirements, false otherwise
     */
    fun isValidPassword(password: String): Boolean {
        return password.length >= 8 && 
               password.any { it.isUpperCase() } &&
               password.any { it.isLowerCase() } &&
               password.any { it.isDigit() }
    }
}