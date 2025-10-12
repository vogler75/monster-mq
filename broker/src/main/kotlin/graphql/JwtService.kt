package at.rocworks.extensions.graphql

import at.rocworks.Utils
import io.jsonwebtoken.Claims
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.security.Keys
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.logging.Logger
import javax.crypto.SecretKey

object JwtService {
    private val logger: Logger = Utils.getLogger(JwtService::class.java)

    private const val JWT_KEY_FILE = "./security/jwt-secret.key"

    // Load or generate a secure key for JWT signing
    // The key is persisted to disk so tokens remain valid across broker restarts
    private val secretKey: SecretKey = loadOrGenerateSecretKey()
    
    // Token expiration time (24 hours)
    private const val TOKEN_EXPIRATION_HOURS = 24L

    /**
     * Load the JWT secret key from disk, or generate and save a new one if it doesn't exist
     */
    private fun loadOrGenerateSecretKey(): SecretKey {
        val keyFile = File(JWT_KEY_FILE)

        return if (keyFile.exists()) {
            try {
                logger.info("Loading existing JWT secret key from $JWT_KEY_FILE")
                val keyBytes = Files.readAllBytes(Paths.get(JWT_KEY_FILE))
                Keys.hmacShaKeyFor(keyBytes)
            } catch (e: Exception) {
                logger.warning("Failed to load JWT key from $JWT_KEY_FILE: ${e.message}. Generating new key.")
                generateAndSaveSecretKey()
            }
        } else {
            logger.info("JWT secret key not found. Generating new key and saving to $JWT_KEY_FILE")
            generateAndSaveSecretKey()
        }
    }

    /**
     * Generate a new secret key and save it to disk
     */
    private fun generateAndSaveSecretKey(): SecretKey {
        val key = Jwts.SIG.HS256.key().build()
        try {
            Files.write(Paths.get(JWT_KEY_FILE), key.encoded)
            logger.info("JWT secret key saved to $JWT_KEY_FILE")
        } catch (e: Exception) {
            logger.severe("Failed to save JWT secret key to $JWT_KEY_FILE: ${e.message}")
        }
        return key
    }

    /**
     * Generate a JWT token for a user
     */
    fun generateToken(username: String, isAdmin: Boolean): String {
        val now = Instant.now()
        val expiration = now.plus(TOKEN_EXPIRATION_HOURS, ChronoUnit.HOURS)
        
        return Jwts.builder()
            .subject(username)
            .claim("isAdmin", isAdmin)
            .issuedAt(Date.from(now))
            .expiration(Date.from(expiration))
            .signWith(secretKey)
            .compact()
    }
    
    /**
     * Validate and extract claims from a JWT token
     * Returns null if token is invalid or expired
     */
    fun validateToken(token: String): Claims? {
        return try {
            Jwts.parser()
                .verifyWith(secretKey)
                .build()
                .parseSignedClaims(token)
                .payload
        } catch (e: Exception) {
            // Token is invalid, expired, or malformed
            null
        }
    }
    
    /**
     * Extract username from JWT token
     */
    fun extractUsername(token: String): String? {
        return try {
            validateToken(token)?.subject
        } catch (e: Exception) {
            logger.warning("Error extracting username from JWT: ${e.message}")
            null
        }
    }
    
    /**
     * Extract admin status from JWT token
     */
    fun extractIsAdmin(token: String): Boolean {
        return try {
            val claims = validateToken(token)
            val isAdminClaim = claims?.get("isAdmin")
            when (isAdminClaim) {
                is Boolean -> isAdminClaim
                is String -> isAdminClaim.toBoolean()
                else -> false
            }
        } catch (e: Exception) {
            logger.warning("Error extracting isAdmin claim from JWT: ${e.message}")
            false
        }
    }
    
    /**
     * Check if token is expired
     */
    fun isTokenExpired(token: String): Boolean {
        val claims = validateToken(token)
        return claims?.expiration?.before(Date()) ?: true
    }
    
    /**
     * Extract token from Authorization header (Bearer token)
     */
    fun extractTokenFromHeader(authorizationHeader: String?): String? {
        if (authorizationHeader == null || !authorizationHeader.startsWith("Bearer ")) {
            return null
        }
        return authorizationHeader.substring(7) // Remove "Bearer " prefix
    }
}

/**
 * Data class representing the authenticated user context
 */
data class AuthContext(
    val username: String,
    val isAdmin: Boolean,
    val token: String
)

