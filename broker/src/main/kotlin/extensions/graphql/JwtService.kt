package at.rocworks.extensions.graphql

import io.jsonwebtoken.Claims
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.security.Keys
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.logging.Logger
import javax.crypto.SecretKey

object JwtService {
    private val logger: Logger = Logger.getLogger(JwtService::class.java.name)
    
    // Generate a secure key for JWT signing
    // In production, this should be configurable and stored securely
    private val secretKey: SecretKey = Jwts.SIG.HS256.key().build()
    
    // Token expiration time (24 hours)
    private const val TOKEN_EXPIRATION_HOURS = 24L
    
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

