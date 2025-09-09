package at.rocworks.tools

import org.mindrot.jbcrypt.BCrypt

/**
 * Simple command-line utility to generate BCrypt password hashes
 * 
 * Usage: kotlin GeneratePasswordHash <password> [rounds]
 * 
 * Arguments:
 *   password - The password to hash
 *   rounds   - BCrypt rounds (optional, default: 12, range: 4-31)
 */
fun main(args: Array<String>) {
    if (args.isEmpty()) {
        println("Password Hash Generator")
        println("=====================")
        println()
        println("Usage: kotlin GeneratePasswordHash <password> [rounds]")
        println()
        println("Arguments:")
        println("  password - The password to hash (required)")
        println("  rounds   - BCrypt cost factor (optional, default: 12, range: 4-31)")
        println()
        println("Examples:")
        println("  kotlin GeneratePasswordHash \"mySecretPassword\"")
        println("  kotlin GeneratePasswordHash \"mySecretPassword\" 10")
        println()
        println("Note: Higher rounds = more secure but slower hashing")
        return
    }
    
    val password = args[0]
    val rounds = if (args.size > 1) {
        try {
            val r = args[1].toInt()
            if (r < 4 || r > 31) {
                println("Error: rounds must be between 4 and 31")
                return
            }
            r
        } catch (e: NumberFormatException) {
            println("Error: rounds must be a valid number")
            return
        }
    } else {
        12 // Default BCrypt rounds
    }
    
    if (password.isEmpty()) {
        println("Error: Password cannot be empty")
        return
    }
    
    try {
        println("Generating BCrypt hash...")
        println()
        
        val startTime = System.currentTimeMillis()
        val salt = BCrypt.gensalt(rounds)
        val hash = BCrypt.hashpw(password, salt)
        val duration = System.currentTimeMillis() - startTime
        
        println("Password: $password")
        println("Rounds:   $rounds")
        println("Hash:     $hash")
        println("Time:     ${duration}ms")
        println()
        
        // Verify the hash works
        val isValid = BCrypt.checkpw(password, hash)
        println("Verification: ${if (isValid) "✓ PASSED" else "✗ FAILED"}")
        
    } catch (e: Exception) {
        println("Error generating hash: ${e.message}")
    }
}