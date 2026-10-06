package at.rocworks

import at.rocworks.data.AclRule
import at.rocworks.data.User
import auth.AclCache
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * ACL rule semantics shared with the Go edge broker (internal/auth/auth.go):
 * rules are checked by descending priority (deny first on equal priority) and
 * the first matching rule that decides wins. A rule with canSubscribe=false and
 * canPublish=false is a deny rule; any other rule allows only the operations set
 * to true and is skipped for the other operation.
 */
class AclCacheDenyRulesTest {

    private fun cache(vararg rules: AclRule): AclCache {
        val cache = AclCache()
        cache.load(listOf(User("alice", "x", enabled = true, canSubscribe = true, canPublish = true)), rules.toList())
        return cache
    }

    private fun rule(pattern: String, sub: Boolean, pub: Boolean, priority: Int) =
        AclRule("", "alice", pattern, canSubscribe = sub, canPublish = pub, priority = priority)

    @Test
    fun singleOperationRulesDoNotCrossContaminate() {
        val c = cache(
            rule("telemetry/#", sub = false, pub = true, priority = 30),
            rule("commands/#", sub = true, pub = false, priority = 20),
            rule("#", sub = true, pub = false, priority = 10),
        )
        assertTrue(c.checkPublishPermission("alice", "telemetry/a"))
        assertTrue(c.checkSubscribePermission("alice", "telemetry/a"))
        assertTrue(c.checkSubscribePermission("alice", "commands/a"))
        assertFalse(c.checkPublishPermission("alice", "commands/a"))
        assertTrue(c.checkSubscribePermission("alice", "other/a"))
        assertFalse(c.checkPublishPermission("alice", "other/a"))
    }

    @Test
    fun denyRuleOverridesLowerPriorityAllow() {
        val c = cache(
            rule("secret/public/#", sub = true, pub = true, priority = 200),
            rule("secret/#", sub = false, pub = false, priority = 100),
            rule("#", sub = true, pub = true, priority = 1),
        )
        assertTrue(c.checkPublishPermission("alice", "data/x"))
        assertTrue(c.checkSubscribePermission("alice", "data/x"))
        assertFalse(c.checkPublishPermission("alice", "secret/x"))
        assertFalse(c.checkSubscribePermission("alice", "secret/x"))
        assertFalse(c.checkSubscribePermission("alice", "secret"))
        assertTrue(c.checkPublishPermission("alice", "secret/public/x"))
        assertTrue(c.checkSubscribePermission("alice", "secret/public/x"))
        assertFalse(c.checkSubscribePermission("alice", "secret/#"))
        assertFalse(c.checkSubscribePermission("alice", "secret/+"))
        assertTrue(c.checkSubscribePermission("alice", "secret/public/#"))
        // Admitted; secret/ topics are filtered at delivery time
        assertTrue(c.checkSubscribePermission("alice", "#"))
        assertTrue(c.checkSubscribePermission("alice", "+/x"))
    }

    @Test
    fun denyPublishOnly() {
        val c = cache(
            rule("machine/#", sub = true, pub = false, priority = 20),
            rule("machine/#", sub = false, pub = false, priority = 10),
            rule("#", sub = true, pub = true, priority = 1),
        )
        assertTrue(c.checkSubscribePermission("alice", "machine/x"))
        assertFalse(c.checkPublishPermission("alice", "machine/x"))
        assertTrue(c.checkPublishPermission("alice", "other/x"))
    }

    @Test
    fun denyWinsOnEqualPriority() {
        val c = cache(
            rule("#", sub = true, pub = true, priority = 5),
            rule("secret/#", sub = false, pub = false, priority = 5),
        )
        assertFalse(c.checkPublishPermission("alice", "secret/x"))
        assertTrue(c.checkPublishPermission("alice", "data/x"))
    }

    @Test
    fun filterCoverageAndDollarTopics() {
        val c = cache(
            rule("a/+", sub = true, pub = false, priority = 10),
            rule("#", sub = false, pub = true, priority = 1),
        )
        assertTrue(c.checkSubscribePermission("alice", "a/b"))
        assertTrue(c.checkSubscribePermission("alice", "a/+"))
        assertFalse(c.checkSubscribePermission("alice", "a/#")) // a/+ does not cover a/b/c
        assertFalse(c.checkPublishPermission("alice", "\$SYS/x")) // "#" does not cover $-topics
        assertTrue(c.checkPublishPermission("alice", "data/x"))
    }

    @Test
    fun disabledUserIsDenied() {
        val c = AclCache()
        c.load(listOf(User("alice", "x", enabled = false)), emptyList())
        assertFalse(c.checkPublishPermission("alice", "any/topic"))
    }

    @Test
    fun noRulesAllowsByGlobalPermission() {
        val c = cache()
        assertTrue(c.checkPublishPermission("alice", "any/topic"))
        assertEquals(0, c.getCacheStats()["userAcls"])
    }
}
