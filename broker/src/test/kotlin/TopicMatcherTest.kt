package at.rocworks

import at.rocworks.data.TopicTree
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class TopicMatcherTest {
    @Test
    fun testExactMatch() {
        assertTrue(TopicTree.matches("a/b/c", "a/b/c"))
        assertFalse(TopicTree.matches("a/b/c", "a/b"))
        assertFalse(TopicTree.matches("a/b", "a/b/c"))
    }

    @Test
    fun testSingleLevelWildcard() {
        assertTrue(TopicTree.matches("a/+/c", "a/b/c"))
        assertTrue(TopicTree.matches("+/b/c", "a/b/c"))
        assertTrue(TopicTree.matches("+/+/+", "a/b/c"))
        assertFalse(TopicTree.matches("a/+/c", "a/b/d"))
        assertFalse(TopicTree.matches("a/+", "a/b/c"))
    }

    @Test
    fun testMultiLevelWildcard() {
        assertTrue(TopicTree.matches("a/#", "a"))
        assertTrue(TopicTree.matches("a/#", "a/b"))
        assertTrue(TopicTree.matches("a/#", "a/b/c/d"))
        assertTrue(TopicTree.matches("#", "a/b/c"))
        assertTrue(TopicTree.matches("#", "a"))
        assertFalse(TopicTree.matches("a/b#", "a/b")) // invalid pattern style should not match
    }

    @Test
    fun testMixedWildcards() {
        assertTrue(TopicTree.matches("a/+/+/#", "a/b/c/d/e"))
        assertTrue(TopicTree.matches("a/+/c/#", "a/b/c"))
        assertFalse(TopicTree.matches("a/+/c/#", "a/b/x"))
    }

    @Test
    fun testEdgeCases() {
        assertFalse(TopicTree.matches("", "")) // empty filter not supported in current impl
        assertFalse(TopicTree.matches("+", ""))
        assertTrue(TopicTree.matches("+/b", "a/b"))
        assertFalse(TopicTree.matches("+/b", "/b"))
    }
}
