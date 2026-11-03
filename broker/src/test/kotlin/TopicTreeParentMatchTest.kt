package at.rocworks

import at.rocworks.data.TopicTree
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/** MQTT §4.7.1.2: a trailing "#" also matches the parent level. */
class TopicTreeParentMatchTest {

    private fun tree(vararg filters: String) = TopicTree<String, Int>().apply {
        filters.forEach { add(it, it, 1) }
    }

    private fun matching(t: TopicTree<String, Int>, topic: String) = t.findDataOfTopicName(topic).map { it.first }.toSet()

    @Test
    fun hashAfterPlusMatchesParent() {
        val t = tree("a/+/#")
        assertEquals(setOf("a/+/#"), matching(t, "a/b"))
        assertEquals(setOf("a/+/#"), matching(t, "a/b/c"))
        assertTrue(matching(t, "a").isEmpty())
        assertTrue(t.isTopicNameMatching("a/b"))
        assertFalse(t.isTopicNameMatching("a"))
    }

    @Test
    fun hashAfterExactLevelMatchesParent() {
        val t = tree("a/b/#")
        assertEquals(setOf("a/b/#"), matching(t, "a/b"))
        assertTrue(t.isTopicNameMatching("a/b"))
    }

    @Test
    fun plusDoesNotMatchParent() {
        val t = tree("a/+", "a/+/+")
        assertTrue(matching(t, "a").isEmpty())
        assertEquals(setOf("a/+"), matching(t, "a/b"))
    }
}
