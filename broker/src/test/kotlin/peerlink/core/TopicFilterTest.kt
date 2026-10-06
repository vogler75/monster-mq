package at.rocworks.peerlink.core

import org.junit.Assert.*
import org.junit.Test

class TopicFilterTest {

    @Test
    fun testTopicFilterMatching() {
        val f = TopicFilter.compile(listOf("a/b", "pre/#", "x/+/z", "m/+/#", "+/leaf"))
        val cases = mapOf(
            "a/b" to true, "a/b/c" to false, "a" to false,
            "pre" to true, "pre/x" to true, "pre/x/y" to true, "prefix" to false, "pr" to false,
            "x/y/z" to true, "x/y" to false, "x/y/z/w" to false, "x//z" to true,
            "m/1" to true, "m/1/2/3" to true, "m" to false,
            "q/leaf" to true, "q/leaf/x" to false,
            "other" to false
        )
        for ((topic, want) in cases) {
            assertEquals("match($topic)", want, f.match(topic))
        }

        val all = TopicFilter.compile(listOf("#"))
        assertTrue(all.match("anything/at/all"))

        val empty = TopicFilter.compile(emptyList())
        assertFalse(empty.match("x"))

        for (bad in listOf("a/#/b", "a+/b", "a/b#", "")) {
            try {
                TopicFilter.compile(listOf(bad))
                fail("Filter '$bad' should have been rejected")
            } catch (e: IllegalArgumentException) {
                // Expected
            }
        }

        val ie = IncludeExclude.create(emptyList(), listOf("secret/#"))
        assertTrue(ie.accept("public"))
        assertFalse(ie.accept("secret/x"))
        assertFalse(ie.accept("secret"))
    }

    @Test
    fun testUnderRoot() {
        val cases = listOf(
            Triple("winccoa", "winccoa", true),
            Triple("winccoa/x", "winccoa", true),
            Triple("winccoax", "winccoa", false),
            Triple("winc", "winccoa", false),
            Triple("x", "", false),
            Triple("a/b/c", "a/b", true)
        )
        for ((t, root, want) in cases) {
            assertEquals("underRoot($t, $root)", want, TopicFilter.underRoot(t, root))
        }
    }
}
