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

    @Test
    fun testMaskNodeMatch() {
        val n = MaskNode()
        n.set("a/b", 1L, true)
        n.set("a/+", 2L, true)
        n.set("a/#", 4L, true)
        n.set("#", 8L, true)
        n.set("+/+/c", 16L, true)
        n.set("\$SYS/#", 32L, true)
        assertEquals(1L or 2L or 4L or 8L, n.match("a/b"))
        assertEquals(4L or 8L, n.match("a"))
        assertEquals(2L or 4L or 8L, n.match("a/"))
        assertEquals(4L or 8L or 16L, n.match("a/b/c"))
        assertEquals(8L or 16L, n.match("x/y/c"))
        assertEquals(8L, n.match("x"))
        // Leading wildcards do not match '$' topics.
        assertEquals(32L, n.match("\$SYS/x"))
        assertEquals(0L, n.match("\$other/x"))
    }

    @Test
    fun testMaskNodeClearPrunes() {
        val n = MaskNode()
        n.set("a/b/c", 1L, true)
        n.set("a/+/c", 2L, true)
        n.set("a/b/c", 2L, true)
        assertEquals(3L, n.match("a/b/c"))
        n.set("a/b/c", 1L, false)
        assertEquals(2L, n.match("a/b/c"))
        n.set("a/b/c", 2L, false)
        n.set("a/+/c", 2L, false)
        assertEquals(0L, n.match("a/b/c"))
        assertTrue(n.isEmpty())
        // Clearing a filter that was never set is a no-op.
        n.set("x/y", 1L, false)
        assertTrue(n.isEmpty())
    }
}
