package at.rocworks.agents

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ContextBudgetTest {

    private fun rows(n: Int, prefix: String = "row") = (1..n).map { "$prefix-$it,${"x".repeat(36)}" }

    private fun renderedTokens(sections: List<ContextBudget.Section>) =
        sections.sumOf { ContextBudget.sectionTokens(it) }

    @Test
    fun testUnlimitedAndFittingSectionsAreUnchanged() {
        val sections = listOf(ContextBudget.Section("title", rows(5)))
        assertEquals(sections, ContextBudget.fit(sections, 0))
        assertEquals(sections, ContextBudget.fit(sections, 10_000))
    }

    @Test
    fun testHistoryKeepsHeaderAndNewestRows() {
        val section = ContextBudget.Section("[History] t1:", listOf("time,value") + rows(200), headLines = 1, keepTail = true)
        val fitted = ContextBudget.fit(listOf(section), 500).single()

        assertEquals("[History] t1:", fitted.title)
        assertEquals("time,value", fitted.lines[0])
        assertTrue(fitted.lines[1].startsWith("[... "))
        assertEquals("row-200,${"x".repeat(36)}", fitted.lines.last())
        assertTrue(renderedTokens(listOf(fitted)) <= 500)
    }

    @Test
    fun testLastValuesKeepFirstLines() {
        val section = ContextBudget.Section(null, rows(200))
        val fitted = ContextBudget.fit(listOf(section), 300).single()
        assertEquals("row-1,${"x".repeat(36)}", fitted.lines.first())
        assertTrue(fitted.lines.last().startsWith("[... "))
        assertTrue(renderedTokens(listOf(fitted)) <= 300)
    }

    @Test
    fun testSmallSectionsKeptWhileLargeOnesShareTheRest() {
        val small = ContextBudget.Section(null, rows(3, "small"))
        val large1 = ContextBudget.Section("a", rows(300, "a"), keepTail = true)
        val large2 = ContextBudget.Section("b", rows(300, "b"), keepTail = true)
        val fitted = ContextBudget.fit(listOf(small, large1, large2), 1000)

        assertEquals(small, fitted[0])
        val bodyA = fitted[1].lines.size
        val bodyB = fitted[2].lines.size
        assertTrue(bodyA in 2..299 && bodyB in 2..299)
        assertTrue(kotlin.math.abs(bodyA - bodyB) <= 1)
        assertTrue(renderedTokens(fitted) <= 1000)
    }

    @Test
    fun testTinyBudgetKeepsOnlyFixedParts() {
        val section = ContextBudget.Section("title", listOf("h") + rows(50), headLines = 1)
        val fitted = ContextBudget.fit(listOf(section), 5).single()
        assertEquals(listOf("h", ContextBudget.truncationMarker(50)), fitted.lines)
    }
}
