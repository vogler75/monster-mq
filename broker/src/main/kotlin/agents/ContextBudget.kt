package at.rocworks.agents

/**
 * Fits injected agent context data into a token budget.
 *
 * Tokens are estimated as characters / 4, which is close enough for English text, topic names and CSV
 * for all supported providers and avoids a tokenizer dependency per provider.
 *
 * The budget is shared between sections by water-filling: small sections are kept completely and
 * the remaining budget is split evenly between the larger ones. A section's title and head lines
 * (e.g. a CSV header) are always kept. History sections keep their newest (last) rows.
 */
object ContextBudget {

    data class Section(
        val title: String?,
        val lines: List<String>,
        /** Number of leading lines that are always kept (e.g. the CSV header). */
        val headLines: Int = 0,
        /** Keep the last lines instead of the first ones when truncating (newest history rows). */
        val keepTail: Boolean = false
    ) {
        val head: List<String> get() = lines.take(headLines)
        val body: List<String> get() = lines.drop(headLines)
    }

    fun estimateTokens(text: String): Int = (text.length + 3) / 4

    private fun lineTokens(line: String): Int = estimateTokens(line) + 1  // + newline

    private fun linesTokens(lines: List<String>): Int = lines.sumOf { lineTokens(it) }

    fun truncationMarker(count: Int) = "[... $count lines truncated to fit context budget ...]"

    fun sectionTokens(section: Section): Int =
        (section.title?.let { lineTokens(it) } ?: 0) + linesTokens(section.lines)

    /**
     * Returns the sections truncated so that their rendered size stays within [maxTokens].
     * [maxTokens] <= 0 means unlimited.
     */
    fun fit(sections: List<Section>, maxTokens: Int): List<Section> {
        if (maxTokens <= 0) return sections
        if (sections.sumOf { sectionTokens(it) } <= maxTokens) return sections

        // Fixed part of every section, including a potential truncation marker
        val markerTokens = lineTokens(truncationMarker(999999))
        val fixed = sections.map { s -> (s.title?.let { lineTokens(it) } ?: 0) + linesTokens(s.head) }
        val bodyCosts = sections.map { linesTokens(it.body) }
        var available = maxTokens - fixed.sum()

        // Water-filling over the bodies, smallest first
        val allotment = IntArray(sections.size)
        val order = sections.indices.sortedBy { bodyCosts[it] }
        var remainingSections = order.size
        for (i in order) {
            val share = if (available > 0) available / remainingSections else 0
            val cost = bodyCosts[i]
            allotment[i] = if (cost <= share) cost else maxOf(0, share - markerTokens)
            available -= if (cost <= share) cost else share
            remainingSections--
        }

        return sections.mapIndexed { i, section -> truncate(section, allotment[i]) }
    }

    private fun truncate(section: Section, bodyBudget: Int): Section {
        val body = section.body
        if (linesTokens(body) <= bodyBudget) return section

        val kept = ArrayList<String>()
        var used = 0
        val candidates = if (section.keepTail) body.asReversed() else body
        for (line in candidates) {
            val cost = lineTokens(line)
            if (used + cost > bodyBudget) break
            kept.add(line)
            used += cost
        }
        val marker = truncationMarker(body.size - kept.size)
        val newBody = if (section.keepTail) listOf(marker) + kept.asReversed() else kept + marker
        return section.copy(lines = section.head + newBody)
    }

    fun render(sections: List<Section>): List<String> =
        sections.flatMap { s -> listOfNotNull(s.title) + s.lines }
}
