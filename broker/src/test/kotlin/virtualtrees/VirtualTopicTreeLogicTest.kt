package at.rocworks.virtualtrees

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

/** Semantics shared with the Go edge broker and the acceptance oracle (issue #217). */
class VirtualTopicTreeLogicTest {

    private fun node(id: String, parent: String?, segment: String, kind: String? = null, target: String = "", access: String = VtTree.READ_ONLY) =
        VtNode(id, parent, segment, null, kind?.let { VtBinding(it, target, access) })

    private fun tree(id: String, root: String, vararg nodes: VtNode, enabled: Boolean = true) =
        VtTree(VtTree.SCHEMA_VERSION, id, id, root, null, enabled, nodes.toList())

    // v/plant/area mounts plc/line1 read-write, v/plant/area/power overrides it with an exact read-only binding
    private val example = tree(
        "t1", "v",
        node("plant", null, "plant"),
        node("area", "plant", "area", VtTree.SUBTREE, "plc/line1", VtTree.READ_WRITE),
        node("power", "area", "power", VtTree.EXACT, "meter/p")
    )

    private val resolver = VtResolver(listOf(example))

    @Test
    fun validTreeHasNoIssues() {
        assertTrue(VtRules.validate(example, emptyList()).isEmpty())
    }

    @Test
    fun duplicateSegmentsAreRejected() {
        val t = tree("t2", "w", node("a", null, "x"), node("b", null, "x"))
        assertTrue(VtRules.validate(t, emptyList()).any { it.code == VtCodes.DUPLICATE_PATH })
    }

    @Test
    fun overlappingRootsConflict() {
        val t = tree("t2", "v/plant", node("a", null, "x"))
        assertTrue(VtRules.validate(t, listOf(example)).any { it.code == VtCodes.ROOT_CONFLICT })
    }

    @Test
    fun targetInsideVirtualRootIsRejected() {
        val t = tree("t2", "w", node("a", null, "x", VtTree.EXACT, "v/plant/area/y"))
        assertTrue(VtRules.validate(t, listOf(example)).any { it.code == VtCodes.TARGET_IN_VIRTUAL_ROOT })
    }

    @Test
    fun publishResolvesMountAndRespectsAccess() {
        assertEquals("plc/line1/a/b", resolver.publish("v/plant/area/a/b").canonical)
        assertEquals("plc/line1", resolver.publish("v/plant/area").canonical)
        assertEquals(VtCodes.REJECT_READ_ONLY, resolver.publish("v/plant/area/power").reject)
        assertEquals(VtCodes.REJECT_UNBOUND, resolver.publish("v/plant/x").reject)
        assertEquals("other/topic", resolver.publish("other/topic").canonical)
    }

    @Test
    fun disabledTreeKeepsRootReserved() {
        val r = VtResolver(listOf(example.copy(enabled = false)))
        assertEquals(VtCodes.REJECT_DISABLED, r.publish("v/plant/area/a").reject)
        assertTrue(r.inVirtualNamespace("v/plant/area/a"))
        assertNull(r.readResolve("v/plant/area/a"))
        assertTrue(r.project("plc/line1/a").isEmpty())
    }

    @Test
    fun projectionHonoursOverrides() {
        assertEquals(listOf("v/plant/area/a"), resolver.project("plc/line1/a").map { it.virtual })
        assertEquals(listOf("v/plant/area/power"), resolver.project("meter/p").map { it.virtual })
        // The mount's own "power" descendant is shadowed by the exact binding
        assertTrue(resolver.project("plc/line1/power").isEmpty())
        assertEquals(listOf("plc/line1/a", "v/plant/area/a"), resolver.presentations("plc/line1/a"))
    }

    @Test
    fun readResolveMapsVirtualTopicsOnly() {
        assertEquals("plc/line1/x", resolver.readResolve("v/plant/area/x"))
        assertEquals("meter/p", resolver.readResolve("v/plant/area/power"))
        assertNull(resolver.readResolve("v/plant"))
        assertEquals("a/b", resolver.readResolve("a/b"))
    }

    @Test
    fun canonicalQueriesCoverVirtualFilter() {
        val qs = resolver.canonicalQueries("v/plant/area/#").toSet()
        assertTrue("plc/line1/#" in qs && "plc/line1" in qs && "meter/p" in qs)
        assertTrue(resolver.canonicalQueries("v/other/#").isEmpty())
    }

    @Test
    fun findPresentedHidesPhysicalTopicsInRoots() {
        val store = listOf("plc/line1/a", "meter/p", "v/plant/raw", "x/y")
        val finder = { f: String, cb: (String) -> Boolean -> store.filter { VtRules.match(f, it) }.forEach { if (!cb(it)) return@forEach } }
        val out = ArrayList<String>()
        resolver.findPresented("#", finder, { it }) { p, _ -> out.add(p); true }
        assertFalse("v/plant/raw" in out)
        assertTrue(out.containsAll(listOf("plc/line1/a", "x/y", "v/plant/area/a", "v/plant/area/power")))
    }
}
