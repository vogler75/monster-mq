package at.rocworks.extensions

import org.junit.Assert.assertEquals
import org.junit.Test

class I3xCatalogMappingTest {
    @Test fun emptyAndUnmappedCatalogPreserveTopics() {
        assertEquals("plant/pump", I3xCatalogMapping.topic(emptyMap(), "plant/pump"))
        assertEquals("plant/pump", I3xCatalogMapping.topic(mapOf("pump" to "other"), "plant/pump"))
        assertEquals("pump-10", I3xCatalogMapping.topic(mapOf("pump-1" to "plant/one"), "pump-10"))
    }

    @Test fun resolvesRootDescendantsAndMostSpecificInstance() {
        val mappings = mapOf("pump" to "plant/one", "pump/sensor" to "remote/sensor")
        assertEquals("plant/one", I3xCatalogMapping.topic(mappings, "pump"))
        assertEquals("plant/one/speed", I3xCatalogMapping.topic(mappings, "pump/speed"))
        assertEquals("remote/sensor/value", I3xCatalogMapping.topic(mappings, "pump/sensor/value"))
    }

    @Test fun catalogDoesNotChangeUnmappedWildcardSubscriptions() {
        val mappings = mapOf("pump" to "plant/one")
        assertEquals(listOf("other/two"), I3xCatalogMapping.subscriptionIds(mappings, setOf("other/+"), "other/two", 1))
        assertEquals(listOf("other/two/temperature"), I3xCatalogMapping.subscriptionIds(mappings, setOf("other/#"), "other/two/temperature", 1))
        assertEquals(listOf("other/two"), I3xCatalogMapping.subscriptionIds(mappings, setOf("/other/two/"), "other/two", 1))
    }

    @Test fun overlappingCatalogIdsUseTheSameTopicForReadsAndSubscriptions() {
        val mappings = mapOf("pump" to "plant/one", "pump/sensor" to "remote/sensor")
        val ids = setOf("pump")
        assertEquals(emptyList<String>(), I3xCatalogMapping.subscriptionIds(mappings, ids, "plant/one/sensor", 0))
        assertEquals(listOf("pump/sensor"), I3xCatalogMapping.subscriptionIds(mappings, ids, "remote/sensor", 0))
        assertEquals(emptyList<String>(), I3xCatalogMapping.subscriptionIds(mappings, ids, "remote/sensor", 1))
        assertEquals(mapOf("pump" to 2, "pump/sensor" to 1), I3xCatalogMapping.subscriptionBindings(mappings, ids, 2))
        assertEquals(emptyList<String>(), I3xCatalogMapping.subscriptionIds(mappings, ids, "remote/sensor/deep", 2))
        assertEquals(listOf("pump/sensor/deep"), I3xCatalogMapping.subscriptionIds(mappings, ids, "remote/sensor/deep", 3))
    }

    @Test fun identityParentMappingCannotEmitAShadowedChild() {
        val mappings = mapOf("pump" to "pump", "pump/sensor" to "remote/sensor")
        assertEquals(emptyList<String>(), I3xCatalogMapping.subscriptionIds(mappings, setOf("pump"), "pump/sensor", 0))
        assertEquals(listOf("pump/sensor"), I3xCatalogMapping.subscriptionIds(mappings, setOf("pump"), "remote/sensor", 0))
    }

    @Test fun subscriptionsPreserveAliasesAndDepth() {
        val mappings = mapOf("pump" to "plant/one", "second-name" to "plant/one")
        val ids = setOf("pump", "second-name", "plant/one")
        assertEquals(listOf("pump", "second-name", "plant/one"),
            I3xCatalogMapping.subscriptionIds(mappings, ids, "plant/one", 1))
        assertEquals(emptyList<String>(), I3xCatalogMapping.subscriptionIds(mappings, ids, "plant/one/speed", 1))
        assertEquals(listOf("pump/speed", "second-name/speed", "plant/one/speed"),
            I3xCatalogMapping.subscriptionIds(mappings, ids, "plant/one/speed", 2))
        assertEquals(emptyList<String>(), I3xCatalogMapping.subscriptionIds(mappings, ids, "plant/one/a/b", 2))
        assertEquals(listOf("pump/a/b", "second-name/a/b", "plant/one/a/b"),
            I3xCatalogMapping.subscriptionIds(mappings, ids, "plant/one/a/b", 0))
    }
}
