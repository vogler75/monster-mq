package at.rocworks

import at.rocworks.data.TopicTree
import org.junit.Assert.*
import org.junit.Test
import java.io.File
import kotlin.system.measureTimeMillis

class TopicMatcherTest {
    companion object {
        private const val TEST_DATA_FILE = "src/test/resources/topic-tree-test-data.txt"
        private const val REFERENCE_OUTPUT_DIR = "src/test/resources/reference-output"
        private const val TEST_OUTPUT_DIR = "src/test/resources/test-output"
        
        private val GENERATE_MODE = System.getProperty("generateReference", "false") == "true"
        
        data class ISA88Config(
            val enterprises: Int = 5,
            val sites: Int = 4,
            val areas: Int = 5,
            val lines: Int = 6,
            val cells: Int = 4,
            val units: Int = 5,
            val equipment: Int = 3
        )
        
        fun generateISA88Topics(config: ISA88Config = ISA88Config()): List<String> {
            val topics = mutableListOf<String>()
            
            for (e in 1..config.enterprises) {
                val enterprise = "enterprise$e"
                for (s in 1..config.sites) {
                    val site = "site$s"
                    for (a in 1..config.areas) {
                        val area = "area$a"
                        for (l in 1..config.lines) {
                            val line = "line$l"
                            for (c in 1..config.cells) {
                                val cell = "cell$c"
                                for (u in 1..config.units) {
                                    val unit = "unit$u"
                                    for (eq in 1..config.equipment) {
                                        val equip = "equipment$eq"
                                        topics.add("$enterprise/$site/$area/$line/$cell/$unit/$equip")
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            for (i in 1..1000) {
                topics.add("\$SYS/broker/clients/client$i")
                topics.add("\$SYS/broker/messages/received/msg$i")
                topics.add("\$SYS/broker/load/messages/sent/msg$i")
                topics.add("\$SYS/broker/uptime/hours/$i")
                topics.add("\$SYS/broker/subscriptions/count/sub$i")
            }
            
            return topics
        }
        
        fun saveTopics(topics: List<String>, filePath: String) {
            File(filePath).apply {
                parentFile?.mkdirs()
                writeText(topics.joinToString("\n"))
            }
        }
        
        fun loadTopics(filePath: String): List<String> {
            val file = File(filePath)
            return if (file.exists()) {
                file.readLines().filter { it.isNotBlank() }
            } else {
                emptyList()
            }
        }
        
        fun writeMatchResults(fileName: String, filter: String, topics: List<String>, matches: List<Boolean>) {
            val outputDir = if (GENERATE_MODE) REFERENCE_OUTPUT_DIR else TEST_OUTPUT_DIR
            val file = File(outputDir, fileName)
            file.parentFile?.mkdirs()
            
            file.printWriter().use { out ->
                out.println("Filter: $filter")
                out.println("Total topics: ${topics.size}")
                out.println("Matches: ${matches.count { it }}")
                out.println("---")
                topics.forEachIndexed { index, topic ->
                    if (matches[index]) {
                        out.println(topic)
                    }
                }
            }
        }
        
        fun compareWithReference(fileName: String): Pair<Boolean, String> {
            val referenceFile = File(REFERENCE_OUTPUT_DIR, fileName)
            val testFile = File(TEST_OUTPUT_DIR, fileName)
            
            if (!referenceFile.exists()) {
                return false to "Reference file does not exist: ${referenceFile.path}"
            }
            
            if (!testFile.exists()) {
                return false to "Test output file does not exist: ${testFile.path}"
            }
            
            val referenceContent = referenceFile.readText()
            val testContent = testFile.readText()
            
            return if (referenceContent == testContent) {
                true to "Match results identical to reference"
            } else {
                val refLines = referenceContent.lines()
                val testLines = testContent.lines()
                val diff = StringBuilder()
                diff.append("Files differ:\n")
                diff.append("Reference lines: ${refLines.size}, Test lines: ${testLines.size}\n")
                
                val maxLines = maxOf(refLines.size, testLines.size)
                for (i in 0 until minOf(10, maxLines)) {
                    val refLine = refLines.getOrNull(i) ?: "<missing>"
                    val testLine = testLines.getOrNull(i) ?: "<missing>"
                    if (refLine != testLine) {
                        diff.append("Line $i differs:\n")
                        diff.append("  Reference: $refLine\n")
                        diff.append("  Test:      $testLine\n")
                    }
                }
                
                false to diff.toString()
            }
        }
    }

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
        assertFalse(TopicTree.matches("a/b#", "a/b"))
    }

    @Test
    fun testMixedWildcards() {
        assertTrue(TopicTree.matches("a/+/+/#", "a/b/c/d/e"))
        assertTrue(TopicTree.matches("a/+/c/#", "a/b/c"))
        assertFalse(TopicTree.matches("a/+/c/#", "a/b/x"))
    }

    @Test
    fun testEdgeCases() {
        assertTrue(TopicTree.matches("", ""))
        assertTrue(TopicTree.matches("+", ""))
        assertTrue(TopicTree.matches("+/b", "a/b"))
        assertTrue(TopicTree.matches("+/b", "/b"))
    }

    @Test
    fun testSysTopicsProtection() {
        assertFalse(TopicTree.matches("#", "\$SYS/broker/clients"))
        assertFalse(TopicTree.matches("+/broker/clients", "\$SYS/broker/clients"))
        assertTrue(TopicTree.matches("\$SYS/#", "\$SYS/broker/clients"))
        assertTrue(TopicTree.matches("\$SYS/+/clients", "\$SYS/broker/clients"))
        assertTrue(TopicTree.matches("\$SYS/broker/clients", "\$SYS/broker/clients"))
        
        assertFalse(TopicTree.matches("+/#", "\$SYS/test"))
        assertFalse(TopicTree.matches("+/test", "\$SYS/test"))
        assertTrue(TopicTree.matches("\$SYS/test", "\$SYS/test"))
    }

    @Test
    fun testLargeTopicTree() {
        println("\n=== Large Topic Tree Test ===")
        
        var topics = loadTopics(TEST_DATA_FILE)
        if (topics.isEmpty()) {
            println("Generating ${ISA88Config().let { 
                it.enterprises * it.sites * it.areas * it.lines * it.cells * it.units * it.equipment + 200 
            }} topics...")
            val genTime = measureTimeMillis {
                topics = generateISA88Topics()
            }
            println("Generated ${topics.size} topics in ${genTime}ms")
            saveTopics(topics, TEST_DATA_FILE)
            println("Saved topics to $TEST_DATA_FILE")
        } else {
            println("Loaded ${topics.size} topics from $TEST_DATA_FILE")
        }
        
        val testFilters = listOf(
            "#",
            "enterprise1/#",
            "enterprise1/site1/#",
            "enterprise1/+/area1/#",
            "+/site1/+/line1/#",
            "+/+/+/+/+/+/equipment1",
            "enterprise1/site1/area1/line1/cell1/unit1/+",
            "enterprise+/+/+/+/+/+/+",
            "\$SYS/#",
            "\$SYS/broker/+/+",
            "+/broker/#",
            "enterprise1/site1/area1/+/+/unit1/equipment1",
            "+/+/area2/line3/#",
            "enterprise2/+/+/+/cell2/+/equipment2",
            "enterprise3/site3/area3/line3/cell3/unit3/equipment3",
            "\$SYS/broker/load/+/+/+",
            "enterprise+/site+/+/+/+/+/+",
            "+/+/+/line5/#"
        )
        
        println("\nRunning ${testFilters.size} filter tests...")
        testFilters.forEachIndexed { index, filter ->
            println("\nTest ${index + 1}/${testFilters.size}: Filter '$filter'")
            
            val matches = mutableListOf<Boolean>()
            val matchTime = measureTimeMillis {
                topics.forEach { topic ->
                    matches.add(TopicTree.matches(filter, topic))
                }
            }
            
            val matchCount = matches.count { it }
            println("  Matched $matchCount/${topics.size} topics in ${matchTime}ms")
            
            val fileName = "filter_${index + 1}_${filter.replace("/", "_").replace("+", "plus").replace("#", "hash")}.txt"
            writeMatchResults(fileName, filter, topics, matches)
            println("  Written results to $fileName")
            
            if (!GENERATE_MODE) {
                val (success, message) = compareWithReference(fileName)
                if (success) {
                    println("  ✓ $message")
                } else {
                    fail("Filter '$filter' results differ from reference:\n$message")
                }
            } else {
                println("  Generated reference output")
            }
        }
        
        if (GENERATE_MODE) {
            println("\n=== Reference generation complete ===")
            println("Reference files written to: $REFERENCE_OUTPUT_DIR")
        } else {
            println("\n=== All tests passed ===")
        }
    }

    @Test
    fun testTopicTreePerformance() {
        println("\n=== Topic Tree Performance Test ===")
        
        val topics = loadTopics(TEST_DATA_FILE).takeIf { it.isNotEmpty() } 
            ?: generateISA88Topics().also { saveTopics(it, TEST_DATA_FILE) }
        
        println("Testing with ${topics.size} topics")
        
        val tree = TopicTree<String, String>()
        
        val addTime = measureTimeMillis {
            topics.forEach { topic ->
                tree.add(topic, topic, "value")
            }
        }
        println("Added ${topics.size} topics to tree in ${addTime}ms (${topics.size * 1000 / addTime} topics/sec)")
        
        val testCases = listOf(
            "enterprise1/site1/area1/line1/cell1/unit1/equipment1" to "Exact match",
            "enterprise1/+/area1/+/cell1/+/equipment1" to "Mixed wildcards",
            "enterprise1/site1/#" to "Multi-level wildcard",
            "#" to "Global wildcard"
        )
        
        testCases.forEach { (filter, description) ->
            var matchCount = 0
            val findTime = measureTimeMillis {
                topics.forEach { topic ->
                    if (tree.isTopicNameMatching(topic)) {
                        matchCount++
                    }
                }
            }
            println("$description '$filter': Found $matchCount matches in ${findTime}ms")
        }
    }

    @Test
    fun testWildcardEdgeCases() {
        assertTrue(TopicTree.matches("sport/+/player1", "sport/tennis/player1"))
        assertTrue(TopicTree.matches("sport/#", "sport"))
        assertTrue(TopicTree.matches("sport/tennis/#", "sport/tennis"))
        assertTrue(TopicTree.matches("sport/tennis/#", "sport/tennis/player1"))
        assertTrue(TopicTree.matches("sport/tennis/#", "sport/tennis/player1/ranking"))
        
        assertFalse(TopicTree.matches("sport/tennis#", "sport/tennis"))
        assertFalse(TopicTree.matches("sport/tennis/#/ranking", "sport/tennis/player1/ranking"))
        
        assertTrue(TopicTree.matches("+/monitor/Clients", "/monitor/Clients"))
        assertFalse(TopicTree.matches("+/monitor/Clients", "/monitor/Clients/"))
        
        assertTrue(TopicTree.matches("+", "finance"))
        assertFalse(TopicTree.matches("+", "finance/stock"))
        
        assertTrue(TopicTree.matches("sport/+", "sport/"))
        assertFalse(TopicTree.matches("sport/+", "sport"))
    }

    @Test
    fun testComplexISA88Patterns() {
        val topics = listOf(
            "enterprise1/site1/area1/line1/cell1/unit1/equipment1/temperature",
            "enterprise1/site1/area1/line1/cell1/unit1/equipment1/pressure",
            "enterprise1/site1/area1/line1/cell1/unit2/equipment1/temperature",
            "enterprise1/site1/area2/line1/cell1/unit1/equipment1/temperature",
            "enterprise1/site2/area1/line1/cell1/unit1/equipment1/temperature",
            "enterprise2/site1/area1/line1/cell1/unit1/equipment1/temperature"
        )
        
        assertEquals(6, topics.count { TopicTree.matches("#", it) })
        assertEquals(5, topics.count { TopicTree.matches("enterprise1/#", it) })
        assertEquals(4, topics.count { TopicTree.matches("enterprise1/site1/#", it) })
        assertEquals(3, topics.count { TopicTree.matches("enterprise1/site1/area1/#", it) })
        assertEquals(2, topics.count { TopicTree.matches("enterprise1/site1/area1/line1/cell1/+/equipment1/temperature", it) })
        assertEquals(4, topics.count { TopicTree.matches("enterprise1/+/area1/#", it) })
        assertEquals(5, topics.count { TopicTree.matches("+/+/+/+/+/+/+/temperature", it) })
        assertEquals(2, topics.count { TopicTree.matches("enterprise1/site1/area1/line1/cell1/unit1/equipment1/+", it) })
    }

    @Test
    fun testMalformedFilters() {
        assertFalse(TopicTree.matches("sport/tennis#", "sport/tennis"))
        assertFalse(TopicTree.matches("sport/#/ranking", "sport/tennis/ranking"))
        assertFalse(TopicTree.matches("sport+", "sport"))
        assertFalse(TopicTree.matches("sport/+tennis", "sport/tennis"))
        assertFalse(TopicTree.matches("sport/tennis+", "sport/tennis"))
        
        assertFalse(TopicTree.matches("#/sport", "tennis/sport"))
        
        assertFalse(TopicTree.matches("sport/#/player", "sport/tennis/player"))
        
        assertFalse(TopicTree.matches("a/b#/c", "a/bc/c"))
    }

    @Test
    fun testEmptyAndSpecialTopics() {
        assertTrue(TopicTree.matches("", ""))
        assertFalse(TopicTree.matches("", "a"))
        assertFalse(TopicTree.matches("a", ""))
        
        assertTrue(TopicTree.matches("+", ""))
        assertTrue(TopicTree.matches("#", ""))
        
        assertTrue(TopicTree.matches("/", "/"))
        assertTrue(TopicTree.matches("//", "//"))
        assertTrue(TopicTree.matches("+/+", "/"))
        assertTrue(TopicTree.matches("/+", "/a"))
        assertTrue(TopicTree.matches("+/", "a/"))
        
        assertTrue(TopicTree.matches("/#", "/a/b"))
        assertTrue(TopicTree.matches("a//b", "a//b"))
        assertTrue(TopicTree.matches("a/+/b", "a//b"))
    }

    @Test
    fun testUnicodeTopics() {
        assertTrue(TopicTree.matches("température/+/mesure", "température/salle1/mesure"))
        assertTrue(TopicTree.matches("测试/+/数据", "测试/topic1/数据"))
        assertTrue(TopicTree.matches("テスト/#", "テスト/トピック/データ"))
        assertTrue(TopicTree.matches("Ñoño/+", "Ñoño/España"))
        assertTrue(TopicTree.matches("🌡️/+/📊", "🌡️/sensor1/📊"))
        
        assertTrue(TopicTree.matches("مراقبة/#", "مراقبة/جهاز/حرارة"))
    }

    @Test
    fun testFirstMatchAndAnyMatch() {
        val filters = listOf("a/b/c", "a/+/c", "a/#", "x/y/z")
        
        assertEquals("a/b/c", TopicTree.firstMatch(filters, "a/b/c"))
        assertEquals("a/+/c", TopicTree.firstMatch(filters, "a/x/c"))
        assertEquals("a/#", TopicTree.firstMatch(filters, "a/b/c/d"))
        assertNull(TopicTree.firstMatch(filters, "b/c/d"))
        
        assertTrue(TopicTree.anyMatch(filters, "a/b/c"))
        assertTrue(TopicTree.anyMatch(filters, "a/x/c"))
        assertTrue(TopicTree.anyMatch(filters, "a/b/c/d"))
        assertTrue(TopicTree.anyMatch(filters, "x/y/z"))
        assertFalse(TopicTree.anyMatch(filters, "b/c/d"))
        
        val sysFilters = listOf("#", "\$SYS/#", "sensor/+")
        assertEquals("\$SYS/#", TopicTree.firstMatch(sysFilters, "\$SYS/broker"))
        assertFalse(TopicTree.anyMatch(listOf("#", "sensor/+"), "\$SYS/test"))
    }

    @Test
    fun testFindDataOfTopicName() {
        val tree = TopicTree<String, String>()
        
        tree.add("sensor/+/temperature", "filter1", "temp_handler")
        tree.add("sensor/#", "filter2", "all_sensors")
        tree.add("sensor/kitchen/temperature", "filter3", "kitchen_temp")
        tree.add("#", "filter4", "global")
        tree.add("\$SYS/#", "filter5", "sys_monitor")
        
        val tempData = tree.findDataOfTopicName("sensor/kitchen/temperature")
        assertEquals(4, tempData.size)
        assertTrue(tempData.any { it.first == "filter1" && it.second == "temp_handler" })
        assertTrue(tempData.any { it.first == "filter2" && it.second == "all_sensors" })
        assertTrue(tempData.any { it.first == "filter3" && it.second == "kitchen_temp" })
        assertTrue(tempData.any { it.first == "filter4" && it.second == "global" })
        
        val pressureData = tree.findDataOfTopicName("sensor/kitchen/pressure")
        assertEquals(2, pressureData.size)
        assertTrue(pressureData.any { it.first == "filter2" })
        assertTrue(pressureData.any { it.first == "filter4" })
        
        val sysData = tree.findDataOfTopicName("\$SYS/broker/clients")
        assertEquals(1, sysData.size)
        assertTrue(sysData.any { it.first == "filter5" })
        
        val noSysFromGlobal = tree.findDataOfTopicName("\$SYS/test")
        assertFalse(noSysFromGlobal.any { it.first == "filter4" })
    }

    @Test
    fun testFindMatchingTopicNames() {
        val tree = TopicTree<String, String>()
        
        tree.add("sensor/kitchen/temperature")
        tree.add("sensor/kitchen/humidity")
        tree.add("sensor/living/temperature")
        tree.add("sensor/bedroom/temperature")
        tree.add("actuator/kitchen/light")
        tree.add("actuator/bedroom/fan")
        
        val allTopics = tree.findMatchingTopicNames("#")
        assertEquals(6, allTopics.size)
        assertTrue(allTopics.contains("sensor/kitchen/temperature"))
        
        val sensorTopics = tree.findMatchingTopicNames("sensor/#")
        assertEquals(4, sensorTopics.size)
        assertTrue(sensorTopics.all { it.startsWith("sensor/") })
        
        val kitchenSensors = tree.findMatchingTopicNames("sensor/kitchen/+")
        assertEquals(2, kitchenSensors.size)
        assertTrue(kitchenSensors.contains("sensor/kitchen/temperature"))
        assertTrue(kitchenSensors.contains("sensor/kitchen/humidity"))
        
        val allTemps = tree.findMatchingTopicNames("sensor/+/temperature")
        assertEquals(3, allTemps.size)
        assertTrue(allTemps.contains("sensor/kitchen/temperature"))
        assertTrue(allTemps.contains("sensor/living/temperature"))
        assertTrue(allTemps.contains("sensor/bedroom/temperature"))
        
        val exact = tree.findMatchingTopicNames("actuator/kitchen/light")
        assertEquals(1, exact.size)
        assertEquals("actuator/kitchen/light", exact[0])
        
        val collected = mutableListOf<String>()
        tree.findMatchingTopicNames("sensor/+/temperature") { topic ->
            collected.add(topic)
            true
        }
        assertEquals(3, collected.size)
        
        val earlyExit = mutableListOf<String>()
        tree.findMatchingTopicNames("sensor/#") { topic ->
            earlyExit.add(topic)
            earlyExit.size < 2
        }
        assertEquals(2, earlyExit.size)
    }

    @Test
    fun testFindBrowseTopics() {
        val tree = TopicTree<String, String>()
        
        tree.add("sensor/kitchen/temperature")
        tree.add("sensor/kitchen/humidity")
        tree.add("sensor/kitchen/pressure/bar")
        tree.add("sensor/living/temperature")
        tree.add("sensor/bedroom/temperature")
        tree.add("actuator/kitchen/light")
        
        val browseSensorWildcard = mutableListOf<String>()
        tree.findBrowseTopics("sensor/+") { topic ->
            browseSensorWildcard.add(topic)
            true
        }
        assertEquals(3, browseSensorWildcard.size)
        assertTrue(browseSensorWildcard.contains("sensor/kitchen"))
        assertTrue(browseSensorWildcard.contains("sensor/living"))
        assertTrue(browseSensorWildcard.contains("sensor/bedroom"))
        
        val browseKitchenSensors = mutableListOf<String>()
        tree.findBrowseTopics("sensor/kitchen/+") { topic ->
            browseKitchenSensors.add(topic)
            true
        }
        assertEquals(3, browseKitchenSensors.size)
        assertTrue(browseKitchenSensors.contains("sensor/kitchen/temperature"))
        assertTrue(browseKitchenSensors.contains("sensor/kitchen/humidity"))
        assertTrue(browseKitchenSensors.contains("sensor/kitchen/pressure"))
        
        val browseAllSensors = mutableListOf<String>()
        tree.findBrowseTopics("sensor/#") { topic ->
            browseAllSensors.add(topic)
            true
        }
        assertEquals(10, browseAllSensors.size)
        assertTrue(browseAllSensors.contains("sensor/kitchen"))
        assertTrue(browseAllSensors.contains("sensor/kitchen/temperature"))
        assertTrue(browseAllSensors.contains("sensor/kitchen/humidity"))
        assertTrue(browseAllSensors.contains("sensor/kitchen/pressure"))
        assertTrue(browseAllSensors.contains("sensor/kitchen/pressure/bar"))
        assertTrue(browseAllSensors.contains("sensor/living"))
        assertTrue(browseAllSensors.contains("sensor/living/temperature"))
        assertTrue(browseAllSensors.contains("sensor/bedroom"))
        assertTrue(browseAllSensors.contains("sensor/bedroom/temperature"))
        
        val browseExact = mutableListOf<String>()
        tree.findBrowseTopics("sensor/kitchen/temperature") { topic ->
            browseExact.add(topic)
            true
        }
        assertEquals(1, browseExact.size)
        assertEquals("sensor/kitchen/temperature", browseExact[0])
        
        val earlyExit = mutableListOf<String>()
        tree.findBrowseTopics("sensor/#") { topic ->
            earlyExit.add(topic)
            earlyExit.size < 3
        }
        assertEquals(3, earlyExit.size)
    }

    @Test
    fun testAddDelOperations() {
        val tree = TopicTree<String, String>()
        
        tree.add("a/b/c", "key1", "value1")
        assertEquals(1, tree.findDataOfTopicName("a/b/c").size)
        
        tree.add("a/b/c", "key2", "value2")
        assertEquals(2, tree.findDataOfTopicName("a/b/c").size)
        
        tree.del("a/b/c", "key1")
        val afterDel = tree.findDataOfTopicName("a/b/c")
        assertEquals(1, afterDel.size)
        assertEquals("key2", afterDel[0].first)
        
        tree.del("a/b/c", "key2")
        assertEquals(0, tree.findDataOfTopicName("a/b/c").size)
        
        tree.add("x/y/z")
        assertTrue(tree.findMatchingTopicNames("x/y/z").isNotEmpty())
        tree.del("x/y/z")
        assertTrue(tree.findMatchingTopicNames("x/y/z").isEmpty())
        
        tree.addAll(listOf("topic1", "topic2", "topic3"))
        assertEquals(3, tree.findMatchingTopicNames("#").size)
        tree.delAll(listOf("topic1", "topic2", "topic3"))
        assertEquals(0, tree.findMatchingTopicNames("#").size)
    }

    @Test
    fun testTreeSizeAndDataset() {
        val tree = TopicTree<String, String>()
        
        tree.add("a/b/c", "k1", "v1")
        tree.add("a/b/c", "k2", "v2")
        tree.add("a/b/d", "k3", "v3")
        tree.add("x/y", "k4", "v4")
        
        assertEquals(4, tree.size())
        
        tree.del("a/b/c", "k1")
        assertEquals(3, tree.size())
        
        tree.del("a/b/c", "k2")
        assertEquals(2, tree.size())
        
        tree.del("a/b/d", "k3")
        tree.del("x/y", "k4")
        assertEquals(0, tree.size())
    }

    @Test
    fun testSysTopicProtectionInTree() {
        val tree = TopicTree<String, String>()
        
        tree.add("#", "global", "handler1")
        tree.add("+/broker/#", "wildcard", "handler2")
        tree.add("\$SYS/#", "sys", "handler3")
        tree.add("sensor/#", "sensor", "handler4")
        
        val sysData = tree.findDataOfTopicName("\$SYS/broker/clients")
        assertEquals(1, sysData.size)
        assertTrue(sysData.any { it.first == "sys" })
        assertFalse(sysData.any { it.first == "global" })
        assertFalse(sysData.any { it.first == "wildcard" })
        
        val normalData = tree.findDataOfTopicName("sensor/temp")
        assertTrue(normalData.any { it.first == "global" })
        assertTrue(normalData.any { it.first == "sensor" })
        
        assertTrue(tree.isTopicNameMatching("\$SYS/test"))
        
        tree.add("\$SYS/test")
        assertTrue(tree.isTopicNameMatching("\$SYS/test"))
    }
}
