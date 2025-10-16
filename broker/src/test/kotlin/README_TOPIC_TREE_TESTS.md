# Topic Tree Test Suite

This test suite provides comprehensive testing for the TopicTree MQTT topic matching implementation.

## Test Overview

### Basic Tests (Original)
- `testExactMatch` - Exact topic name matching
- `testSingleLevelWildcard` - Single-level wildcard `+` matching
- `testMultiLevelWildcard` - Multi-level wildcard `#` matching
- `testMixedWildcards` - Combined wildcard patterns
- `testEdgeCases` - Edge cases including empty strings and topics with empty levels

### Advanced Tests (New)
- `testSysTopicsProtection` - MQTT 3.1.1 spec compliance for `$SYS` topics protection
- `testLargeTopicTree` - Large-scale testing with 41,000+ topics and 18 comprehensive wildcard patterns
- `testTopicTreePerformance` - Performance benchmarks for topic tree operations
- `testWildcardEdgeCases` - Extended edge cases for wildcard behavior
- `testComplexISA88Patterns` - ISA-88 hierarchy pattern matching
- `testMalformedFilters` - Validation of malformed/invalid filter patterns
- `testEmptyAndSpecialTopics` - Edge cases with empty strings and special topic structures
- `testUnicodeTopics` - Unicode and emoji support in topic names

## Large Topic Tree Test

### Test Data Generation
The test generates **41,000 topics** following ISA-88 hierarchy:
- 36,000 ISA-88 topics: `enterprise/site/area/line/cell/unit/equipment`
- 5,000 `$SYS` topics for testing MQTT system topic protection

Configuration (can be modified in `ISA88Config`):
- 5 enterprises
- 4 sites per enterprise
- 5 areas per site
- 6 lines per area
- 4 cells per line
- 5 units per cell
- 3 equipment per unit

This configuration generates **100,000+ potential topics** when scaled up further.

### Test Filters
The large test runs 18 comprehensive filter patterns:
1. `#` - Global wildcard (matches all non-$SYS topics)
2. `enterprise1/#` - Specific enterprise
3. `enterprise1/site1/#` - Specific site
4. `enterprise1/+/area1/#` - Mixed wildcards
5. `+/site1/+/line1/#` - Multiple single-level wildcards
6. `+/+/+/+/+/+/equipment1` - Deep wildcard matching
7. `enterprise1/site1/area1/line1/cell1/unit1/+` - Specific path with wildcard
8. `enterprise+/+/+/+/+/+/+` - No matches (tests invalid patterns)
9. `$SYS/#` - System topics (validates $SYS protection works)
10. `$SYS/broker/+/+` - Specific system topic pattern
11. `+/broker/#` - Should NOT match $SYS topics
12. `enterprise1/site1/area1/+/+/unit1/equipment1` - Double wildcard in middle
13. `+/+/area2/line3/#` - Multi-wildcard prefix
14. `enterprise2/+/+/+/cell2/+/equipment2` - Scattered wildcards
15. `enterprise3/site3/area3/line3/cell3/unit3/equipment3` - Exact deep match
16. `$SYS/broker/load/+/+/+` - Triple wildcard suffix on $SYS
17. `enterprise+/site+/+/+/+/+/+` - Invalid pattern (+ as part of literal)
18. `+/+/+/line5/#` - Prefix wildcards with hash suffix

## Running Tests

### Run all tests
```bash
cd broker
mvn test -Dtest=TopicMatcherTest
```

### Generate reference output
First time setup or to update reference files:
```bash
mvn test -Dtest=TopicMatcherTest#testLargeTopicTree -DgenerateReference=true
```

### Run specific test
```bash
mvn test -Dtest=TopicMatcherTest#testSysTopicsProtection
```

## Test Modes

### Comparison Mode (Default)
- Loads existing topics from `broker/src/test/resources/topic-tree-test-data.txt`
- Runs filter tests and writes results to `broker/src/test/resources/test-output/`
- Compares results against reference files in `broker/src/test/resources/reference-output/`
- Fails if results differ from reference

### Generate Reference Mode
Activate with `-DgenerateReference=true`:
- Generates or loads test topics
- Runs filter tests and writes results to `broker/src/test/resources/reference-output/`
- Does NOT perform comparison
- Use this to create initial reference files or update them after intentional changes

## Output Files

Each filter test creates a file named `filter_N_<description>.txt` containing:
```
Filter: <filter pattern>
Total topics: <count>
Matches: <match count>
---
<matched topic 1>
<matched topic 2>
...
```

## Performance Metrics

The `testTopicTreePerformance` test provides benchmarks for:
- **Topic insertion**: Measures time to add 41,000+ topics to tree
- **Exact match lookup**: Specific topic matching
- **Mixed wildcard matching**: Performance with combined wildcards
- **Multi-level wildcard**: `#` pattern performance
- **Global wildcard**: Full tree traversal performance

Example output (41,000 topics):
```
Added 41000 topics to tree in 27ms (1518518 topics/sec)
Exact match 'enterprise1/site1/...': Found 41000 matches in 56ms
Mixed wildcards 'enterprise1/+/area1/+/...': Found 41000 matches in 38ms
Multi-level wildcard 'enterprise1/site1/#': Found 41000 matches in 40ms
Global wildcard '#': Found 41000 matches in 41ms
```

## MQTT 3.1.1 Compliance

The test suite validates compliance with MQTT 3.1.1 specification:

### $SYS Topic Protection
According to MQTT spec, wildcard subscriptions (`#` and `+`) **MUST NOT** match topics starting with `$` unless the subscription filter explicitly starts with `$`.

Examples:
- ✗ `#` does NOT match `$SYS/broker/clients`
- ✗ `+/broker/clients` does NOT match `$SYS/broker/clients`
- ✓ `$SYS/#` DOES match `$SYS/broker/clients`
- ✓ `$SYS/+/clients` DOES match `$SYS/broker/clients`

## Customization

### Modify ISA-88 Hierarchy
Edit the `ISA88Config` data class in `TopicMatcherTest.kt`:
```kotlin
data class ISA88Config(
    val enterprises: Int = 5,  // Current: 41K topics
    val sites: Int = 4,         // Scale to 10/10/10/10/10/10/5 for ~500K topics
    val areas: Int = 5,
    val lines: Int = 6,
    val cells: Int = 4,
    val units: Int = 5,
    val equipment: Int = 3
)
```

### Add Custom Test Filters
Add to the `testFilters` list in `testLargeTopicTree`:
```kotlin
val testFilters = listOf(
    // ... existing filters
    "custom/+/pattern/#",
    "another/test/filter"
)
```

### Regenerate Reference Files
After modifying test data or filters:
```bash
mvn test -Dtest=TopicMatcherTest#testLargeTopicTree -DgenerateReference=true
```

## File Structure

```
broker/src/test/
├── kotlin/
│   └── TopicMatcherTest.kt           # Test suite
└── resources/
    ├── topic-tree-test-data.txt      # Generated/cached topic list
    ├── reference-output/              # Reference match results
    │   ├── filter_1_hash.txt
    │   ├── filter_2_enterprise1_hash.txt
    │   └── ...
    └── test-output/                   # Current test results
        ├── filter_1_hash.txt
        ├── filter_2_enterprise1_hash.txt
        └── ...
```

## Troubleshooting

### "No tests were executed" error
Make sure JUnit dependency is in `pom.xml` and `testSourceDirectory` is configured:
```xml
<testSourceDirectory>src/test/kotlin</testSourceDirectory>
```

### Reference file mismatch
If tests fail with "results differ from reference":
1. Check if the change is intentional
2. If intentional, regenerate references: `mvn test -DgenerateReference=true`
3. If not intentional, investigate why matching behavior changed

### Performance degradation
If performance tests show significant slowdown:
1. Check if topic count increased
2. Verify no debug logging is enabled
3. Consider optimizing TopicTree implementation
