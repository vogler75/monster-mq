package at.rocworks.peerlink

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import org.junit.Assert.assertEquals
import org.junit.Test

// Epochs are uint64 like on the edge broker: an epoch with the top bit set is not written negative.
class StatusJsonTest {

    data class Epoch(@get:JsonSerialize(using = UnsignedLongSerializer::class) val epoch: Long)

    @Test
    fun testEpochWrittenUnsigned() {
        val m = ObjectMapper()
        assertEquals("""{"epoch":18446744073709551615}""", m.writeValueAsString(Epoch(-1L)))
        assertEquals("""{"epoch":42}""", m.writeValueAsString(Epoch(42L)))
        assertEquals("18446744073709551615", m.readTree(m.writeValueAsString(Epoch(-1L)))["epoch"].bigIntegerValue().toString())
    }
}
