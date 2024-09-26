package at.rocworks.tests

fun main(args: Array<String>) {
    println("Hello, World!")

    fun test(qosLevel: Int, isDup: Boolean, isRetain: Boolean, isQueued: Boolean) {

        val status =
            (((qosLevel and 0x03) shl 3) or ((if (isDup) 1 else 0) shl 2) or ((if (isRetain) 1 else 0) shl 1) or (if (isQueued) 1 else 0)).toByte()

        // print status as bitmuster like 1010101010
        val bitmuster = status.toString(2).padStart(10, '0')


        val qos1 = (status.toInt() shr 3) and 0x03
        val isDup1 = ((status.toInt() shr 2) and 0x01) == 1
        val isRetain1 = ((status.toInt() shr 1) and 0x01) == 1
        val isQueued1 = (status.toInt() and 0x01) == 1

        val ok = (qos1 != qosLevel || isDup1 != isDup || isRetain1 != isRetain || isQueued1 != isQueued)
        println("$bitmuster qosLevel: $qosLevel/$qos1, isDup: $isDup/$isDup1, isRetain: $isRetain/$isRetain1, isQueued: $isQueued/$isQueued1")

    }

    // call test with all possible values (qosLevel from 0 to 2)
    for (qosLevel in 0..3) {
        test(qosLevel, false, false, false)
        test(qosLevel, false, false, true)
        test(qosLevel, false, true, false)
        test(qosLevel, false, true, true)
        test(qosLevel, true, false, false)
        test(qosLevel, true, false, true)
        test(qosLevel, true, true, false)
        test(qosLevel, true, true, true)
    }


}