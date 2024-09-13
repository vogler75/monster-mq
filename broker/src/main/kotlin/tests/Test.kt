package at.rocworks.tests

fun main(args: Array<String>) {
    println("Hello, World!")
    val xs : ArrayDeque<String> = ArrayDeque()

    xs.add("a")
    xs.add("b")
    xs.add("c")

    println(xs.first())
    println(xs.size)

    println(xs.last())
    println(xs.size)

    println(xs.removeFirst())
    println(xs.size)
    println(xs.first())


}