package at.rocworks.peerlink

import java.io.File

// The Go edge broker used by the mixed tests: -Dpeerlink.edgeBin, or built from the edge checkout
// next to this one into target/ when Go is installed. Null when neither works.
object EdgeBinary {
    private val built: File? by lazy { build() }

    fun get(): File? {
        System.getProperty("peerlink.edgeBin")?.let { return File(it).takeIf { f -> f.canExecute() } }
        return built
    }

    private fun build(): File? {
        val src = listOf("../../edge", "../edge").map { File(it) }
            .firstOrNull { File(it, "cmd/monstermq-edge").isDirectory } ?: return null
        val out = File("target/monstermq-edge-it").absoluteFile
        val proc = runCatching {
            ProcessBuilder("go", "build", "-o", out.path, "./cmd/monstermq-edge")
                .directory(src).redirectErrorStream(true).start()
        }.getOrNull() ?: return null
        val log = proc.inputStream.bufferedReader().readText()
        return if (proc.waitFor() == 0 && out.canExecute()) out else {
            System.err.println("edge build failed: $log"); null
        }
    }
}
