// WinCC Unified Open Pipe — minimal standalone test program.
//
// Strips away Vert.X and the broker entirely. Confirms whether the round-trip with the
// local WinCC Unified Runtime over the named pipe works at the JDK level.
//
// Requirements:
//   - WinCC Unified Runtime running on this host
//   - The user running this command is a member of group "SIMATIC HMI" (Windows) or
//     "industrial" (Linux)
//   - JDK 11+ for single-file source launch (no compile step)
//
// Run from the repo root:
//   java dev\OpenPipeTest.java                 (default filter "*", probe first)
//   java dev\OpenPipeTest.java "HMI_Tag_*"     (custom filter)
//   java dev\OpenPipeTest.java skip "HMI_Tag_*"  (skip probe, go straight to BrowseTags)
//
// The program:
//   1. Opens \\.\pipe\HmiRuntime (Windows) or /tmp/HmiRuntime (Linux)
//   2. Starts a reader thread that prints every line received (with a timestamp)
//   3. Waits for the reader thread to signal it is running (CountDownLatch)
//   4. Waits an additional 3 s (READER_READY_WAIT_MS + PRE_SEND_DELAY_MS) for RT to attach
//      NOTE: if "TX flushed" never appears, RT isn't reading — restart WinCC RT and retry.
//      Windows named pipes are full-duplex: reads and writes are independent kernel operations,
//      so the reader thread and the main-thread writes run in parallel safely.
//   5. Sends a tiny `ReadConfig DefaultPageSize` probe first — simplest command, fast round-trip.
//      Waits up to 5 s for a response before proceeding.
//   6. Sends one BrowseTags request (expert/JSON syntax)
//   7. Sends Next requests in a loop until the server returns an empty page
//   8. Keeps running until Ctrl+C, printing a heartbeat every 5s.

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class OpenPipeTest {

    private static final String COOKIE = "openpipe-test-1";
    // Windows named pipes are full-duplex: the kernel keeps separate read/write buffers, so
    // ReadFile and WriteFile on the same handle can run concurrently from different threads.
    // RandomAccessFile("rw") opens with GENERIC_READ|GENERIC_WRITE; wrapping its FD with both
    // FileInputStream and FileOutputStream is safe — each issues an independent syscall.
    private static final long READER_READY_WAIT_MS = 2_000L;  // wait after reader thread signals ready
    private static final long PRE_SEND_DELAY_MS = 1_000L;     // extra gap before first send
    private static final long PROBE_WAIT_MS = 5_000L;         // wait this long for probe response
    private static final long INTER_STEP_DELAY_MS = 500L;     // small gap between commands
    private static BufferedWriter sharedWriter;               // synchronized below

    public static void main(String[] args) throws Exception {
        boolean skipProbe = false;
        String filter = "*";
        if (args.length > 0 && "skip".equalsIgnoreCase(args[0])) {
            skipProbe = true;
            if (args.length > 1) filter = args[1];
        } else if (args.length > 0) {
            filter = args[0];
        }

        String pipePath = System.getProperty("os.name", "").toLowerCase().contains("win")
            ? "\\\\.\\pipe\\HmiRuntime"
            : "/tmp/HmiRuntime";

        log("Pipe path:  " + pipePath);
        log("Filter:     " + filter);
        log("Skip probe: " + skipProbe);

        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(pipePath, "rw");
        } catch (FileNotFoundException e) {
            log("Cannot open pipe: " + e.getMessage());
            log("Is WinCC Unified Runtime running and is the current user in the SIMATIC HMI group?");
            System.exit(1);
            return;
        }
        log("Pipe opened");

        BufferedReader reader = new BufferedReader(
            new InputStreamReader(new FileInputStream(raf.getFD()), StandardCharsets.UTF_8));
        sharedWriter = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(raf.getFD()), StandardCharsets.UTF_8));

        AtomicLong lastRx = new AtomicLong(System.currentTimeMillis());
        AtomicLong rxCount = new AtomicLong();
        CountDownLatch readerReady = new CountDownLatch(1);

        Thread readerThread = new Thread(() -> {
            log("Reader thread running — signalling ready");
            readerReady.countDown();   // signal BEFORE blocking on readLine
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    lastRx.set(System.currentTimeMillis());
                    long n = rxCount.incrementAndGet();
                    log("RX #" + n + ": " + line);
                    if (line.contains("\"Message\":\"NotifyBrowseTags\"")) {
                        boolean emptyPage = line.contains("\"Tags\":[]") || !line.contains("\"Tags\":[");
                        if (emptyPage) {
                            log("BrowseTags complete (empty/no-Tags page)");
                        } else {
                            sendNext();
                        }
                    }
                }
                log("Reader: EOF");
            } catch (IOException e) {
                log("Reader IOException: " + e.getMessage());
            }
        }, "openpipe-test-reader");
        readerThread.setDaemon(true);
        readerThread.start();

        log("Waiting for reader thread ready signal...");
        readerReady.await();
        log("Reader thread ready. Waiting " + READER_READY_WAIT_MS + "ms before first send...");
        Thread.sleep(READER_READY_WAIT_MS);
        log("Pre-send delay done. Waiting additional " + PRE_SEND_DELAY_MS + "ms...");
        Thread.sleep(PRE_SEND_DELAY_MS);

        if (!skipProbe) {
            // Probe: read one config value. This is the simplest command in the protocol.
            // If we don't see "TX flushed" within a few seconds, the server isn't reading
            // our bytes at all — that's an RT-side state issue, restart WinCC RT.
            String probe = "{\"Message\":\"ReadConfig\","
                         + "\"Params\":[\"DefaultPageSize\"],"
                         + "\"ClientCookie\":\"openpipe-test-probe\"}";
            sendLine("PROBE", probe);
            log("Waiting up to " + PROBE_WAIT_MS + "ms for probe response...");
            long deadline = System.currentTimeMillis() + PROBE_WAIT_MS;
            long initialRxCount = rxCount.get();
            while (rxCount.get() == initialRxCount && System.currentTimeMillis() < deadline) {
                Thread.sleep(100);
            }
            if (rxCount.get() == initialRxCount) {
                log("WARNING: no response to probe after " + PROBE_WAIT_MS + "ms");
                log("This usually means WinCC Unified Runtime isn't reading the pipe.");
                log("Try restarting WinCC Unified Runtime and re-running this test.");
            } else {
                log("Probe round-trip OK");
            }
            Thread.sleep(INTER_STEP_DELAY_MS);
        }

        String browseRequest = "{\"Message\":\"BrowseTags\","
                             + "\"Params\":{\"Filter\":\"" + escape(filter) + "\"},"
                             + "\"ClientCookie\":\"" + COOKIE + "\"}";
        sendLine("BROWSE", browseRequest);

        // Heartbeat loop — keep running until Ctrl+C.
        long lastHeartbeat = System.currentTimeMillis();
        while (true) {
            Thread.sleep(1_000);
            long now = System.currentTimeMillis();
            if (now - lastHeartbeat >= 5_000) {
                long sinceRx = now - lastRx.get();
                log("heartbeat — RX count=" + rxCount.get() + ", " + (sinceRx / 1000) + "s since last RX");
                lastHeartbeat = now;
            }
        }
    }

    private static void sendNext() {
        String next = "{\"Message\":\"BrowseTags\",\"Params\":\"Next\",\"ClientCookie\":\"" + COOKIE + "\"}";
        sendLine("NEXT", next);
    }

    private static void sendLine(String label, String line) {
        try {
            log(label + " TX: " + line);
            synchronized (sharedWriter) {
                sharedWriter.write(line);
                sharedWriter.write("\n");
                sharedWriter.flush();
            }
            log(label + " TX flushed");
        } catch (IOException e) {
            log(label + " write failed: " + e.getMessage());
        }
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static void log(String msg) {
        System.out.println("[" + Instant.now() + "] " + msg);
    }
}
