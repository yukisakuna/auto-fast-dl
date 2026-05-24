package dev.autofastdl.android;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public final class FastDownloaderTest {
    private static final byte[] PAYLOAD = repeat("0123456789abcdef", 16384).getBytes(StandardCharsets.UTF_8);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void nullSinkDownloadsAllRepeats() throws Exception {
        try (LocalServer server = LocalServer.payload()) {
            FastDownloader downloader = new FastDownloader(options(8, 24, SinkMode.NULL, temporaryFolder.newFolder()));

            DownloadResult result = downloader.run(server.url());

            assertFalse(result.interrupted);
            assertEquals(24, result.stats.totalFiles());
            assertEquals(0, result.stats.failedDownloads());
            assertEquals(PAYLOAD.length * 24L, result.stats.totalBytes());
        }
    }

    @Test
    public void zeroByteDownloadsCountAsSuccess() throws Exception {
        try (LocalServer server = LocalServer.empty()) {
            FastDownloader downloader = new FastDownloader(options(4, 6, SinkMode.NULL, temporaryFolder.newFolder()));

            DownloadResult result = downloader.run(server.url());

            assertEquals(6, result.stats.totalFiles());
            assertEquals(0, result.stats.failedDownloads());
            assertEquals(0, result.stats.totalBytes());
        }
    }

    @Test
    public void diskSinkWritesAndCleansOnlyCurrentRunFiles() throws Exception {
        File outputDir = temporaryFolder.newFolder();
        File unrelated = new File(outputDir, "keep-me.bin");
        Files.write(unrelated.toPath(), "do not delete".getBytes(StandardCharsets.UTF_8));

        try (LocalServer server = LocalServer.payload()) {
            FastDownloader downloader = new FastDownloader(options(4, 5, SinkMode.DISK, outputDir));

            DownloadResult result = downloader.run(server.url());

            assertEquals(5, result.stats.totalFiles());
            assertEquals(0, result.stats.failedDownloads());
            assertEquals(PAYLOAD.length * 5L, result.stats.totalBytes());

            File[] files = outputDir.listFiles((dir, name) -> name.startsWith(downloader.runId() + "-") && name.endsWith(".bin"));
            assertEquals(5, files == null ? 0 : files.length);
            for (File file : files) {
                assertArrayEquals(PAYLOAD, Files.readAllBytes(file.toPath()));
            }

            downloader.cleanupFiles();
            assertEquals("do not delete", new String(Files.readAllBytes(unrelated.toPath()), StandardCharsets.UTF_8));
            File[] remaining = outputDir.listFiles((dir, name) -> name.startsWith(downloader.runId() + "-"));
            assertEquals(0, remaining == null ? 0 : remaining.length);
        }
    }

    @Test
    public void unboundedDiskModeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new DownloadOptions(
                4,
                0,
                SinkMode.DISK,
                temporaryFolder.getRoot(),
                8192,
                5000,
                false,
                0));
    }

    @Test
    public void invalidOutputDirectoryIsReported() throws Exception {
        File fileInsteadOfDir = temporaryFolder.newFile("not-a-directory");
        try (LocalServer server = LocalServer.payload()) {
            FastDownloader downloader = new FastDownloader(options(2, 1, SinkMode.DISK, fileInsteadOfDir));

            IOException error = assertThrows(IOException.class, () -> downloader.run(server.url()));

            assertTrue(error.getMessage().contains("output directory is not a directory"));
        }
    }

    @Test
    public void maxFailuresStopsRetries() throws Exception {
        try (LocalServer server = LocalServer.error()) {
            DownloadOptions options = new DownloadOptions(1, 10, SinkMode.NULL, temporaryFolder.newFolder(), 8192, 5000, false, 1);
            FastDownloader downloader = new FastDownloader(options);

            DownloadResult result = downloader.run(server.url());

            assertEquals(0, result.stats.totalFiles());
            assertEquals(1, result.stats.failedDownloads());
            assertTrue(result.stats.lastError().contains("HTTP 500"));
        }
    }

    @Test
    public void invalidUrlIsRejected() throws Exception {
        FastDownloader downloader = new FastDownloader(options(1, 1, SinkMode.NULL, temporaryFolder.newFolder()));

        assertThrows(IOException.class, () -> downloader.run("ftp://example.com/file.bin"));
    }

    @Test
    public void activeWorkersAreCappedBelowRequestedConcurrency() {
        DownloadOptions options = new DownloadOptions(
                DownloadOptions.MAX_CONCURRENCY,
                0,
                SinkMode.NULL,
                temporaryFolder.getRoot(),
                8192,
                5000,
                false,
                0);

        assertTrue(options.activeWorkerCount() <= DownloadOptions.MAX_ACTIVE_WORKERS);
        assertTrue(options.activeWorkerCount() < options.concurrency);
    }

    @Test
    public void activeWorkersDoNotExceedFiniteRepeat() {
        DownloadOptions options = new DownloadOptions(
                DownloadOptions.MAX_CONCURRENCY,
                3,
                SinkMode.NULL,
                temporaryFolder.getRoot(),
                DownloadOptions.DEFAULT_CHUNK_SIZE,
                5000,
                false,
                0);

        assertEquals(3, options.activeWorkerCount());
    }

    @Test
    public void buildProfileControlsDefaults() {
        if (BuildConfig.PERFORMANCE_BUILD) {
            assertEquals(2048, DownloadOptions.MAX_CONCURRENCY);
            assertEquals(256, DownloadOptions.MAX_ACTIVE_WORKERS);
            assertEquals(1024 * 1024, DownloadOptions.DEFAULT_CHUNK_SIZE);
            assertEquals("0.1.0-performance", FastDownloader.VERSION);
        } else {
            assertEquals(512, DownloadOptions.MAX_CONCURRENCY);
            assertEquals(64, DownloadOptions.MAX_ACTIVE_WORKERS);
            assertEquals(64 * 1024, DownloadOptions.DEFAULT_CHUNK_SIZE);
            assertEquals("0.1.0", FastDownloader.VERSION);
        }
    }

    private DownloadOptions options(int concurrency, int repeat, SinkMode sinkMode, File outputDir) {
        return new DownloadOptions(concurrency, repeat, sinkMode, outputDir, 8192, 5000, false, 0);
    }

    private static String repeat(String value, int count) {
        StringBuilder builder = new StringBuilder(value.length() * count);
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }

    private static final class LocalServer implements AutoCloseable {
        private final int status;
        private final byte[] body;
        private final ServerSocket serverSocket;
        private final ExecutorService executor = Executors.newCachedThreadPool();
        private final AtomicBoolean running = new AtomicBoolean(true);

        private LocalServer(int status, byte[] body) throws IOException {
            this.status = status;
            this.body = body;
            this.serverSocket = new ServerSocket(0);
            executor.execute(this::acceptLoop);
        }

        static LocalServer payload() throws IOException {
            return new LocalServer(200, PAYLOAD);
        }

        static LocalServer empty() throws IOException {
            return new LocalServer(200, new byte[0]);
        }

        static LocalServer error() throws IOException {
            return new LocalServer(500, new byte[0]);
        }

        String url() {
            return "http://127.0.0.1:" + serverSocket.getLocalPort() + "/payload.bin";
        }

        @Override
        public void close() {
            running.set(false);
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
            executor.shutdownNow();
        }

        private void acceptLoop() {
            while (running.get()) {
                try {
                    Socket socket = serverSocket.accept();
                    executor.execute(() -> handle(socket));
                } catch (IOException error) {
                    if (running.get()) {
                        throw new RuntimeException(error);
                    }
                }
            }
        }

        private void handle(Socket socket) {
            try (Socket ignored = socket) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
                String line;
                while ((line = reader.readLine()) != null && !line.isEmpty()) {
                    // Consume request headers.
                }
                OutputStream output = socket.getOutputStream();
                String reason = status == 200 ? "OK" : "Internal Server Error";
                String headers = "HTTP/1.1 " + status + " " + reason + "\r\n"
                        + "Content-Type: application/octet-stream\r\n"
                        + "Content-Length: " + body.length + "\r\n"
                        + "Connection: close\r\n"
                        + "\r\n";
                output.write(headers.getBytes(StandardCharsets.US_ASCII));
                output.write(body);
                output.flush();
            } catch (IOException ignored) {
            }
        }
    }
}
