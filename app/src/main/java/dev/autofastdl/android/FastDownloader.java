package dev.autofastdl.android;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class FastDownloader {
    public static final String VERSION = BuildConfig.PERFORMANCE_BUILD ? "0.1.0-performance" : "0.1.0";

    private final DownloadOptions options;
    private final DownloadStats stats = new DownloadStats();
    private final AtomicLong nextJob = new AtomicLong();
    private final AtomicBoolean stop = new AtomicBoolean();
    private final AtomicBoolean interrupted = new AtomicBoolean();
    private final Set<HttpURLConnection> activeConnections =
            Collections.newSetFromMap(new ConcurrentHashMap<HttpURLConnection, Boolean>());
    private final Set<File> createdFiles =
            Collections.newSetFromMap(new ConcurrentHashMap<File, Boolean>());

    private volatile ExecutorService executor;
    private volatile String runId = newRunId();

    public FastDownloader(DownloadOptions options) {
        this.options = options;
    }

    public DownloadResult run(String rawUrl) throws IOException, InterruptedException {
        URL url = validateUrl(rawUrl);
        stats.reset();
        nextJob.set(0);
        stop.set(false);
        interrupted.set(false);
        createdFiles.clear();
        runId = newRunId();

        if (options.sinkMode == SinkMode.DISK) {
            prepareOutputDir(options.outputDir);
        }

        int workerCount = activeWorkerCount();
        executor = Executors.newFixedThreadPool(workerCount, new DownloadThreadFactory());
        CountDownLatch done = new CountDownLatch(workerCount);
        for (int i = 0; i < workerCount; i++) {
            executor.execute(new Worker(url, done));
        }

        try {
            done.await();
        } catch (InterruptedException error) {
            cancel();
            Thread.currentThread().interrupt();
            throw error;
        } finally {
            ExecutorService localExecutor = executor;
            if (localExecutor != null) {
                localExecutor.shutdownNow();
            }
            if (options.cleanup) {
                cleanupFiles();
            }
        }

        return new DownloadResult(stats, interrupted.get());
    }

    public void cancel() {
        interrupted.set(true);
        stop.set(true);
        for (HttpURLConnection connection : activeConnections) {
            connection.disconnect();
        }
        ExecutorService localExecutor = executor;
        if (localExecutor != null) {
            localExecutor.shutdownNow();
        }
    }

    public DownloadStats stats() {
        return stats;
    }

    public String runId() {
        return runId;
    }

    public int requestedConcurrency() {
        return options.concurrency;
    }

    public int activeWorkerCount() {
        return options.activeWorkerCount();
    }

    public void cleanupFiles() {
        if (!createdFiles.isEmpty()) {
            for (File file : createdFiles) {
                if (file.isFile() && !file.delete()) {
                    stats.recordFailure(new IOException("cleanup failed for " + file.getAbsolutePath()));
                }
            }
            createdFiles.clear();
            return;
        }

        File[] files = options.outputDir.listFiles();
        if (files == null) {
            return;
        }
        String prefix = runId + "-";
        for (File file : files) {
            if (file.isFile() && file.getName().startsWith(prefix) && !file.delete()) {
                stats.recordFailure(new IOException("cleanup failed for " + file.getAbsolutePath()));
            }
        }
    }

    private final class Worker implements Runnable {
        private final URL url;
        private final CountDownLatch done;

        private Worker(URL url, CountDownLatch done) {
            this.url = url;
            this.done = done;
        }

        @Override
        public void run() {
            try {
                byte[] buffer = null;
                while (!stop.get() && !Thread.currentThread().isInterrupted()) {
                    long jobId = nextJob.incrementAndGet();
                    if (options.repeat > 0 && jobId > options.repeat) {
                        return;
                    }
                    if (buffer == null) {
                        buffer = new byte[options.chunkSize];
                    }
                    try {
                        downloadOne(url, jobId, buffer);
                    } catch (InterruptedIOException error) {
                        if (interrupted.get() || stop.get()) {
                            return;
                        }
                        recordFailure(error);
                    } catch (IOException | RuntimeException error) {
                        if (interrupted.get() || Thread.currentThread().isInterrupted()) {
                            return;
                        }
                        recordFailure(error);
                    } catch (OutOfMemoryError error) {
                        recordFailure(new IOException("device resource limit reached; lower concurrency"));
                        cancel();
                        return;
                    }
                }
            } finally {
                done.countDown();
            }
        }
    }

    private void downloadOne(URL url, long jobId, byte[] buffer) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        activeConnections.add(connection);
        File tempFile = null;
        try {
            connection.setConnectTimeout(options.timeoutMillis);
            connection.setReadTimeout(options.timeoutMillis);
            connection.setRequestMethod("GET");
            connection.setInstanceFollowRedirects(true);
            connection.setRequestProperty("Accept-Encoding", "identity");
            connection.setRequestProperty("User-Agent", "auto-fast-dl-android/" + VERSION);

            int status = connection.getResponseCode();
            if (status < HttpURLConnection.HTTP_OK || status >= HttpURLConnection.HTTP_MULT_CHOICE) {
                drain(connection.getErrorStream(), buffer);
                throw new IOException("GET " + url + " returned HTTP " + status);
            }

            InputStream input = connection.getInputStream();
            if (options.sinkMode == SinkMode.DISK) {
                File outputFile = new File(options.outputDir, outputFileName(url, jobId));
                tempFile = new File(outputFile.getAbsolutePath() + ".part");
                streamToDisk(input, tempFile, buffer);
                if (!tempFile.renameTo(outputFile)) {
                    throw new IOException("failed to move " + tempFile.getAbsolutePath() + " to " + outputFile.getAbsolutePath());
                }
                createdFiles.add(outputFile);
            } else {
                streamToNull(input, buffer);
            }
            stats.addFile();
        } finally {
            activeConnections.remove(connection);
            connection.disconnect();
            if (tempFile != null && tempFile.exists() && !tempFile.delete()) {
                tempFile.deleteOnExit();
            }
        }
    }

    private void streamToDisk(InputStream input, File tempFile, byte[] buffer) throws IOException {
        long pendingBytes = 0;
        long flushBytes = statFlushBytes(buffer.length);
        try (InputStream body = input; FileOutputStream output = new FileOutputStream(tempFile)) {
            int read;
            while ((read = body.read(buffer)) != -1) {
                if (Thread.currentThread().isInterrupted() || stop.get()) {
                    throw new InterruptedIOException("download interrupted");
                }
                output.write(buffer, 0, read);
                pendingBytes += read;
                if (pendingBytes >= flushBytes) {
                    stats.addBytes(pendingBytes);
                    pendingBytes = 0;
                }
            }
            stats.addBytes(pendingBytes);
            pendingBytes = 0;
        } catch (IOException | RuntimeException error) {
            stats.addBytes(pendingBytes);
            throw error;
        }
    }

    private void streamToNull(InputStream input, byte[] buffer) throws IOException {
        long pendingBytes = 0;
        long flushBytes = statFlushBytes(buffer.length);
        try (InputStream body = input) {
            int read;
            while ((read = body.read(buffer)) != -1) {
                if (Thread.currentThread().isInterrupted() || stop.get()) {
                    throw new InterruptedIOException("download interrupted");
                }
                pendingBytes += read;
                if (pendingBytes >= flushBytes) {
                    stats.addBytes(pendingBytes);
                    pendingBytes = 0;
                }
            }
            stats.addBytes(pendingBytes);
            pendingBytes = 0;
        } catch (IOException | RuntimeException error) {
            stats.addBytes(pendingBytes);
            throw error;
        }
    }

    private static long statFlushBytes(int bufferLength) {
        long flushBytes = (long) bufferLength * 4L;
        if (flushBytes < 8192L) {
            flushBytes = 8192L;
        }
        if (flushBytes > 1024L * 1024L) {
            flushBytes = 1024L * 1024L;
        }
        return flushBytes;
    }

    private void recordFailure(Throwable error) {
        stats.recordFailure(error);
        if (options.maxFailures > 0 && stats.failedDownloads() >= options.maxFailures) {
            stop.set(true);
        }
    }

    private String outputFileName(URL url, long jobId) {
        String path = url.getPath();
        String ext = ".bin";
        int dot = path == null ? -1 : path.lastIndexOf('.');
        int slash = path == null ? -1 : path.lastIndexOf('/');
        if (dot > slash && dot < path.length() - 1) {
            ext = path.substring(dot);
        }
        return String.format("%s-%08d%s", runId, jobId, ext);
    }

    private static URL validateUrl(String rawUrl) throws MalformedURLException {
        if (rawUrl == null || rawUrl.trim().isEmpty()) {
            throw new MalformedURLException("URL is required");
        }
        URL url = new URL(rawUrl.trim());
        String protocol = url.getProtocol();
        if (!"http".equals(protocol) && !"https".equals(protocol)) {
            throw new MalformedURLException("URL must start with http:// or https://");
        }
        if (url.getHost() == null || url.getHost().trim().isEmpty()) {
            throw new MalformedURLException("URL must include a host");
        }
        return url;
    }

    private static void prepareOutputDir(File outputDir) throws IOException {
        if (outputDir.exists() && !outputDir.isDirectory()) {
            throw new IOException("output directory is not a directory: " + outputDir.getAbsolutePath());
        }
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new IOException("failed to create output directory: " + outputDir.getAbsolutePath());
        }
    }

    private static void drain(InputStream input, byte[] buffer) {
        if (input == null) {
            return;
        }
        try (InputStream body = input) {
            while (body.read(buffer) != -1) {
                // Drain response bytes so the connection can close cleanly.
            }
        } catch (IOException ignored) {
            // The original HTTP status is the useful error for callers.
        }
    }

    private static String newRunId() {
        return Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime());
    }

    private static final class DownloadThreadFactory implements ThreadFactory {
        private final AtomicLong nextThread = new AtomicLong();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "auto-fast-dl-" + nextThread.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
