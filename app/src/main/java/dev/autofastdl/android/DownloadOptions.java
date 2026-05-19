package dev.autofastdl.android;

import java.io.File;
import java.util.Locale;

public final class DownloadOptions {
    public static final int MAX_CONCURRENCY = 512;
    public static final int MAX_ACTIVE_WORKERS = 64;
    public static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

    public final int concurrency;
    public final int repeat;
    public final SinkMode sinkMode;
    public final File outputDir;
    public final int chunkSize;
    public final int timeoutMillis;
    public final boolean cleanup;
    public final int maxFailures;

    public DownloadOptions(
            int concurrency,
            int repeat,
            SinkMode sinkMode,
            File outputDir,
            int chunkSize,
            int timeoutMillis,
            boolean cleanup,
            int maxFailures) {
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be >= 1");
        }
        if (concurrency > MAX_CONCURRENCY) {
            throw new IllegalArgumentException("concurrency must be <= " + MAX_CONCURRENCY);
        }
        if (repeat < 0) {
            throw new IllegalArgumentException("repeat must be >= 0; use 0 for infinite");
        }
        if (chunkSize < 1024) {
            throw new IllegalArgumentException("chunk size must be >= 1024 bytes");
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
        if (maxFailures < 0) {
            throw new IllegalArgumentException("max failures must be >= 0");
        }
        if (sinkMode == null) {
            throw new IllegalArgumentException("sink must be disk or null");
        }
        if (repeat == 0 && sinkMode == SinkMode.DISK) {
            throw new IllegalArgumentException("repeat=0 is only supported with sink null");
        }
        if (outputDir == null) {
            throw new IllegalArgumentException("output directory is required");
        }

        this.concurrency = concurrency;
        this.repeat = repeat;
        this.sinkMode = sinkMode;
        this.outputDir = outputDir;
        this.chunkSize = chunkSize;
        this.timeoutMillis = timeoutMillis;
        this.cleanup = cleanup;
        this.maxFailures = maxFailures;
    }

    public static int defaultConcurrency() {
        int cpus = Runtime.getRuntime().availableProcessors();
        int value = cpus * 4;
        if (value < 8) {
            value = 8;
        }
        return Math.min(value, MAX_ACTIVE_WORKERS);
    }

    public int activeWorkerCount() {
        int value = Math.min(concurrency, defaultConcurrency());
        if (repeat > 0) {
            value = Math.min(value, repeat);
        }
        return value;
    }

    public static SinkMode parseSink(String value) {
        if (value == null) {
            return SinkMode.NULL;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if ("disk".equals(normalized)) {
            return SinkMode.DISK;
        }
        if ("null".equals(normalized)) {
            return SinkMode.NULL;
        }
        throw new IllegalArgumentException("sink must be disk or null");
    }
}
