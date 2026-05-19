package dev.autofastdl.android;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class DownloadStats {
    private final AtomicLong totalFiles = new AtomicLong();
    private final AtomicLong failedDownloads = new AtomicLong();
    private final AtomicLong totalBytes = new AtomicLong();
    private final AtomicReference<String> lastError = new AtomicReference<>("");
    private volatile long startNanos = System.nanoTime();

    public void reset() {
        totalFiles.set(0);
        failedDownloads.set(0);
        totalBytes.set(0);
        lastError.set("");
        startNanos = System.nanoTime();
    }

    public void addBytes(long bytes) {
        if (bytes > 0) {
            totalBytes.addAndGet(bytes);
        }
    }

    public void addFile() {
        totalFiles.incrementAndGet();
    }

    public void recordFailure(Throwable error) {
        failedDownloads.incrementAndGet();
        lastError.set(error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage());
    }

    public long totalFiles() {
        return totalFiles.get();
    }

    public long failedDownloads() {
        return failedDownloads.get();
    }

    public long totalBytes() {
        return totalBytes.get();
    }

    public String lastError() {
        return lastError.get();
    }

    public double elapsedSeconds() {
        long elapsed = System.nanoTime() - startNanos;
        return Math.max(elapsed / 1_000_000_000.0, 0.000001);
    }

    public long elapsedMillis() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    public double mibPerSecond() {
        return totalBytes() / 1024.0 / 1024.0 / elapsedSeconds();
    }

    public double mbps() {
        return bytesToMbps(totalBytes(), elapsedSeconds());
    }

    public double gb() {
        return totalBytes() / 1_000_000_000.0;
    }

    public double gbPerHour() {
        return totalBytes() / elapsedSeconds() * 3600.0 / 1_000_000_000.0;
    }

    public double filesPerSecond() {
        return totalFiles() / elapsedSeconds();
    }

    public static double bytesToMbps(long bytes, double elapsedSeconds) {
        if (bytes <= 0 || elapsedSeconds <= 0) {
            return 0.0;
        }
        return bytes * 8.0 / elapsedSeconds / 1_000_000.0;
    }

    public static double gbPerHourFromMbps(double mbps) {
        if (mbps <= 0) {
            return 0.0;
        }
        return mbps * 1_000_000.0 / 8.0 * 3600.0 / 1_000_000_000.0;
    }
}
