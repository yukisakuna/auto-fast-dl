package dev.autofastdl.android;

public final class DownloadServiceStatus {
    public final boolean running;
    public final String state;
    public final DownloadStats stats;
    public final String errorMessage;
    public final int requestedConcurrency;
    public final int activeWorkerCount;
    public final double currentMbps;

    public DownloadServiceStatus(
            boolean running,
            String state,
            DownloadStats stats,
            String errorMessage,
            int requestedConcurrency,
            int activeWorkerCount,
            double currentMbps) {
        this.running = running;
        this.state = state;
        this.stats = stats;
        this.errorMessage = errorMessage == null ? "" : errorMessage;
        this.requestedConcurrency = requestedConcurrency;
        this.activeWorkerCount = activeWorkerCount;
        this.currentMbps = currentMbps;
    }

    public static DownloadServiceStatus idle() {
        DownloadStats stats = new DownloadStats();
        stats.reset();
        return new DownloadServiceStatus(false, "idle", stats, "", 0, 0, 0);
    }
}
