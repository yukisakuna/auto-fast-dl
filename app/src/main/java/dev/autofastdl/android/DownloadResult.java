package dev.autofastdl.android;

public final class DownloadResult {
    public final DownloadStats stats;
    public final boolean interrupted;

    public DownloadResult(DownloadStats stats, boolean interrupted) {
        this.stats = stats;
        this.interrupted = interrupted;
    }
}
