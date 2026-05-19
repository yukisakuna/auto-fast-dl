package dev.autofastdl.android;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.pm.ServiceInfo;
import android.os.Build;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class DownloadForegroundService extends Service {
    public static final String ACTION_START = "dev.autofastdl.android.action.START";
    public static final String ACTION_STOP = "dev.autofastdl.android.action.STOP";

    private static final String EXTRA_URL = "url";
    private static final String EXTRA_CONCURRENCY = "concurrency";
    private static final String EXTRA_REPEAT = "repeat";
    private static final String EXTRA_SINK = "sink";
    private static final String EXTRA_OUTPUT_DIR = "output_dir";
    private static final String EXTRA_CHUNK_SIZE = "chunk_size";
    private static final String EXTRA_TIMEOUT_MILLIS = "timeout_millis";
    private static final String EXTRA_CLEANUP = "cleanup";
    private static final String EXTRA_MAX_FAILURES = "max_failures";

    private static final int NOTIFICATION_ID = 1001;
    private static final String CHANNEL_ID = "downloads";
    private static final long STATUS_UPDATE_MILLIS = 1000L;

    private static final Object STATUS_LOCK = new Object();
    private static volatile FastDownloader currentDownloader;
    private static volatile DownloadStats lastStats;
    private static volatile String state = "idle";
    private static volatile String errorMessage = "";
    private static volatile int requestedConcurrency;
    private static volatile int activeWorkerCount;
    private static volatile double currentMbps;

    private final Handler handler = new Handler(Looper.getMainLooper());
    private ExecutorService runner;
    private long lastBytes;
    private long lastSampleMillis;

    private final Runnable notificationTick = new Runnable() {
        @Override
        public void run() {
            FastDownloader downloader = currentDownloader;
            if (downloader == null) {
                return;
            }
            sampleCurrentMbps(downloader.stats());
            updateNotification();
            handler.postDelayed(this, STATUS_UPDATE_MILLIS);
        }
    };

    public static Intent startIntent(Context context, String rawUrl, DownloadOptions options) {
        Intent intent = new Intent(context, DownloadForegroundService.class);
        intent.setAction(ACTION_START);
        intent.putExtra(EXTRA_URL, rawUrl);
        intent.putExtra(EXTRA_CONCURRENCY, options.concurrency);
        intent.putExtra(EXTRA_REPEAT, options.repeat);
        intent.putExtra(EXTRA_SINK, options.sinkMode == SinkMode.DISK ? "disk" : "null");
        intent.putExtra(EXTRA_OUTPUT_DIR, options.outputDir.getAbsolutePath());
        intent.putExtra(EXTRA_CHUNK_SIZE, options.chunkSize);
        intent.putExtra(EXTRA_TIMEOUT_MILLIS, options.timeoutMillis);
        intent.putExtra(EXTRA_CLEANUP, options.cleanup);
        intent.putExtra(EXTRA_MAX_FAILURES, options.maxFailures);
        return intent;
    }

    public static Intent stopIntent(Context context) {
        Intent intent = new Intent(context, DownloadForegroundService.class);
        intent.setAction(ACTION_STOP);
        return intent;
    }

    public static DownloadServiceStatus snapshot() {
        synchronized (STATUS_LOCK) {
            FastDownloader downloader = currentDownloader;
            DownloadStats stats = downloader == null ? lastStats : downloader.stats();
            if (stats == null) {
                return DownloadServiceStatus.idle();
            }
            return new DownloadServiceStatus(
                    downloader != null,
                    state,
                    stats,
                    errorMessage,
                    requestedConcurrency,
                    activeWorkerCount,
                    currentMbps);
        }
    }

    @Override
    public void onCreate() {
        super.onCreate();
        createNotificationChannel();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        String action = intent == null ? ACTION_STOP : intent.getAction();
        if (ACTION_STOP.equals(action)) {
            stopActiveDownload("stopping", "");
            return START_NOT_STICKY;
        }

        try {
            startDownload(intent);
        } catch (RuntimeException error) {
            setFinalStatus("failed", null, error.getMessage());
            stopForegroundCompat();
            stopSelf();
        }
        return START_NOT_STICKY;
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    @Override
    public void onDestroy() {
        stopActiveDownload("interrupted", "");
        ExecutorService localRunner = runner;
        if (localRunner != null) {
            localRunner.shutdownNow();
        }
        handler.removeCallbacks(notificationTick);
        super.onDestroy();
    }

    @Override
    public void onTaskRemoved(Intent rootIntent) {
        // Keep the foreground service alive when the UI task is removed.
    }

    @Override
    public void onTimeout(int startId, int fgsType) {
        stopActiveDownload("failed", "foreground service time limit reached");
        stopForegroundCompat();
        stopSelf();
    }

    private void startDownload(Intent intent) {
        stopActiveDownload("interrupted", "");

        String rawUrl = intent.getStringExtra(EXTRA_URL);
        DownloadOptions options = new DownloadOptions(
                intent.getIntExtra(EXTRA_CONCURRENCY, DownloadOptions.defaultConcurrency()),
                intent.getIntExtra(EXTRA_REPEAT, 1),
                DownloadOptions.parseSink(intent.getStringExtra(EXTRA_SINK)),
                new File(intent.getStringExtra(EXTRA_OUTPUT_DIR)),
                intent.getIntExtra(EXTRA_CHUNK_SIZE, DownloadOptions.DEFAULT_CHUNK_SIZE),
                intent.getIntExtra(EXTRA_TIMEOUT_MILLIS, 30_000),
                intent.getBooleanExtra(EXTRA_CLEANUP, false),
                intent.getIntExtra(EXTRA_MAX_FAILURES, 0));

        FastDownloader downloader = new FastDownloader(options);
        synchronized (STATUS_LOCK) {
            currentDownloader = downloader;
            lastStats = downloader.stats();
            state = "running";
            errorMessage = "";
            requestedConcurrency = downloader.requestedConcurrency();
            activeWorkerCount = downloader.activeWorkerCount();
            currentMbps = 0;
            lastBytes = 0;
            lastSampleMillis = System.currentTimeMillis();
        }

        startForegroundCompat(buildNotification("starting", downloader.stats()));
        handler.post(notificationTick);

        runner = Executors.newSingleThreadExecutor();
        runner.execute(() -> {
            try {
                DownloadResult result = downloader.run(rawUrl);
                finishDownload(downloader, result.interrupted ? "interrupted" : "complete", "");
            } catch (OutOfMemoryError error) {
                downloader.cancel();
                finishDownload(downloader, "failed", "device resource limit reached; lower concurrency");
            } catch (Exception error) {
                finishDownload(downloader, "failed", error.getMessage());
            }
        });
    }

    private void finishDownload(FastDownloader downloader, String finalState, String error) {
        synchronized (STATUS_LOCK) {
            if (currentDownloader != downloader) {
                return;
            }
            sampleCurrentMbps(downloader.stats());
            lastStats = downloader.stats();
            currentDownloader = null;
            state = finalState;
            errorMessage = error == null ? "" : error;
        }
        handler.removeCallbacks(notificationTick);
        ExecutorService localRunner = runner;
        if (localRunner != null) {
            localRunner.shutdown();
            runner = null;
        }
        updateNotification();
        stopForegroundCompat();
        stopSelf();
    }

    private void stopActiveDownload(String finalState, String error) {
        FastDownloader downloader;
        synchronized (STATUS_LOCK) {
            downloader = currentDownloader;
            if (downloader == null) {
                return;
            }
            lastStats = downloader.stats();
            currentDownloader = null;
            state = finalState;
            errorMessage = error == null ? "" : error;
        }
        downloader.cancel();
        ExecutorService localRunner = runner;
        if (localRunner != null) {
            localRunner.shutdownNow();
            runner = null;
        }
        handler.removeCallbacks(notificationTick);
    }

    private static void setFinalStatus(String finalState, DownloadStats stats, String error) {
        synchronized (STATUS_LOCK) {
            lastStats = stats;
            currentDownloader = null;
            state = finalState;
            errorMessage = error == null ? "" : error;
            currentMbps = 0;
        }
    }

    private void sampleCurrentMbps(DownloadStats stats) {
        long now = System.currentTimeMillis();
        long bytes = stats.totalBytes();
        long elapsedMillis = Math.max(now - lastSampleMillis, 1);
        currentMbps = DownloadStats.bytesToMbps(bytes - lastBytes, elapsedMillis / 1000.0);
        lastBytes = bytes;
        lastSampleMillis = now;
    }

    private void startForegroundCompat(Notification notification) {
        if (Build.VERSION.SDK_INT >= 29) {
            startForeground(NOTIFICATION_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC);
        } else {
            startForeground(NOTIFICATION_ID, notification);
        }
    }

    @SuppressWarnings("deprecation")
    private void stopForegroundCompat() {
        if (Build.VERSION.SDK_INT >= 24) {
            stopForeground(STOP_FOREGROUND_REMOVE);
        } else {
            stopForeground(true);
        }
    }

    private void updateNotification() {
        NotificationManager manager = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        if (manager != null) {
            DownloadServiceStatus status = snapshot();
            manager.notify(NOTIFICATION_ID, buildNotification(status.state, status.stats));
        }
    }

    @SuppressWarnings("deprecation")
    private Notification buildNotification(String notificationState, DownloadStats stats) {
        Intent openIntent = new Intent(this, MainActivity.class);
        PendingIntent openPendingIntent = PendingIntent.getActivity(
                this,
                0,
                openIntent,
                PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        PendingIntent stopPendingIntent = PendingIntent.getService(
                this,
                1,
                stopIntent(this),
                PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

        String title = "auto-fast-dl " + notificationState;
        String text = String.format(
                Locale.US,
                "workers=%d/%d files=%d failed=%d avg=%.2f Mbps now=%.2f Mbps",
                activeWorkerCount,
                requestedConcurrency,
                stats.totalFiles(),
                stats.failedDownloads(),
                stats.mbps(),
                currentMbps);

        Notification.Builder builder = Build.VERSION.SDK_INT >= 26
                ? new Notification.Builder(this, CHANNEL_ID)
                : new Notification.Builder(this);
        return builder
                .setSmallIcon(R.drawable.ic_stat_download)
                .setContentTitle(title)
                .setContentText(text)
                .setStyle(new Notification.BigTextStyle().bigText(text))
                .setContentIntent(openPendingIntent)
                .setOngoing(currentDownloader != null)
                .addAction(R.drawable.ic_stat_download, "Stop", stopPendingIntent)
                .build();
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT < 26) {
            return;
        }
        NotificationChannel channel = new NotificationChannel(
                CHANNEL_ID,
                "Downloads",
                NotificationManager.IMPORTANCE_LOW);
        channel.setDescription("auto-fast-dl background downloads");
        NotificationManager manager = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        if (manager != null) {
            manager.createNotificationChannel(channel);
        }
    }
}
