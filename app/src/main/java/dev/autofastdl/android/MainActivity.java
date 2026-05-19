package dev.autofastdl.android;

import android.Manifest;
import android.app.Activity;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.graphics.Typeface;
import android.os.Build;
import android.os.Bundle;
import android.os.Environment;
import android.os.Handler;
import android.os.Looper;
import android.text.InputType;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.Spinner;
import android.widget.TextView;

import java.io.File;
import java.util.Locale;

public final class MainActivity extends Activity {
    private final Handler handler = new Handler(Looper.getMainLooper());

    private EditText urlInput;
    private EditText concurrencyInput;
    private EditText repeatInput;
    private EditText outputDirInput;
    private EditText chunkSizeInput;
    private EditText timeoutInput;
    private EditText maxFailuresInput;
    private Spinner sinkInput;
    private CheckBox endlessInput;
    private CheckBox cleanupInput;
    private CheckBox liveProgressInput;
    private Button startButton;
    private Button stopButton;
    private TextView statusView;
    private TextView errorView;

    private final Runnable progressTick = new Runnable() {
        @Override
        public void run() {
            DownloadServiceStatus status = DownloadForegroundService.snapshot();
            if ("idle".equals(status.state)) {
                if (stopButton.isEnabled()) {
                    statusView.setText("starting");
                    handler.postDelayed(this, 1000);
                }
                return;
            }
            if (liveProgressInput.isChecked()) {
                renderStats(status, false);
            }
            if (status.running) {
                handler.postDelayed(this, 1000);
            } else {
                setRunning(false);
            }
        }
    };

    @Override
    protected void onCreate(Bundle state) {
        super.onCreate(state);
        setContentView(buildContent());
        requestNotificationPermissionIfNeeded();
        updateSinkDefaults();
        syncServiceState();
    }

    @Override
    protected void onResume() {
        super.onResume();
        syncServiceState();
    }

    @Override
    protected void onDestroy() {
        handler.removeCallbacks(progressTick);
        super.onDestroy();
    }

    private View buildContent() {
        ScrollView scrollView = new ScrollView(this);
        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.setPadding(dp(18), dp(18), dp(18), dp(18));
        scrollView.addView(root);

        TextView title = new TextView(this);
        title.setText("auto-fast-dl");
        title.setTextSize(26);
        title.setTypeface(Typeface.DEFAULT_BOLD);
        root.addView(title);

        TextView version = new TextView(this);
        version.setText("Version " + FastDownloader.VERSION);
        root.addView(version);

        urlInput = field("URL", "https://example.com/file.bin", InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_VARIATION_URI);
        root.addView(urlInput);

        sinkInput = new Spinner(this);
        sinkInput.setAdapter(new ArrayAdapter<>(this, android.R.layout.simple_spinner_dropdown_item, new String[]{"null", "disk"}));
        sinkInput.setSelection(0);
        sinkInput.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener() {
            @Override
            public void onItemSelected(AdapterView<?> parent, View view, int position, long id) {
                updateSinkDefaults();
            }

            @Override
            public void onNothingSelected(AdapterView<?> parent) {
                updateSinkDefaults();
            }
        });
        root.addView(label("Sink"));
        root.addView(sinkInput);

        concurrencyInput = numberField("Concurrency", String.valueOf(DownloadOptions.defaultConcurrency()));
        repeatInput = numberField("Repeat", "1");
        outputDirInput = field("Output directory", defaultOutputDir().getAbsolutePath(), InputType.TYPE_CLASS_TEXT);
        chunkSizeInput = numberField("Chunk size", String.valueOf(DownloadOptions.DEFAULT_CHUNK_SIZE));
        timeoutInput = numberField("Timeout seconds", "30");
        maxFailuresInput = numberField("Max failures", "0");
        root.addView(concurrencyInput);
        root.addView(repeatInput);
        root.addView(outputDirInput);
        root.addView(chunkSizeInput);
        root.addView(timeoutInput);
        root.addView(maxFailuresInput);

        endlessInput = new CheckBox(this);
        endlessInput.setText("Endless mode");
        endlessInput.setOnClickListener(view -> applyEndlessMode());
        root.addView(endlessInput);

        cleanupInput = new CheckBox(this);
        cleanupInput.setText("Cleanup files created by this run");
        root.addView(cleanupInput);

        liveProgressInput = new CheckBox(this);
        liveProgressInput.setText("Live progress");
        liveProgressInput.setChecked(true);
        root.addView(liveProgressInput);

        LinearLayout buttons = new LinearLayout(this);
        buttons.setOrientation(LinearLayout.HORIZONTAL);
        startButton = new Button(this);
        startButton.setText("Start");
        startButton.setOnClickListener(view -> startDownload());
        stopButton = new Button(this);
        stopButton.setText("Stop");
        stopButton.setOnClickListener(view -> stopDownload());
        buttons.addView(startButton, new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1));
        buttons.addView(stopButton, new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1));
        root.addView(buttons);

        statusView = new TextView(this);
        statusView.setTextSize(15);
        statusView.setTypeface(Typeface.MONOSPACE);
        statusView.setText("idle");
        root.addView(statusView);

        errorView = new TextView(this);
        errorView.setTextSize(14);
        root.addView(errorView);

        return scrollView;
    }

    private void startDownload() {
        try {
            String rawUrl = urlInput.getText().toString().trim();
            SinkMode sinkMode = DownloadOptions.parseSink((String) sinkInput.getSelectedItem());
            int repeat = parseInt(repeatInput, "repeat");
            int concurrency = parseInt(concurrencyInput, "concurrency");
            if (endlessInput.isChecked()) {
                sinkMode = SinkMode.NULL;
                repeat = 0;
                concurrency = DownloadOptions.MAX_CONCURRENCY;
            }
            DownloadOptions options = new DownloadOptions(
                    concurrency,
                    repeat,
                    sinkMode,
                    new File(outputDirInput.getText().toString().trim()),
                    parseInt(chunkSizeInput, "chunk size"),
                    parseInt(timeoutInput, "timeout seconds") * 1000,
                    cleanupInput.isChecked(),
                    parseInt(maxFailuresInput, "max failures"));
            setRunning(true);
            errorView.setText("");
            startForegroundDownload(rawUrl, options);
            renderStats(DownloadForegroundService.snapshot(), false);
            handler.post(progressTick);
        } catch (RuntimeException error) {
            errorView.setText("error: " + error.getMessage());
        }
    }

    private void stopDownload() {
        Intent intent = DownloadForegroundService.stopIntent(this);
        startService(intent);
        DownloadServiceStatus status = DownloadForegroundService.snapshot();
        if (!"idle".equals(status.state)) {
            renderStats(status, false);
        }
        setRunning(false);
    }

    private void setRunning(boolean running) {
        startButton.setEnabled(!running);
        stopButton.setEnabled(running);
        if (!running) {
            handler.removeCallbacks(progressTick);
        }
    }

    private void renderStats(DownloadServiceStatus status, boolean finalLine) {
        DownloadStats stats = status.stats;
        double currentMbps = status.currentMbps;

        statusView.setText(String.format(
                Locale.US,
                "%s\nworkers=%d/%d files=%d failed=%d bytes=%d\ntotal=%.3f GB elapsed=%.2fs\nnow=%.2f Mbps avg=%.2f Mbps %.2f MiB/s\n1h@now=%.2f GB est_1h=%.2f GB files/s=%.2f",
                status.state,
                status.activeWorkerCount,
                status.requestedConcurrency,
                stats.totalFiles(),
                stats.failedDownloads(),
                stats.totalBytes(),
                stats.gb(),
                stats.elapsedSeconds(),
                currentMbps,
                stats.mbps(),
                stats.mibPerSecond(),
                DownloadStats.gbPerHourFromMbps(currentMbps),
                stats.gbPerHour(),
                stats.filesPerSecond()));
        if (!status.errorMessage.isEmpty()) {
            errorView.setText("error: " + status.errorMessage);
        } else if (finalLine && !stats.lastError().isEmpty()) {
            errorView.setText("last error: " + stats.lastError());
        }
    }

    private void startForegroundDownload(String rawUrl, DownloadOptions options) {
        Intent intent = DownloadForegroundService.startIntent(this, rawUrl, options);
        if (Build.VERSION.SDK_INT >= 26) {
            startForegroundService(intent);
        } else {
            startService(intent);
        }
    }

    private void syncServiceState() {
        DownloadServiceStatus status = DownloadForegroundService.snapshot();
        boolean hasStatus = !"idle".equals(status.state);
        setRunning(status.running);
        if (hasStatus) {
            renderStats(status, !status.running);
        } else {
            statusView.setText("idle");
        }
        if (status.running) {
            handler.removeCallbacks(progressTick);
            handler.post(progressTick);
        }
    }

    private void requestNotificationPermissionIfNeeded() {
        if (Build.VERSION.SDK_INT >= 33 && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
            requestPermissions(new String[]{Manifest.permission.POST_NOTIFICATIONS}, 100);
        }
    }

    private void updateSinkDefaults() {
        if (repeatInput == null || sinkInput == null) {
            return;
        }
        if (endlessInput != null && endlessInput.isChecked()) {
            return;
        }
        String sink = (String) sinkInput.getSelectedItem();
        if ("disk".equals(sink) && "0".contentEquals(repeatInput.getText())) {
            repeatInput.setText("1");
        }
    }

    private void applyEndlessMode() {
        boolean endless = endlessInput.isChecked();
        if (endless) {
            sinkInput.setSelection(0);
            concurrencyInput.setText(String.valueOf(DownloadOptions.MAX_CONCURRENCY));
            repeatInput.setText("0");
        }
        sinkInput.setEnabled(!endless);
        concurrencyInput.setEnabled(!endless);
        repeatInput.setEnabled(!endless);
    }

    private EditText field(String label, String value, int inputType) {
        EditText editText = new EditText(this);
        editText.setHint(label);
        editText.setText(value);
        editText.setSingleLine(true);
        editText.setInputType(inputType);
        return editText;
    }

    private EditText numberField(String label, String value) {
        return field(label, value, InputType.TYPE_CLASS_NUMBER);
    }

    private TextView label(String text) {
        TextView label = new TextView(this);
        label.setText(text);
        label.setTextSize(13);
        label.setTypeface(Typeface.DEFAULT_BOLD);
        return label;
    }

    private int parseInt(EditText field, String name) {
        String value = field.getText().toString().trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException(name + " is required");
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException(name + " must be a number");
        }
    }

    private File defaultOutputDir() {
        File external = getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS);
        File base = external == null ? getFilesDir() : external;
        return new File(base, "downloads");
    }

    private int dp(int value) {
        return (int) (value * getResources().getDisplayMetrics().density + 0.5f);
    }
}
