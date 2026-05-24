# auto-fast-dl

High-throughput downloader for authorized download and bandwidth tests.

The Go CLI remains supported (`go run .` / built binary). The Android port lives in `app/` and mirrors the Go feature set in a native UI.

## Go Setup

```powershell
go test ./...
go build -o auto-fast-dl.exe .
go build -ldflags="-X main.BuildProfile=performance" -o auto-fast-dl-performance.exe .
```

## Go Usage

Download one copy and discard it after receiving the bytes:

```powershell
go run . https://example.com/file.bin
```

Save files to disk only when you explicitly need them:

```powershell
go run . https://example.com/file.bin --sink disk --repeat 1
```

Fast discard mode for an authorized endpoint:

```powershell
go run . https://example.com/file.bin --repeat 1000 --concurrency 128
```

Run continuously until Ctrl+C:

```powershell
go run . https://example.com/file.bin --endless
```

`--endless` forces `--sink null`, `--repeat 0`, and `--concurrency 512`.
Infinite runs are only supported with `--sink null`; use a finite `--repeat` and explicit `--sink disk` for disk writes.
Progress output shows current Mbps, average Mbps, total GB received, and the current 1-hour GB projection. Use `--no-progress` to hide the live status line.

The performance build is intentionally not power-saving. It uses higher defaults and raises the Go concurrency cap to `2048`; `--endless` uses that cap in performance binaries.

Useful options:

- `--sink null|disk`: discard bytes after receiving them or save files. Default: `null`.
- `--endless`: run continuously with `--sink null`, `--repeat 0`, and the build's max concurrency.
- `--repeat N` / `-n N`: number of downloads. `0` means infinite.
- `--concurrency N` / `-c N`: number of parallel requests, max `512` in standard builds and `2048` in performance builds.
- `--chunk-size N`: streaming chunk size in bytes. Default: `65536`.
- `--cleanup`: delete files created by the current run only.
- `--max-failures N`: stop after N failed downloads.

Only use this against URLs you own or have permission to test.

## Android

The Android app includes the same core controls as the Go version:

- URL validation for `http://` and `https://`.
- `sink` mode: `null` discards bytes after receiving them; `disk` saves files.
- `repeat`, including `0` for endless mode with `sink=null`.
- `Endless mode`, which requests Go-style `sink=null`, `repeat=0`, and `concurrency=512`.
- `concurrency` up to `512`.
- `output directory`, `chunk size` (default: `65536`), `timeout`, `cleanup`, and `max failures`.
- Live stats for current Mbps, average Mbps, total GB, 1-hour projection, files, failures, and files/s.
- Start runs in a foreground service, so the download keeps running after the app screen is closed. Use the app Stop button or the notification Stop action to cancel it.

Android has standard and performance build flavors. The standard app caps requested concurrency at `512` and active workers at `64`. The performance app uses a separate application id, caps requested concurrency at `2048`, and raises active workers to `256`. It never starts more workers than a finite repeat count requires, while preserving the requested concurrency value in the UI.

Build and test all release files:

```powershell
$env:JAVA_HOME = "C:\Program Files\Android\Android Studio\jbr"
$env:ANDROID_HOME = "$env:LOCALAPPDATA\Android\Sdk"
.\build-all.ps1
```

The generated files are written to `dist\`:

```text
auto-fast-dl-android-performance-debug.apk
auto-fast-dl-android-standard-debug.apk
auto-fast-dl-darwin-amd64
auto-fast-dl-darwin-arm64
auto-fast-dl-linux-amd64
auto-fast-dl-linux-arm64
auto-fast-dl-performance-darwin-amd64
auto-fast-dl-performance-darwin-arm64
auto-fast-dl-performance-linux-amd64
auto-fast-dl-performance-linux-arm64
auto-fast-dl-performance-windows-amd64.exe
auto-fast-dl-windows-amd64.exe
SHA256SUMS
```

Install it on a connected device or emulator:

```powershell
& "$env:ANDROID_HOME\platform-tools\adb.exe" install -r dist\auto-fast-dl-android-standard-debug.apk
& "$env:ANDROID_HOME\platform-tools\adb.exe" install -r dist\auto-fast-dl-android-performance-debug.apk
```

Disk downloads are saved under the app-specific downloads directory by default, so no broad storage permission is required.

## Verify

```powershell
go test ./... -v
go test -ldflags="-X main.BuildProfile=performance" ./... -v
.\gradlew.bat testStandardDebugUnitTest testPerformanceDebugUnitTest assembleStandardDebug assemblePerformanceDebug
go test ./... -run ^$ -bench BenchmarkNullSink -benchtime=1000x
```
