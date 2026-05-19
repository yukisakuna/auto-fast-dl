# auto-fast-dl

High-throughput downloader for authorized download and bandwidth tests.

The supported implementation is Go (`go run .` / built binary).

## Setup

```powershell
go test ./...
go build -o auto-fast-dl.exe .
```

## Usage

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

Useful options:

- `--sink null|disk`: discard bytes after receiving them or save files. Default: `null`.
- `--endless`: run continuously with `--sink null`, `--repeat 0`, and `--concurrency 512`.
- `--repeat N` / `-n N`: number of downloads. `0` means infinite.
- `--concurrency N` / `-c N`: number of parallel requests, max `512`.
- `--chunk-size N`: streaming chunk size in bytes.
- `--cleanup`: delete files created by the current run only.
- `--max-failures N`: stop after N failed downloads.

Only use this against URLs you own or have permission to test.

## Verify

```powershell
go test ./... -v
go test ./... -run ^$ -bench BenchmarkNullSink -benchtime=1000x
python -B -m unittest -v
```
