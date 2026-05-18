# auto-fast-dl

High-throughput downloader for authorized download and bandwidth tests.

The fast path is the Go implementation (`go run .` / built binary). The Python version remains in `main.py` for compatibility and comparison.

## Setup

Go version:

```powershell
go test ./...
go build -o auto-fast-dl.exe .
```

Python version:

```powershell
python -m pip install -r requirements.txt
```

## Usage

Download one copy to disk:

```powershell
go run . https://example.com/file.bin
```

Fast discard mode for an authorized endpoint:

```powershell
go run . https://example.com/file.bin --sink null --repeat 1000 --concurrency 128
```

Run continuously until Ctrl+C:

```powershell
go run . https://example.com/file.bin --sink null --repeat 0 --concurrency 128
```

Infinite runs are only supported with `--sink null`; use a finite `--repeat` for disk writes.

Useful options:

- `--sink disk|null`: save files or discard bytes after receiving them.
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
