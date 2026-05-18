package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

var testPayload = []byte(strings.Repeat("0123456789abcdef", 16_384))

func newManagerForTest(t *testing.T, opts Options) *DownloadManager {
	t.Helper()

	if opts.Concurrency == 0 {
		opts.Concurrency = 4
	}
	if opts.Repeat == 0 {
		opts.Repeat = 1
	}
	if opts.Sink == "" {
		opts.Sink = "null"
	}
	if opts.OutputDir == "" {
		opts.OutputDir = t.TempDir()
	}
	if opts.ChunkSize == 0 {
		opts.ChunkSize = 8192
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}

	manager, err := NewDownloadManager(opts)
	if err != nil {
		t.Fatalf("NewDownloadManager() error = %v", err)
	}
	return manager
}

func payloadServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(testPayload)))
		_, _ = w.Write(testPayload)
	}))
}

func TestNullSinkDownloadsAllRepeats(t *testing.T) {
	server := payloadServer(t)
	defer server.Close()

	manager := newManagerForTest(t, Options{
		Concurrency: 8,
		Repeat:      24,
		Sink:        "null",
	})

	stats, err := manager.Run(context.Background(), server.URL+"/payload.bin")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got, want := stats.TotalFiles(), int64(24); got != want {
		t.Fatalf("TotalFiles() = %d, want %d", got, want)
	}
	if got := stats.FailedDownloads(); got != 0 {
		t.Fatalf("FailedDownloads() = %d, want 0", got)
	}
	if got, want := stats.TotalBytes(), int64(len(testPayload)*24); got != want {
		t.Fatalf("TotalBytes() = %d, want %d", got, want)
	}
}

func TestMissingContentLengthDoesNotCrash(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(testPayload)
	}))
	defer server.Close()

	manager := newManagerForTest(t, Options{
		Concurrency: 4,
		Repeat:      8,
		Sink:        "null",
	})

	stats, err := manager.Run(context.Background(), server.URL+"/payload.bin")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got, want := stats.TotalFiles(), int64(8); got != want {
		t.Fatalf("TotalFiles() = %d, want %d", got, want)
	}
	if got, want := stats.TotalBytes(), int64(len(testPayload)*8); got != want {
		t.Fatalf("TotalBytes() = %d, want %d", got, want)
	}
}

func TestZeroByteDownloadsCountAsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "0")
	}))
	defer server.Close()

	manager := newManagerForTest(t, Options{
		Concurrency: 4,
		Repeat:      6,
		Sink:        "null",
		ChunkSize:   1024,
	})

	stats, err := manager.Run(context.Background(), server.URL+"/empty.bin")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got, want := stats.TotalFiles(), int64(6); got != want {
		t.Fatalf("TotalFiles() = %d, want %d", got, want)
	}
	if got := stats.TotalBytes(); got != 0 {
		t.Fatalf("TotalBytes() = %d, want 0", got)
	}
}

func TestUnboundedDiskModeIsRejected(t *testing.T) {
	_, err := NewDownloadManager(Options{
		Concurrency: 4,
		Repeat:      0,
		Sink:        "disk",
		OutputDir:   t.TempDir(),
		ChunkSize:   8192,
		Timeout:     5 * time.Second,
	})
	if err == nil {
		t.Fatal("NewDownloadManager() error = nil, want error")
	}
}

func TestDiskSinkWritesAndCleansOnlyCurrentRunFiles(t *testing.T) {
	server := payloadServer(t)
	defer server.Close()

	outputDir := t.TempDir()
	unrelatedPath := filepath.Join(outputDir, "keep-me.bin")
	if err := os.WriteFile(unrelatedPath, []byte("do not delete"), 0o644); err != nil {
		t.Fatal(err)
	}

	manager := newManagerForTest(t, Options{
		Concurrency: 4,
		Repeat:      5,
		Sink:        "disk",
		OutputDir:   outputDir,
		ChunkSize:   4096,
	})

	stats, err := manager.Run(context.Background(), server.URL+"/payload.bin")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got, want := stats.TotalFiles(), int64(5); got != want {
		t.Fatalf("TotalFiles() = %d, want %d", got, want)
	}

	files, err := filepath.Glob(filepath.Join(outputDir, manager.runID+"-*.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(files), 5; got != want {
		t.Fatalf("downloaded file count = %d, want %d", got, want)
	}
	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if string(content) != string(testPayload) {
			t.Fatalf("downloaded content mismatch for %s", file)
		}
	}

	if err := manager.cleanupFiles(); err != nil {
		t.Fatal(err)
	}
	if content, err := os.ReadFile(unrelatedPath); err != nil || string(content) != "do not delete" {
		t.Fatalf("unrelated file changed, content=%q err=%v", content, err)
	}
	files, err = filepath.Glob(filepath.Join(outputDir, manager.runID+"-*"))
	if err != nil {
		t.Fatal(err)
	}
	if got := len(files); got != 0 {
		t.Fatalf("cleanup left %d run files", got)
	}
}

func TestInvalidOutputDirectoryIsReported(t *testing.T) {
	server := payloadServer(t)
	defer server.Close()

	outputPath := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(outputPath, []byte("file"), 0o644); err != nil {
		t.Fatal(err)
	}

	manager := newManagerForTest(t, Options{
		Concurrency: 2,
		Repeat:      1,
		Sink:        "disk",
		OutputDir:   outputPath,
	})

	_, err := manager.Run(context.Background(), server.URL+"/payload.bin")
	if err == nil {
		t.Fatal("Run() error = nil, want error")
	}
}

func TestMaxFailuresStopsRetries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer server.Close()

	manager := newManagerForTest(t, Options{
		Concurrency: 1,
		Repeat:      10,
		Sink:        "null",
		MaxFailures: 1,
	})

	stats, err := manager.Run(context.Background(), server.URL+"/fail.bin")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := stats.TotalFiles(); got != 0 {
		t.Fatalf("TotalFiles() = %d, want 0", got)
	}
	if got := stats.FailedDownloads(); got != 1 {
		t.Fatalf("FailedDownloads() = %d, want 1", got)
	}
	if !strings.Contains(stats.LastError(), "HTTP 500") {
		t.Fatalf("LastError() = %q, want HTTP 500", stats.LastError())
	}
}

func TestParseCLIAcceptsURLBeforeFlags(t *testing.T) {
	opts, rawURL, err := parseCLI([]string{
		"http://example.com/payload.bin",
		"--sink",
		"null",
		"--repeat",
		"10",
		"-c",
		"4",
	})
	if err != nil {
		t.Fatalf("parseCLI() error = %v", err)
	}
	if rawURL != "http://example.com/payload.bin" {
		t.Fatalf("rawURL = %q", rawURL)
	}
	if opts.Sink != "null" || opts.Repeat != 10 || opts.Concurrency != 4 {
		t.Fatalf("opts = %+v", opts)
	}
}

func BenchmarkNullSink(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(testPayload)))
		_, _ = w.Write(testPayload)
	}))
	defer server.Close()

	manager, err := NewDownloadManager(Options{
		Concurrency: 32,
		Repeat:      b.N,
		Sink:        "null",
		OutputDir:   b.TempDir(),
		ChunkSize:   64 * 1024,
		Timeout:     5 * time.Second,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(testPayload)))
	b.ResetTimer()

	stats, err := manager.Run(context.Background(), server.URL+"/payload.bin")
	if err != nil {
		b.Fatal(err)
	}

	b.StopTimer()
	if got, want := stats.TotalFiles(), int64(b.N); got != want {
		b.Fatalf("TotalFiles() = %d, want %d", got, want)
	}
	if got := stats.FailedDownloads(); got != 0 {
		b.Fatalf("FailedDownloads() = %d, want 0", got)
	}
}
