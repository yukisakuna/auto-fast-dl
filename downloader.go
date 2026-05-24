package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const Version = "0.1.0"

const minStatFlushBytes = 8 * 1024
const maxStatFlushBytes = 1024 * 1024

var BuildProfile = "standard"

var maxConcurrency = configuredMaxConcurrency()
var defaultChunkSize = configuredDefaultChunkSize()

func configuredMaxConcurrency() int {
	if performanceBuild() {
		return 2048
	}
	return 512
}

func configuredDefaultChunkSize() int {
	if performanceBuild() {
		return 1024 * 1024
	}
	return 64 * 1024
}

func performanceBuild() bool {
	return strings.EqualFold(BuildProfile, "performance")
}

func versionString() string {
	if performanceBuild() {
		return Version + "-performance"
	}
	return Version
}

type Options struct {
	Concurrency int
	Repeat      int
	Sink        string
	OutputDir   string
	ChunkSize   int
	Timeout     time.Duration
	Cleanup     bool
	MaxFailures int
	NoProgress  bool
}

type DownloadStats struct {
	totalFiles      atomic.Int64
	failedDownloads atomic.Int64
	totalBytes      atomic.Int64

	start time.Time

	mu        sync.Mutex
	lastError string
}

func (s *DownloadStats) reset() {
	s.totalFiles.Store(0)
	s.failedDownloads.Store(0)
	s.totalBytes.Store(0)
	s.start = time.Now()
	s.mu.Lock()
	s.lastError = ""
	s.mu.Unlock()
}

func (s *DownloadStats) TotalFiles() int64 {
	return s.totalFiles.Load()
}

func (s *DownloadStats) FailedDownloads() int64 {
	return s.failedDownloads.Load()
}

func (s *DownloadStats) TotalBytes() int64 {
	return s.totalBytes.Load()
}

func (s *DownloadStats) LastError() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastError
}

func (s *DownloadStats) Elapsed() time.Duration {
	if s.start.IsZero() {
		return 0
	}
	return time.Since(s.start)
}

func (s *DownloadStats) MiBPerSecond() float64 {
	elapsed := s.Elapsed().Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(s.TotalBytes()) / (1024 * 1024) / elapsed
}

func (s *DownloadStats) Mbps() float64 {
	elapsed := s.Elapsed().Seconds()
	if elapsed <= 0 {
		return 0
	}
	return bytesToMbps(float64(s.TotalBytes()), elapsed)
}

func (s *DownloadStats) GB() float64 {
	return float64(s.TotalBytes()) / 1_000_000_000
}

func (s *DownloadStats) GBPerHour() float64 {
	elapsed := s.Elapsed().Seconds()
	if elapsed <= 0 {
		return 0
	}
	return (float64(s.TotalBytes()) / elapsed) * 3600 / 1_000_000_000
}

func (s *DownloadStats) FilesPerSecond() float64 {
	elapsed := s.Elapsed().Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(s.TotalFiles()) / elapsed
}

type DownloadManager struct {
	opts         Options
	stats        DownloadStats
	runID        string
	nextJob      atomic.Int64
	createdMu    sync.Mutex
	createdFiles []string
}

type countingWriter struct {
	w          io.Writer
	stats      *DownloadStats
	pending    int64
	flushBytes int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n := len(p)
	var err error
	if cw.w != nil {
		n, err = cw.w.Write(p)
	}
	if n > 0 {
		cw.pending += int64(n)
		if cw.pending >= cw.flushBytes {
			cw.Flush()
		}
	}
	return n, err
}

func (cw *countingWriter) Flush() {
	if cw.pending > 0 {
		if cw.stats != nil {
			cw.stats.totalBytes.Add(cw.pending)
		}
		cw.pending = 0
	}
}

func NewDownloadManager(opts Options) (*DownloadManager, error) {
	if strings.TrimSpace(opts.Sink) == "" {
		opts.Sink = "null"
	}
	if opts.ChunkSize == 0 {
		opts.ChunkSize = defaultChunkSize
	}
	if opts.Concurrency < 1 {
		return nil, fmt.Errorf("concurrency must be >= 1")
	}
	if opts.Concurrency > maxConcurrency {
		return nil, fmt.Errorf("concurrency must be <= %d", maxConcurrency)
	}
	if opts.Repeat < 0 {
		return nil, fmt.Errorf("repeat must be >= 0; use 0 for infinite")
	}
	if opts.ChunkSize < 1024 {
		return nil, fmt.Errorf("chunk size must be >= 1024 bytes")
	}
	if opts.Timeout <= 0 {
		return nil, fmt.Errorf("timeout must be > 0")
	}
	if opts.Sink != "disk" && opts.Sink != "null" {
		return nil, fmt.Errorf("sink must be disk or null")
	}
	if opts.Repeat == 0 && opts.Sink == "disk" {
		return nil, fmt.Errorf("repeat=0 is only supported with sink null")
	}
	if opts.OutputDir == "" {
		opts.OutputDir = "downloads"
	}
	return &DownloadManager{
		opts:  opts,
		runID: newRunID(),
	}, nil
}

func (m *DownloadManager) Run(parent context.Context, rawURL string) (*DownloadStats, error) {
	m.stats.reset()
	m.runID = newRunID()
	m.nextJob.Store(0)
	m.createdMu.Lock()
	m.createdFiles = nil
	m.createdMu.Unlock()

	parsedURL, err := validateURL(rawURL)
	if err != nil {
		return &m.stats, err
	}
	urlString := parsedURL.String()
	workerCount := m.workerCount()

	if m.opts.Sink == "disk" {
		if err := prepareOutputDir(m.opts.OutputDir); err != nil {
			return &m.stats, err
		}
	}
	if m.opts.Cleanup {
		defer func() {
			_ = m.cleanupFiles()
		}()
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stopProgress := m.startProgress()
	defer stopProgress()

	transport := &http.Transport{
		MaxConnsPerHost:     workerCount,
		MaxIdleConns:        workerCount,
		MaxIdleConnsPerHost: workerCount,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   true,
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Timeout:   m.opts.Timeout,
		Transport: transport,
	}

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var buf []byte
			for {
				if ctx.Err() != nil {
					return
				}

				jobID := m.nextJob.Add(1)
				if m.opts.Repeat > 0 && jobID > int64(m.opts.Repeat) {
					return
				}
				if buf == nil {
					buf = make([]byte, m.opts.ChunkSize)
				}

				if err := m.downloadOne(ctx, client, parsedURL, urlString, jobID, buf); err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}

					m.recordFailure(err)
					if m.opts.MaxFailures > 0 && m.stats.FailedDownloads() >= int64(m.opts.MaxFailures) {
						cancel()
						return
					}
				}
			}
		}()
	}

	wg.Wait()

	if parent.Err() != nil {
		return &m.stats, parent.Err()
	}

	return &m.stats, nil
}

func (m *DownloadManager) workerCount() int {
	if m.opts.Repeat > 0 && m.opts.Repeat < m.opts.Concurrency {
		return m.opts.Repeat
	}
	return m.opts.Concurrency
}

func (m *DownloadManager) downloadOne(ctx context.Context, client *http.Client, rawURL *url.URL, urlString string, jobID int64, buf []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlString, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("User-Agent", "auto-fast-dl-go/"+versionString())

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		counter := newCountingWriter(nil, nil, len(buf))
		_, _ = io.CopyBuffer(&counter, resp.Body, buf)
		return fmt.Errorf("GET %s returned HTTP %d", urlString, resp.StatusCode)
	}

	if m.opts.Sink == "disk" {
		return m.saveToDisk(resp.Body, jobID, rawURL, buf)
	}

	counter := newCountingWriter(nil, &m.stats, len(buf))
	_, err = io.CopyBuffer(&counter, resp.Body, buf)
	counter.Flush()
	if err != nil {
		return err
	}
	m.stats.totalFiles.Add(1)
	return nil
}

func (m *DownloadManager) saveToDisk(body io.Reader, jobID int64, rawURL *url.URL, buf []byte) error {
	outputPath := filepath.Join(m.opts.OutputDir, m.outputFileName(rawURL, jobID))
	tempPath := outputPath + ".part"

	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	counter := newCountingWriter(file, &m.stats, len(buf))
	_, err = io.CopyBuffer(&counter, body, buf)
	counter.Flush()
	if err != nil {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, outputPath); err != nil {
		_ = os.Remove(tempPath)
		return err
	}

	m.recordCreatedFile(outputPath)
	m.stats.totalFiles.Add(1)
	return nil
}

func newCountingWriter(w io.Writer, stats *DownloadStats, bufSize int) countingWriter {
	flushBytes := int64(bufSize) * 4
	if flushBytes < minStatFlushBytes {
		flushBytes = minStatFlushBytes
	}
	if flushBytes > maxStatFlushBytes {
		flushBytes = maxStatFlushBytes
	}
	return countingWriter{
		w:          w,
		stats:      stats,
		flushBytes: flushBytes,
	}
}

func (m *DownloadManager) recordCreatedFile(path string) {
	m.createdMu.Lock()
	m.createdFiles = append(m.createdFiles, path)
	m.createdMu.Unlock()
}

func (m *DownloadManager) recordFailure(err error) {
	m.stats.failedDownloads.Add(1)
	m.stats.mu.Lock()
	m.stats.lastError = err.Error()
	m.stats.mu.Unlock()
}

func (m *DownloadManager) startProgress() func() {
	if m.opts.NoProgress || !isTerminal(os.Stderr) {
		return func() {}
	}

	done := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once

	go func() {
		defer close(finished)
		m.renderProgress(done)
	}()

	return func() {
		once.Do(func() {
			close(done)
			<-finished
		})
	}
}

func (m *DownloadManager) renderProgress(done <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	frames := []string{"|", "/", "-", "\\"}
	frameIndex := 0
	lastBytes := m.stats.TotalBytes()
	lastTime := time.Now()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			totalBytes := m.stats.TotalBytes()
			currentMbps := sampleMbps(totalBytes-lastBytes, now.Sub(lastTime))
			fmt.Fprintf(os.Stderr, "\r\033[2K%s", formatProgressLine(&m.stats, currentMbps, m.opts, frames[frameIndex%len(frames)]))
			lastBytes = totalBytes
			lastTime = now
			frameIndex++
		case <-done:
			now := time.Now()
			totalBytes := m.stats.TotalBytes()
			currentMbps := sampleMbps(totalBytes-lastBytes, now.Sub(lastTime))
			fmt.Fprintf(os.Stderr, "\r\033[2K%s\n", formatProgressLine(&m.stats, currentMbps, m.opts, "done"))
			return
		}
	}
}

func formatProgressLine(stats *DownloadStats, currentMbps float64, opts Options, marker string) string {
	return fmt.Sprintf(
		"[%s] now=%7.2f Mbps avg=%7.2f Mbps total=%8.3f GB 1h@now=%8.2f GB files=%d failed=%d elapsed=%s sink=%s",
		marker,
		currentMbps,
		stats.Mbps(),
		stats.GB(),
		gbPerHourFromMbps(currentMbps),
		stats.TotalFiles(),
		stats.FailedDownloads(),
		stats.Elapsed().Round(time.Second),
		opts.Sink,
	)
}

func sampleMbps(byteDelta int64, elapsed time.Duration) float64 {
	if byteDelta <= 0 || elapsed <= 0 {
		return 0
	}
	return bytesToMbps(float64(byteDelta), elapsed.Seconds())
}

func bytesToMbps(bytes float64, elapsedSeconds float64) float64 {
	if bytes <= 0 || elapsedSeconds <= 0 {
		return 0
	}
	return (bytes * 8) / elapsedSeconds / 1_000_000
}

func gbPerHourFromMbps(mbps float64) float64 {
	if mbps <= 0 {
		return 0
	}
	return (mbps * 1_000_000 / 8) * 3600 / 1_000_000_000
}

func isTerminal(file *os.File) bool {
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

func (m *DownloadManager) cleanupFiles() error {
	m.createdMu.Lock()
	createdFiles := append([]string(nil), m.createdFiles...)
	m.createdMu.Unlock()

	if len(createdFiles) > 0 {
		for _, file := range createdFiles {
			_ = os.Remove(file)
		}
		return nil
	}

	entries, err := os.ReadDir(m.opts.OutputDir)
	if err != nil {
		return err
	}

	prefix := m.runID + "-"
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, prefix) {
			continue
		}
		_ = os.Remove(filepath.Join(m.opts.OutputDir, name))
	}
	return nil
}

func (m *DownloadManager) outputFileName(rawURL *url.URL, jobID int64) string {
	ext := path.Ext(rawURL.Path)
	if ext == "" {
		ext = ".bin"
	}
	return fmt.Sprintf("%s-%08d%s", m.runID, jobID, ext)
}

func validateURL(rawURL string) (*url.URL, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("URL must start with http:// or https://")
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("URL must include a host")
	}
	return parsed, nil
}

func prepareOutputDir(dir string) error {
	info, err := os.Stat(dir)
	switch {
	case err == nil && !info.IsDir():
		return fmt.Errorf("output directory is not a directory: %s", dir)
	case err == nil:
		return nil
	case os.IsNotExist(err):
		return os.MkdirAll(dir, 0o755)
	default:
		return err
	}
}

func newRunID() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}
