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

const maxConcurrency = 512

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

func (s *DownloadStats) FilesPerSecond() float64 {
	elapsed := s.Elapsed().Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(s.TotalFiles()) / elapsed
}

type DownloadManager struct {
	opts    Options
	stats   DownloadStats
	runID   string
	nextJob atomic.Int64
}

func NewDownloadManager(opts Options) (*DownloadManager, error) {
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

	parsedURL, err := validateURL(rawURL)
	if err != nil {
		return &m.stats, err
	}

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

	client := &http.Client{
		Timeout: m.opts.Timeout,
		Transport: &http.Transport{
			MaxConnsPerHost:     m.opts.Concurrency,
			MaxIdleConns:        m.opts.Concurrency * 2,
			MaxIdleConnsPerHost: m.opts.Concurrency,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
			ForceAttemptHTTP2:   true,
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < m.opts.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			buf := make([]byte, m.opts.ChunkSize)
			for {
				if ctx.Err() != nil {
					return
				}

				jobID := m.nextJob.Add(1)
				if m.opts.Repeat > 0 && jobID > int64(m.opts.Repeat) {
					return
				}

				if err := m.downloadOne(ctx, client, parsedURL, jobID, buf); err != nil {
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

func (m *DownloadManager) downloadOne(ctx context.Context, client *http.Client, rawURL *url.URL, jobID int64, buf []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("User-Agent", "auto-fast-dl-go/"+Version)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("GET %s returned HTTP %d", rawURL.String(), resp.StatusCode)
	}

	if m.opts.Sink == "disk" {
		return m.saveToDisk(resp.Body, jobID, rawURL, buf)
	}

	n, err := io.CopyBuffer(io.Discard, resp.Body, buf)
	if err != nil {
		return err
	}
	m.stats.totalFiles.Add(1)
	m.stats.totalBytes.Add(n)
	return nil
}

func (m *DownloadManager) saveToDisk(body io.Reader, jobID int64, rawURL *url.URL, buf []byte) error {
	outputPath := filepath.Join(m.opts.OutputDir, m.outputFileName(rawURL, jobID))
	tempPath := outputPath + ".part"

	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
		_ = os.Remove(tempPath)
	}()

	n, err := io.CopyBuffer(file, body, buf)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, outputPath); err != nil {
		return err
	}

	m.stats.totalFiles.Add(1)
	m.stats.totalBytes.Add(n)
	return nil
}

func (m *DownloadManager) recordFailure(err error) {
	m.stats.failedDownloads.Add(1)
	m.stats.mu.Lock()
	m.stats.lastError = err.Error()
	m.stats.mu.Unlock()
}

func (m *DownloadManager) cleanupFiles() error {
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
