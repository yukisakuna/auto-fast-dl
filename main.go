package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"time"
)

func main() {
	opts, rawURL, err := parseCLI(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if rawURL == "" {
		rawURL, err = promptURL()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}

	manager, err := NewDownloadManager(opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	stats, err := manager.Run(ctx, rawURL)
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	interrupted := errors.Is(err, context.Canceled) || ctx.Err() != nil
	printSummary(stats, interrupted)

	if interrupted {
		os.Exit(130)
	}
	if stats.FailedDownloads() > 0 {
		os.Exit(1)
	}
}

func parseCLI(args []string) (Options, string, error) {
	opts := Options{
		Concurrency: defaultConcurrency(),
		Repeat:      1,
		Sink:        "null",
		OutputDir:   "downloads",
		ChunkSize:   defaultChunkSize,
		Timeout:     30 * time.Second,
	}
	timeoutSeconds := 30
	endless := false
	showHelp := false
	showVersion := false

	fs := flag.NewFlagSet("auto-fast-dl", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.IntVar(&opts.Concurrency, "concurrency", opts.Concurrency, "parallel downloads")
	fs.IntVar(&opts.Concurrency, "c", opts.Concurrency, "parallel downloads")
	fs.IntVar(&opts.Repeat, "repeat", opts.Repeat, "number of downloads; 0 means run until interrupted")
	fs.IntVar(&opts.Repeat, "n", opts.Repeat, "number of downloads; 0 means run until interrupted")
	fs.BoolVar(&endless, "endless", false, fmt.Sprintf("run forever with --sink null, --repeat 0, and --concurrency %d", maxConcurrency))
	fs.StringVar(&opts.Sink, "sink", opts.Sink, "null discards bytes after receiving them; disk saves files")
	fs.StringVar(&opts.OutputDir, "output-dir", opts.OutputDir, "directory for disk downloads")
	fs.StringVar(&opts.OutputDir, "o", opts.OutputDir, "directory for disk downloads")
	fs.IntVar(&opts.ChunkSize, "chunk-size", opts.ChunkSize, "streaming chunk size in bytes")
	fs.IntVar(&timeoutSeconds, "timeout", timeoutSeconds, "socket timeout in seconds")
	fs.BoolVar(&opts.Cleanup, "cleanup", false, "delete files created by this run after completion")
	fs.IntVar(&opts.MaxFailures, "max-failures", opts.MaxFailures, "stop after this many failed downloads; 0 disables the limit")
	fs.BoolVar(&opts.NoProgress, "no-progress", false, "disable progress output")
	fs.BoolVar(&showVersion, "version", false, "show version")
	fs.BoolVar(&showHelp, "h", false, "show help")
	fs.BoolVar(&showHelp, "help", false, "show help")

	flagArgs, rawURL := splitArgs(args)
	if err := fs.Parse(flagArgs); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.Usage()
		}
		return opts, rawURL, err
	}

	if timeoutSeconds <= 0 {
		timeoutSeconds = 30
	}
	opts.Timeout = time.Duration(timeoutSeconds) * time.Second

	if showHelp {
		printUsage(fs)
		return opts, rawURL, flag.ErrHelp
	}
	if showVersion {
		fmt.Println(versionString())
		return opts, rawURL, flag.ErrHelp
	}

	if strings.TrimSpace(opts.Sink) == "" {
		opts.Sink = "null"
	}
	if endless {
		opts.Concurrency = maxConcurrency
		opts.Repeat = 0
		opts.Sink = "null"
	}

	return opts, rawURL, nil
}

func splitArgs(args []string) ([]string, string) {
	boolFlags := map[string]struct{}{
		"cleanup":     {},
		"endless":     {},
		"no-progress": {},
		"version":     {},
		"h":           {},
		"help":        {},
	}

	var flagArgs []string
	var rawURL string
	expectValue := false

	for i := 0; i < len(args); i++ {
		arg := args[i]

		if expectValue {
			flagArgs = append(flagArgs, arg)
			expectValue = false
			continue
		}

		if arg == "--" {
			for j := i + 1; j < len(args); j++ {
				if rawURL == "" {
					rawURL = args[j]
				} else {
					flagArgs = append(flagArgs, args[j])
				}
			}
			break
		}

		if strings.HasPrefix(arg, "-") && arg != "-" {
			flagArgs = append(flagArgs, arg)
			if strings.Contains(arg, "=") {
				continue
			}
			name := strings.TrimLeft(arg, "-")
			if idx := strings.IndexByte(name, '='); idx >= 0 {
				name = name[:idx]
			}
			if _, ok := boolFlags[name]; ok {
				continue
			}
			expectValue = true
			continue
		}

		if rawURL == "" {
			rawURL = arg
			continue
		}
		flagArgs = append(flagArgs, arg)
	}

	return flagArgs, rawURL
}

func printUsage(fs *flag.FlagSet) {
	fmt.Fprintf(os.Stderr, "Usage: %s [flags] [url]\n\n", os.Args[0])
	fs.SetOutput(os.Stderr)
	fs.PrintDefaults()
}

func printSummary(stats *DownloadStats, interrupted bool) {
	status := "complete"
	if interrupted {
		status = "interrupted"
	}

	fmt.Printf(
		"%s: files=%d failed=%d bytes=%d total=%.3f GB elapsed=%.2fs speed=%.2f Mbps (%.2f MiB/s) est_1h=%.2f GB files/s=%.2f\n",
		status,
		stats.TotalFiles(),
		stats.FailedDownloads(),
		stats.TotalBytes(),
		stats.GB(),
		stats.Elapsed().Seconds(),
		stats.Mbps(),
		stats.MiBPerSecond(),
		stats.GBPerHour(),
		stats.FilesPerSecond(),
	)
	if lastError := stats.LastError(); lastError != "" {
		fmt.Printf("last error: %s\n", lastError)
	}
}

func promptURL() (string, error) {
	fmt.Print("Enter the URL to download: ")
	reader := bufio.NewReader(os.Stdin)
	text, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return strings.TrimSpace(text), nil
}

func defaultConcurrency() int {
	cpus := runtime.NumCPU()
	multiplier := 8
	floor := 32
	if performanceBuild() {
		multiplier = 16
		floor = 64
	}
	value := cpus * multiplier
	if value < floor {
		value = floor
	}
	if value > maxConcurrency {
		value = maxConcurrency
	}
	return value
}
