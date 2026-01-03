package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/lsgox/dlkit"
)

func main() {
	uri := flag.String("url", "", "download URL (required)")
	output := flag.String("o", "", "output file path, default uses temp file")
	workers := flag.Int("w", 5, "concurrent workers, 1 disables chunking")
	chunkSize := flag.Int64("chunk", 10*1024*1024, "chunk size in bytes, only when worker>1")
	resume := flag.Bool("resume", true, "enable resume")
	forceNewConn := flag.Bool("force-new-conn", false, "force new connection per chunk")
	tempDir := flag.String("temp", "", "temp directory, default system temp")

	flag.Parse()

	if *uri == "" {
		flag.Usage()
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dl := dlkit.NewDownloader()

	dl.EnableResume(*resume)
	dl.ForceNewConnection(*forceNewConn)
	if *tempDir != "" {
		dl.Temp(*tempDir)
	}
	if *chunkSize > 0 {
		dl.Chunk(*chunkSize)
	}
	if *workers > 0 {
		dl.Workers(*workers)
	}

	start := time.Now()
	var progressMu sync.Mutex
	lastPrint := time.Now()
	dl.Progress(func(p *dlkit.Progress) {
		progressMu.Lock()
		defer progressMu.Unlock()

		now := time.Now()
		if now.Sub(lastPrint) < 200*time.Millisecond && p.Percentage < 100 {
			return
		}
		lastPrint = now

		eta := dlkit.EstimateRemainingTime(p)
		log.Printf("\rdownloaded: %s / %s (%.2f%%) speed: %s eta: %v",
			dlkit.FormatBytes(p.Downloaded),
			dlkit.FormatBytes(p.TotalSize),
			p.Percentage,
			dlkit.FormatSpeed(p.Speed),
			eta.Truncate(time.Second))
	})

	log.Println("start download:", *uri)
	result, err := dl.Download(ctx, *uri)
	if err != nil {
		log.Printf("\ndownload failed: %v\n", err)
		os.Exit(1)
	}
	defer result.CleanupTempFiles()

	finalPath := result.FilePath
	outPath := *output
	if outPath == "" {
		if parsed, err := url.Parse(*uri); err == nil {
			name := path.Base(parsed.Path)
			if name == "" || name == "." || name == "/" {
				name = fmt.Sprintf("download-%d", time.Now().Unix())
			}
			outPath = filepath.Join(".", name)
		}
	}
	if outPath != "" {
		if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil && !os.IsExist(err) {
			log.Printf("\ncreate output dir failed: %v\n", err)
			os.Exit(1)
		}
		if err := result.SaveTo(outPath); err != nil {
			log.Printf("\nsave file failed: %v\n", err)
			os.Exit(1)
		}
		finalPath = outPath
	}

	elapsed := time.Since(start)
	log.Printf("download finished: %s (elapsed: %v)\n", finalPath, elapsed)
}
