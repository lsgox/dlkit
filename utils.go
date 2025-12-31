package dlkit

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func FormatSpeed(speed float64) string {
	return FormatBytes(int64(speed)) + "/s"
}

func EstimateRemainingTime(progress *Progress) time.Duration {
	if progress.Speed <= 0 {
		return 0
	}
	remaining := progress.TotalSize - progress.Downloaded
	seconds := float64(remaining) / progress.Speed
	return time.Duration(seconds) * time.Second
}

func parseHashETag(etag string) string {
	if etag == "" {
		return ""
	}

	etagTrimmed := strings.TrimSpace(etag)
	if strings.HasPrefix(etagTrimmed, "W/") || strings.HasPrefix(etagTrimmed, "w/") {
		return ""
	}

	etag = strings.Trim(etag, "\"")
	etag = strings.TrimSpace(etag)
	return etag
}

func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func md5File(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ChunkConfig configures adaptive chunking behavior
type ChunkConfig struct {
	MinChunkSize       int64 // minimum chunk size (default: 1MB)
	MaxChunkSize       int64 // maximum chunk size (default: 10MB)
	OptimalConcurrency int   // optimal workers (default: 5)
	MinConcurrency     int   // minimum workers for chunking (default: 4)
	MaxConcurrency     int   // maximum workers (default: 10)
	ChunksPerWorker    int   // chunks per worker for load balancing (default: 2)
}

// DefaultChunkConfig returns default configuration optimized from test results
func DefaultChunkConfig() *ChunkConfig {
	return &ChunkConfig{
		MinChunkSize:       1 * 1024 * 1024,
		MaxChunkSize:       10 * 1024 * 1024,
		OptimalConcurrency: 5,
		MinConcurrency:     4,
		MaxConcurrency:     10,
		ChunksPerWorker:    2,
	}
}

// AdaptiveChunk creates a function that calculates optimal chunk size and concurrency
func AdaptiveChunk(config *ChunkConfig) func(fileSize int64, currentChunkSize int64, currentConcurrency int) (chunkSize int64, concurrency int) {
	if config == nil {
		config = DefaultChunkConfig()
	}

	return func(fileSize int64, currentChunkSize int64, currentConcurrency int) (chunkSize int64, concurrency int) {
		concurrency = currentConcurrency
		if concurrency < config.MinConcurrency {
			concurrency = config.OptimalConcurrency
		} else if concurrency > config.MaxConcurrency {
			concurrency = config.MaxConcurrency
		}

		desiredChunkCount := config.ChunksPerWorker * concurrency
		chunkSize = fileSize / int64(desiredChunkCount)

		if chunkSize < config.MinChunkSize {
			chunkSize = config.MinChunkSize
		}
		if chunkSize > config.MaxChunkSize {
			chunkSize = config.MaxChunkSize
		}

		actualChunkCount := int((fileSize + chunkSize - 1) / chunkSize)
		if actualChunkCount < concurrency {
			targetChunkCount := concurrency * config.ChunksPerWorker
			chunkSize = fileSize / int64(targetChunkCount)
			if chunkSize < config.MinChunkSize {
				chunkSize = config.MinChunkSize
			}
		}

		return chunkSize, concurrency
	}
}
