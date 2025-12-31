package dlkit

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Downloader struct {
	concurrency    int
	chunkSize      int64
	client         *http.Client
	onProgress     func(progress *Progress)
	resume         bool
	onChunkConfig  func(fileSize int64, currentChunkSize int64, currentConcurrency int) (chunkSize int64, concurrency int)
	onChunkSpeed   func(chunkIndex int, speed float64, chunkSize int64)
	tempDir        string
	defaultHeaders map[string]string
	// forceNewConnection forces each chunk to use a new connection
	// For chunked downloads, using separate connections provides better parallelism,
	// especially with HTTP/1.1 where requests on the same connection are serialized.
	// Default: false (reuse connections when possible via connection pool)
	forceNewConnection bool
}

type Progress struct {
	TotalSize   int64
	Downloaded  int64
	Percentage  float64
	Speed       float64
	ElapsedTime time.Duration
}

type DownloadResult struct {
	FilePath string
	FileInfo *FileInfo
	TempPath string
	TempDir  string
}

func (r *DownloadResult) CleanupTempFiles() error {
	var errs []error
	if r.TempDir != "" {
		if err := os.RemoveAll(r.TempDir); err != nil {
			errs = append(errs, fmt.Errorf("failed to remove temp dir %s: %w", r.TempDir, err))
		}
	}
	if r.TempPath != "" && r.TempPath != r.FilePath {
		if err := os.Remove(r.TempPath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("failed to remove temp file %s: %w", r.TempPath, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("cleanup errors: %v", errs)
	}
	return nil
}

type Chunk struct {
	Index    int
	Start    int64
	End      int64
	FilePath string
	Speed    float64
}

type chunkStats struct {
	speeds []float64
	mu     sync.Mutex
}

func NewDownloader() *Downloader {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}

	return &Downloader{
		concurrency: 5,
		chunkSize:   10 * 1024 * 1024,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
		resume: true,
		defaultHeaders: map[string]string{
			"User-Agent": "",
		},
	}
}

func (d *Downloader) Workers(n int) *Downloader {
	d.concurrency = n
	return d
}

func (d *Downloader) Chunk(size int64) *Downloader {
	d.chunkSize = size
	return d
}

func (d *Downloader) HTTPClient(client *http.Client) *Downloader {
	d.client = client
	return d
}

func (d *Downloader) Progress(fn func(*Progress)) *Downloader {
	d.onProgress = fn
	return d
}

func (d *Downloader) EnableResume(enable bool) *Downloader {
	d.resume = enable
	return d
}

func (d *Downloader) AdaptiveChunk(fn func(fileSize int64, currentChunkSize int64, currentConcurrency int) (chunkSize int64, concurrency int)) *Downloader {
	d.onChunkConfig = fn
	return d
}

func (d *Downloader) ChunkSpeed(fn func(chunkIndex int, speed float64, chunkSize int64)) *Downloader {
	d.onChunkSpeed = fn
	return d
}

func (d *Downloader) Temp(dir string) *Downloader {
	d.tempDir = dir
	return d
}

func (d *Downloader) NewConnection(force bool) *Downloader {
	d.forceNewConnection = force
	return d
}

func (d *Downloader) checkExistingFile(filePath string, fileInfo *FileInfo) (bool, error) {
	if !d.resume {
		return false, nil
	}

	info, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	if fileInfo.Size > 0 && info.Size() != fileInfo.Size {
		return false, nil
	}

	if fileInfo.ContentMD5 != "" || fileInfo.HashETag != "" {
		if _, err := d.verifyFile(filePath, fileInfo); err == nil {
			return true, nil
		}
		return false, nil
	}

	if fileInfo.Size > 0 && info.Size() == fileInfo.Size {
		return true, nil
	}

	return false, nil
}

func (d *Downloader) Download(ctx context.Context, url string) (*DownloadResult, error) {
	fileInfo, err := d.getFileInfo(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	tempFile, err := d.createTempFile()
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	tempFile.Close()

	if complete, err := d.checkExistingFile(tempPath, fileInfo); err != nil {
		return nil, fmt.Errorf("failed to check existing file: %w", err)
	} else if complete {
		return &DownloadResult{
			FilePath: tempPath,
			FileInfo: fileInfo,
			TempPath: tempPath,
			TempDir:  "",
		}, nil
	}

	if !fileInfo.SupportsRange {
		if err := d.downloadDirect(ctx, url, tempPath, fileInfo.Size); err != nil {
			return &DownloadResult{
				FilePath: tempPath,
				FileInfo: fileInfo,
				TempPath: tempPath,
				TempDir:  "",
			}, err
		}
		verifiedPath, err := d.verifyFile(tempPath, fileInfo)
		if err != nil {
			return &DownloadResult{
				FilePath: tempPath,
				FileInfo: fileInfo,
				TempPath: tempPath,
				TempDir:  "",
			}, err
		}
		return &DownloadResult{
			FilePath: verifiedPath,
			FileInfo: fileInfo,
			TempPath: tempPath,
			TempDir:  "",
		}, nil
	}

	chunkSize := d.chunkSize
	concurrency := d.concurrency
	if d.onChunkConfig != nil {
		chunkSize, concurrency = d.onChunkConfig(fileInfo.Size, d.chunkSize, d.concurrency)
	}

	tempDir := tempPath + ".tmp"
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return &DownloadResult{
			FilePath: tempPath,
			FileInfo: fileInfo,
			TempPath: tempPath,
			TempDir:  tempDir,
		}, fmt.Errorf("failed to create temp directory: %w", err)
	}

	chunks := d.calculateChunks(fileInfo.Size, chunkSize, tempDir)

	progress := &Progress{
		TotalSize: fileInfo.Size,
	}
	startTime := time.Now()
	stats := &chunkStats{}

	if err := d.downloadChunks(ctx, url, chunks, progress, startTime, concurrency, stats); err != nil {
		return &DownloadResult{
			FilePath: tempPath,
			FileInfo: fileInfo,
			TempPath: tempPath,
			TempDir:  tempDir,
		}, err
	}

	if err := d.mergeChunks(chunks, tempDir, tempPath); err != nil {
		return &DownloadResult{
			FilePath: tempPath,
			FileInfo: fileInfo,
			TempPath: tempPath,
			TempDir:  tempDir,
		}, fmt.Errorf("failed to merge chunks: %w", err)
	}

	os.RemoveAll(tempDir)
	verifiedPath, err := d.verifyFile(tempPath, fileInfo)
	if err != nil {
		return &DownloadResult{
			FilePath: tempPath,
			FileInfo: fileInfo,
			TempPath: tempPath,
			TempDir:  tempDir,
		}, err
	}
	return &DownloadResult{
		FilePath: verifiedPath,
		FileInfo: fileInfo,
		TempPath: tempPath,
		TempDir:  "",
	}, nil
}

func (d *Downloader) createTempFile() (*os.File, error) {
	if d.tempDir != "" {
		if err := os.MkdirAll(d.tempDir, 0755); err != nil {
			return nil, err
		}
		return os.CreateTemp(d.tempDir, "dlkit-*")
	}
	return os.CreateTemp("", "dlkit-*")
}

type FileInfo struct {
	Size          int64
	SupportsRange bool
	HashETag      string
	ContentMD5    string
}

func (d *Downloader) setHeaders(req *http.Request) {
	for k, v := range d.defaultHeaders {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
}

func (d *Downloader) getFileInfo(ctx context.Context, url string) (*FileInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return nil, err
	}
	d.setHeaders(req)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP status code: %d", resp.StatusCode)
	}

	info := &FileInfo{
		SupportsRange: resp.Header.Get("Accept-Ranges") == "bytes",
		HashETag:      parseHashETag(resp.Header.Get("ETag")),
		ContentMD5:    resp.Header.Get("Content-MD5"),
	}

	if resp.ContentLength > 0 {
		info.Size = resp.ContentLength
		return info, nil
	}

	if !info.SupportsRange {
		return nil, fmt.Errorf("unable to determine file size: HEAD request didn't return Content-Length and server doesn't support Range requests")
	}

	req, err = http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	d.setHeaders(req)
	req.Header.Set("Range", "bytes=0-0")

	resp, err = d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	io.CopyN(io.Discard, resp.Body, 1)

	switch resp.StatusCode {
	case http.StatusPartialContent:
		contentRange := resp.Header.Get("Content-Range")
		if contentRange != "" {
			var size int64
			n, err := fmt.Sscanf(contentRange, "bytes 0-0/%d", &size)
			if err == nil && n == 1 && size > 0 {
				info.Size = size
				return info, nil
			}
			parts := strings.Split(contentRange, "/")
			if len(parts) == 2 && parts[1] != "*" {
				if size, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil && size > 0 {
					info.Size = size
					return info, nil
				}
			}
		}
	case http.StatusOK:
		if resp.ContentLength > 0 {
			info.Size = resp.ContentLength
			info.SupportsRange = false
			return info, nil
		}
	}

	return nil, fmt.Errorf("unable to determine file size")
}

func (d *Downloader) downloadDirect(ctx context.Context, url, destPath string, fileSize int64) error {
	var startOffset int64 = 0
	var file *os.File
	var err error

	if d.resume {
		if info, err := os.Stat(destPath); err == nil {
			existingSize := info.Size()
			if existingSize == fileSize {
				return nil
			} else if existingSize > 0 && existingSize < fileSize {
				startOffset = existingSize
			} else {
				os.Remove(destPath)
			}
		}
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	d.setHeaders(req)

	if startOffset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", startOffset))
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if startOffset > 0 {
		if resp.StatusCode != http.StatusPartialContent {
			os.Remove(destPath)
			startOffset = 0
			req, _ = http.NewRequestWithContext(ctx, "GET", url, nil)
			d.setHeaders(req)
			resp, err = d.client.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
		}
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("HTTP status code: %d", resp.StatusCode)
	}

	if startOffset > 0 {
		file, err = os.OpenFile(destPath, os.O_WRONLY|os.O_APPEND, 0644)
	} else {
		file, err = os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	}
	if err != nil {
		return err
	}
	defer file.Close()

	progress := &Progress{
		TotalSize:  fileSize,
		Downloaded: startOffset,
	}
	startTime := time.Now()
	downloaded := startOffset

	// 使用原子操作替代锁
	reader := &progressReader{
		reader:     resp.Body,
		progress:   progress,
		downloaded: &downloaded,
		onProgress: d.onProgress,
		startTime:  startTime,
	}

	_, err = io.Copy(file, reader)
	if err != nil {
		return err
	}

	if d.onProgress != nil {
		progress.Downloaded = progress.TotalSize
		progress.Percentage = 100
		elapsed := time.Since(startTime)
		progress.ElapsedTime = elapsed
		if elapsed.Seconds() > 0 {
			progress.Speed = float64(progress.TotalSize) / elapsed.Seconds()
		}
		d.onProgress(progress)
	}

	return nil
}

type progressReader struct {
	reader     io.Reader
	progress   *Progress
	downloaded *int64
	onProgress func(*Progress)
	startTime  time.Time
	lastUpdate time.Time
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	if n > 0 {
		downloaded := atomic.AddInt64(pr.downloaded, int64(n))
		pr.progress.Downloaded = downloaded

		now := time.Now()
		if now.Sub(pr.lastUpdate) < 100*time.Millisecond && err == nil {
			return n, err
		}
		pr.lastUpdate = now

		elapsed := time.Since(pr.startTime)
		pr.progress.ElapsedTime = elapsed
		if elapsed.Seconds() > 0 {
			pr.progress.Speed = float64(downloaded) / elapsed.Seconds()
		}
		pr.progress.Percentage = float64(downloaded) / float64(pr.progress.TotalSize) * 100
		if pr.onProgress != nil {
			pr.onProgress(pr.progress)
		}
	}
	return n, err
}

func (d *Downloader) calculateChunks(fileSize, chunkSize int64, tempDir string) []Chunk {
	chunkCount := int((fileSize + chunkSize - 1) / chunkSize)
	chunks := make([]Chunk, 0, chunkCount)

	var start int64 = 0
	for i := 0; start < fileSize; i++ {
		end := start + chunkSize - 1
		if end >= fileSize {
			end = fileSize - 1
		}

		chunks = append(chunks, Chunk{
			Index:    i,
			Start:    start,
			End:      end,
			FilePath: filepath.Join(tempDir, fmt.Sprintf("chunk_%d", i)),
		})

		start = end + 1
	}

	return chunks
}

func (d *Downloader) downloadChunks(ctx context.Context, url string, chunks []Chunk, progress *Progress, startTime time.Time, concurrency int, stats *chunkStats) error {
	semaphore := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	errCh := make(chan error, len(chunks))
	progressCh := make(chan int64, len(chunks))

	for i := range chunks {
		wg.Add(1)
		go func(chunk Chunk) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			chunkStartTime := time.Now()
			downloaded, err := d.downloadChunk(ctx, url, chunk)
			chunkElapsed := time.Since(chunkStartTime)

			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}

			chunkSize := chunk.End - chunk.Start + 1
			var chunkSpeed float64
			if chunkElapsed.Seconds() > 0 {
				chunkSpeed = float64(chunkSize) / chunkElapsed.Seconds()
			}

			if d.onChunkSpeed != nil {
				d.onChunkSpeed(chunk.Index, chunkSpeed, chunkSize)
			}

			stats.mu.Lock()
			stats.speeds = append(stats.speeds, chunkSpeed)
			stats.mu.Unlock()

			select {
			case progressCh <- downloaded:
			default:
			}
		}(chunks[i])
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		lastUpdate := time.Now()
		const updateInterval = 100 * time.Millisecond

		for downloaded := range progressCh {
			progress.Downloaded += downloaded
			elapsed := time.Since(startTime)
			progress.ElapsedTime = elapsed
			if elapsed.Seconds() > 0 {
				progress.Speed = float64(progress.Downloaded) / elapsed.Seconds()
			}
			progress.Percentage = float64(progress.Downloaded) / float64(progress.TotalSize) * 100

			if d.onProgress != nil {
				now := time.Now()
				if now.Sub(lastUpdate) >= updateInterval || progress.Percentage >= 100 {
					d.onProgress(progress)
					lastUpdate = now
				}
			}
		}
		if d.onProgress != nil && progress.Percentage < 100 {
			d.onProgress(progress)
		}
	}()

	wg.Wait()
	close(progressCh)
	<-done

	select {
	case err := <-errCh:
		return fmt.Errorf("failed to download chunks: %w", err)
	default:
		return nil
	}
}

func (d *Downloader) downloadChunk(ctx context.Context, url string, chunk Chunk) (int64, error) {
	expectedSize := chunk.End - chunk.Start + 1
	var startOffset int64 = 0
	var file *os.File
	var err error

	if d.resume {
		if info, err := os.Stat(chunk.FilePath); err == nil {
			existingSize := info.Size()
			if existingSize == expectedSize {
				return expectedSize, nil
			} else if existingSize > 0 && existingSize < expectedSize {
				startOffset = existingSize
				file, err = os.OpenFile(chunk.FilePath, os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					return 0, fmt.Errorf("failed to open chunk file for resume: %w", err)
				}
			} else {
				os.Remove(chunk.FilePath)
			}
		}
	}

	if file == nil {
		file, err = os.OpenFile(chunk.FilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return 0, err
		}
	}
	defer file.Close()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, err
	}
	d.setHeaders(req)

	actualStart := chunk.Start + startOffset
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", actualStart, chunk.End))

	// Force new connection if configured
	if d.forceNewConnection {
		req.Close = true
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP status code: %d", resp.StatusCode)
	}

	written, err := io.Copy(file, resp.Body)
	if err != nil {
		return 0, err
	}

	totalDownloaded := startOffset + written
	if totalDownloaded != expectedSize {
		return 0, fmt.Errorf("chunk size mismatch: expected %d, got %d", expectedSize, totalDownloaded)
	}

	return written, nil
}

func (d *Downloader) mergeChunks(chunks []Chunk, tempDir, destPath string) error {
	destFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer destFile.Close()

	buf := make([]byte, 32*1024)

	for _, chunk := range chunks {
		chunkFile, err := os.Open(chunk.FilePath)
		if err != nil {
			return err
		}

		if _, err := io.CopyBuffer(destFile, chunkFile, buf); err != nil {
			chunkFile.Close()
			return err
		}

		chunkFile.Close()
	}

	return nil
}

func (d *Downloader) verifyFile(filePath string, fileInfo *FileInfo) (string, error) {
	// If Content-MD5 is available, use it for verification
	if fileInfo.ContentMD5 != "" {
		md5Hash, err := d.calculateMD5(filePath)
		if err != nil {
			return "", fmt.Errorf("failed to calculate MD5: %w", err)
		}
		expectedMD5, err := d.decodeBase64MD5(fileInfo.ContentMD5)
		if err != nil {
			return "", fmt.Errorf("failed to decode Content-MD5: %w", err)
		}
		if md5Hash != expectedMD5 {
			os.Remove(filePath)
			return "", fmt.Errorf("Content-MD5 mismatch: expected %s, got %s", expectedMD5, md5Hash)
		}
		return filePath, nil
	}

	if fileInfo.HashETag != "" {
		if len(fileInfo.HashETag) == 32 && isHexString(fileInfo.HashETag) {
			md5Hash, err := d.calculateMD5(filePath)
			if err != nil {
				return "", fmt.Errorf("failed to calculate MD5: %w", err)
			}
			if md5Hash != fileInfo.HashETag {
				os.Remove(filePath)
				return "", fmt.Errorf("ETag mismatch: expected %s, got %s", fileInfo.HashETag, md5Hash)
			}
			return filePath, nil
		}
	}

	fileInfo2, err := os.Stat(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to stat file: %w", err)
	}

	if fileInfo.Size > 0 && fileInfo2.Size() != fileInfo.Size {
		os.Remove(filePath)
		return "", fmt.Errorf("file size mismatch: expected %d, got %d", fileInfo.Size, fileInfo2.Size())
	}

	return filePath, nil
}

func (d *Downloader) calculateMD5(filePath string) (string, error) {
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

func (d *Downloader) decodeBase64MD5(base64MD5 string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(base64MD5)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(decoded), nil
}
