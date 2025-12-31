package dlkit

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
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

const (
	defaultProgressUpdateInterval = 100 * time.Millisecond
	defaultChunkSize              = 10 * 1024 * 1024
	defaultConcurrency            = 5
	defaultHTTPTimeout            = 30 * time.Second
	defaultIdleConnTimeout        = 90 * time.Second
	defaultCopyBufferSize         = 256 * 1024
)

type Downloader struct {
	concurrency        int
	chunkSize          int64
	client             *http.Client
	onProgress         func(progress *Progress)
	resume             bool
	onChunkConfig      func(fileSize int64, currentChunkSize int64, currentConcurrency int) (chunkSize int64, concurrency int)
	onChunkSpeed       func(chunkIndex int, speed float64, chunkSize int64)
	tempDir            string
	defaultHeaders     map[string]string
	forceNewConnection bool // default: true, better performance for chunked downloads
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

func NewDownloader() *Downloader {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     defaultIdleConnTimeout,
		DisableKeepAlives:   false,
	}

	return &Downloader{
		concurrency:        defaultConcurrency,
		chunkSize:          defaultChunkSize,
		forceNewConnection: false,
		client: &http.Client{
			Transport: transport,
			Timeout:   defaultHTTPTimeout,
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

// Transport allows customizing the underlying HTTP transport for connection pooling/tuning.
func (d *Downloader) Transport(transport *http.Transport) *Downloader {
	if transport != nil {
		d.client.Transport = transport
	}
	return d
}

func (d *Downloader) ForceNewConnection(force bool) *Downloader {
	d.forceNewConnection = force
	return d
}

func (d *Downloader) checkExistingFile(filePath string, fileInfo *FileInfo) bool {
	if !d.resume {
		return false
	}

	info, err := os.Stat(filePath)
	if err != nil {
		return false
	}

	if fileInfo.Size > 0 && info.Size() != fileInfo.Size {
		return false
	}

	if fileInfo.ContentMD5 != "" || fileInfo.HashETag != "" {
		if _, err := d.verifyFile(filePath, fileInfo, ""); err == nil {
			return true
		}
		return false
	}

	if fileInfo.Size > 0 && info.Size() == fileInfo.Size {
		return true
	}

	return false
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

	result := &DownloadResult{
		FileInfo: fileInfo,
		TempPath: tempPath,
	}

	if d.checkExistingFile(tempPath, fileInfo) {
		result.FilePath = tempPath
		return result, nil
	}

	if !fileInfo.SupportsRange {
		md5Sum, err := d.downloadDirect(ctx, url, tempPath, fileInfo.Size, false, fileInfo)
		if err != nil {
			result.FilePath = tempPath
			return result, err
		}
		verifiedPath, err := d.verifyFile(tempPath, fileInfo, md5Sum)
		if err != nil {
			result.FilePath = tempPath
			return result, err
		}
		result.FilePath = verifiedPath
		return result, nil
	}

	chunkSize := d.chunkSize
	concurrency := d.concurrency
	if d.onChunkConfig != nil {
		chunkSize, concurrency = d.onChunkConfig(fileInfo.Size, d.chunkSize, d.concurrency)
	}

	expectedChunkCount := int((fileInfo.Size + chunkSize - 1) / chunkSize)
	if fileInfo.Size == 0 {
		expectedChunkCount = 1
	}

	shouldUseDirect := concurrency == 1 || expectedChunkCount <= 1

	if shouldUseDirect {
		md5Sum, err := d.downloadDirect(ctx, url, tempPath, fileInfo.Size, true, fileInfo)
		if err != nil {
			result.FilePath = tempPath
			return result, err
		}
		verifiedPath, err := d.verifyFile(tempPath, fileInfo, md5Sum)
		if err != nil {
			result.FilePath = tempPath
			return result, err
		}
		result.FilePath = verifiedPath
		return result, nil
	}

	tempDir := tempPath + ".tmp"
	result.TempDir = tempDir
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		result.FilePath = tempPath
		return result, fmt.Errorf("failed to create temp directory: %w", err)
	}

	chunks := d.calculateChunks(fileInfo.Size, chunkSize, tempDir)

	if concurrency > len(chunks) {
		concurrency = len(chunks)
	}

	var progress *Progress
	var startTime time.Time
	if d.onProgress != nil {
		progress = &Progress{
			TotalSize: fileInfo.Size,
		}
		startTime = time.Now()
	}

	if err := d.downloadChunks(ctx, url, chunks, progress, startTime, concurrency); err != nil {
		result.FilePath = tempPath
		return result, err
	}

	md5Sum, err := d.mergeChunks(chunks, tempPath, fileInfo)
	if err != nil {
		result.FilePath = tempPath
		return result, fmt.Errorf("failed to merge chunks: %w", err)
	}

	os.RemoveAll(tempDir)
	verifiedPath, err := d.verifyFile(tempPath, fileInfo, md5Sum)
	if err != nil {
		result.FilePath = tempPath
		return result, err
	}
	result.FilePath = verifiedPath
	result.TempDir = ""
	return result, nil
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

	// If HEAD request succeeds, try to get info from headers
	if resp.StatusCode == http.StatusOK {
		info := &FileInfo{
			SupportsRange: resp.Header.Get("Accept-Ranges") == "bytes",
			HashETag:      parseHashETag(resp.Header.Get("ETag")),
			ContentMD5:    resp.Header.Get("Content-MD5"),
		}

		if resp.ContentLength > 0 {
			info.Size = resp.ContentLength
			return info, nil
		}

		// If HEAD doesn't have Content-Length but supports Range, try Range request
		if info.SupportsRange {
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

			if resp.StatusCode == http.StatusPartialContent {
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
			}
		}

		return nil, fmt.Errorf("unable to determine file size: HEAD request didn't return Content-Length")
	}

	// HEAD request failed (e.g., 405 Method Not Allowed), try GET request
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

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP status code: %d", resp.StatusCode)
	}

	info := &FileInfo{
		SupportsRange: resp.Header.Get("Accept-Ranges") == "bytes",
		HashETag:      parseHashETag(resp.Header.Get("ETag")),
		ContentMD5:    resp.Header.Get("Content-MD5"),
	}

	if resp.StatusCode == http.StatusPartialContent {
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
	}

	if resp.ContentLength > 0 {
		info.Size = resp.ContentLength
		return info, nil
	}

	return nil, fmt.Errorf("unable to determine file size from GET Range probe")
}

func shouldCheckMD5(info *FileInfo) bool {
	if info == nil {
		return false
	}
	if info.ContentMD5 != "" {
		return true
	}
	if len(info.HashETag) == 32 && isHexString(info.HashETag) {
		return true
	}
	return false
}

func (d *Downloader) downloadDirect(ctx context.Context, url, destPath string, fileSize int64, supportsRange bool, fileInfo *FileInfo) (string, error) {
	var startOffset int64 = 0
	var file *os.File
	var err error

	if d.resume && supportsRange {
		if info, err := os.Stat(destPath); err == nil {
			existingSize := info.Size()
			if existingSize == fileSize {
				return "", nil
			} else if existingSize > 0 && existingSize < fileSize {
				startOffset = existingSize
			}
		}
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}
	d.setHeaders(req)

	if startOffset > 0 && supportsRange {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", startOffset))
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if startOffset > 0 && resp.StatusCode != http.StatusPartialContent {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		os.Remove(destPath)
		startOffset = 0
		req, err = http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return "", err
		}
		d.setHeaders(req)
		resp, err = d.client.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return "", fmt.Errorf("HTTP status code: %d", resp.StatusCode)
	}

	if startOffset > 0 {
		file, err = os.OpenFile(destPath, os.O_WRONLY|os.O_APPEND, 0644)
	} else {
		file, err = os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	}
	if err != nil {
		return "", err
	}
	defer file.Close()

	var reader io.Reader = resp.Body
	if d.onProgress != nil {
		progress := &Progress{
			TotalSize:  fileSize,
			Downloaded: startOffset,
		}
		startTime := time.Now()
		downloaded := startOffset

		reader = &progressReader{
			reader:     resp.Body,
			progress:   progress,
			downloaded: &downloaded,
			onProgress: d.onProgress,
			startTime:  startTime,
		}
	}

	var hasher hash.Hash
	var writer io.Writer = file
	if startOffset == 0 && shouldCheckMD5(fileInfo) {
		hasher = md5.New()
		writer = io.MultiWriter(file, hasher)
	}

	buf := make([]byte, defaultCopyBufferSize)
	_, err = io.CopyBuffer(writer, reader, buf)
	if err != nil {
		return "", err
	}

	if d.onProgress != nil {
		if pr, ok := reader.(*progressReader); ok {
			now := time.Now()
			if shouldUpdateProgress(now, pr.lastUpdate, pr.progress.Percentage) {
				updateProgress(pr.progress, pr.progress.TotalSize, pr.startTime)
				pr.lastUpdate = now
				d.onProgress(pr.progress)
			}
		}
	}

	if hasher != nil {
		return hex.EncodeToString(hasher.Sum(nil)), nil
	}
	return "", nil
}

func updateProgress(progress *Progress, downloaded int64, startTime time.Time) {
	progress.Downloaded = downloaded
	elapsed := time.Since(startTime)
	progress.ElapsedTime = elapsed
	if elapsed.Seconds() > 0 {
		progress.Speed = float64(downloaded) / elapsed.Seconds()
	}
	if progress.TotalSize > 0 {
		progress.Percentage = float64(downloaded) / float64(progress.TotalSize) * 100
	} else {
		progress.Percentage = 100
	}
}

func shouldUpdateProgress(now time.Time, lastUpdate time.Time, percentage float64) bool {
	return percentage < 100 || now.Sub(lastUpdate) >= defaultProgressUpdateInterval
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
	if n > 0 && pr.onProgress != nil {
		downloaded := atomic.AddInt64(pr.downloaded, int64(n))
		pr.progress.Downloaded = downloaded

		now := time.Now()
		if !shouldUpdateProgress(now, pr.lastUpdate, pr.progress.Percentage) && err == nil {
			return n, err
		}
		pr.lastUpdate = now

		updateProgress(pr.progress, downloaded, pr.startTime)
		pr.onProgress(pr.progress)
	}
	return n, err
}

func (d *Downloader) calculateChunks(fileSize, chunkSize int64, tempDir string) []Chunk {
	if fileSize == 0 {
		return []Chunk{
			{
				Index:    0,
				Start:    0,
				End:      -1,
				FilePath: filepath.Join(tempDir, "chunk_0"),
			},
		}
	}

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

func (d *Downloader) downloadChunks(ctx context.Context, url string, chunks []Chunk, progress *Progress, startTime time.Time, concurrency int) error {
	semaphore := make(chan struct{}, concurrency)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, len(chunks))
	var progressCh chan int64
	if progress != nil {
		progressCh = make(chan int64, len(chunks))
	}

	for i := range chunks {
		chunk := chunks[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-semaphore }()

			chunkStartTime := time.Now()
			downloaded, err := d.downloadChunk(ctx, url, chunk)
			chunkElapsed := time.Since(chunkStartTime)
			if err != nil {
				errCh <- err
				cancel()
				return
			}

			chunkSize := chunk.End - chunk.Start + 1
			if d.onChunkSpeed != nil {
				var chunkSpeed float64
				if chunkElapsed.Seconds() > 0 {
					chunkSpeed = float64(chunkSize) / chunkElapsed.Seconds()
				}
				d.onChunkSpeed(chunk.Index, chunkSpeed, chunkSize)
			}

			if progress != nil {
				select {
				case progressCh <- downloaded:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	var done chan struct{}
	if progress != nil {
		done = make(chan struct{})
		go func() {
			defer close(done)
			lastUpdate := time.Now()

			for {
				select {
				case downloaded, ok := <-progressCh:
					if !ok {
						now := time.Now()
						if shouldUpdateProgress(now, lastUpdate, progress.Percentage) {
							d.onProgress(progress)
						}
						return
					}
					progress.Downloaded += downloaded
					updateProgress(progress, progress.Downloaded, startTime)

					now := time.Now()
					if shouldUpdateProgress(now, lastUpdate, progress.Percentage) {
						d.onProgress(progress)
						lastUpdate = now
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	wg.Wait()

	if progress != nil {
		close(progressCh)
		<-done
	}

	var errs []error
	for {
		select {
		case err := <-errCh:
			errs = append(errs, err)
		default:
			if len(errs) == 0 {
				return nil
			}
			if len(errs) == 1 {
				return errs[0]
			}
			return fmt.Errorf("failed to download %d chunks: %v", len(errs), errs)
		}
	}
}

func (d *Downloader) downloadChunk(ctx context.Context, url string, chunk Chunk) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if chunk.End < chunk.Start {
		if d.resume {
			if info, err := os.Stat(chunk.FilePath); err == nil {
				if info.Size() == 0 {
					return 0, nil
				}
				os.Remove(chunk.FilePath)
			}
		}
		file, err := os.OpenFile(chunk.FilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return 0, err
		}
		file.Close()
		return 0, nil
	}

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

	if d.forceNewConnection {
		req.Close = true
	}

	resp, err := d.client.Do(req)
	if err != nil {
		os.Remove(chunk.FilePath)
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		os.Remove(chunk.FilePath)
		return 0, fmt.Errorf("server does not support Range requests for chunked download")
	}

	if resp.StatusCode != http.StatusPartialContent {
		os.Remove(chunk.FilePath)
		return 0, fmt.Errorf("HTTP status code: %d", resp.StatusCode)
	}

	buf := make([]byte, defaultCopyBufferSize)
	written, err := io.CopyBuffer(file, resp.Body, buf)
	if err != nil {
		os.Remove(chunk.FilePath)
		return 0, err
	}

	totalDownloaded := startOffset + written
	if totalDownloaded != expectedSize {
		os.Remove(chunk.FilePath)
		return 0, fmt.Errorf("chunk size mismatch: expected %d, got %d", expectedSize, totalDownloaded)
	}

	return written, nil
}

func (d *Downloader) mergeChunks(chunks []Chunk, destPath string, fileInfo *FileInfo) (string, error) {
	destFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}
	defer destFile.Close()

	var hasher hash.Hash
	var writer io.Writer = destFile
	if shouldCheckMD5(fileInfo) {
		hasher = md5.New()
		writer = io.MultiWriter(destFile, hasher)
	}

	buf := make([]byte, defaultCopyBufferSize)

	for _, chunk := range chunks {
		chunkFile, err := os.Open(chunk.FilePath)
		if err != nil {
			return "", err
		}

		if _, err := io.CopyBuffer(writer, chunkFile, buf); err != nil {
			chunkFile.Close()
			return "", err
		}

		chunkFile.Close()
	}

	if err := destFile.Sync(); err != nil {
		return "", err
	}
	if hasher != nil {
		return hex.EncodeToString(hasher.Sum(nil)), nil
	}
	return "", nil
}

func (d *Downloader) verifyFile(filePath string, fileInfo *FileInfo, precomputedMD5 string) (string, error) {
	if fileInfo == nil {
		return filePath, nil
	}

	if fileInfo.ContentMD5 != "" {
		md5Hash := precomputedMD5
		var err error
		if md5Hash == "" {
			md5Hash, err = md5File(filePath)
			if err != nil {
				return "", fmt.Errorf("failed to calculate MD5: %w", err)
			}
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
			md5Hash := precomputedMD5
			var err error
			if md5Hash == "" {
				md5Hash, err = md5File(filePath)
				if err != nil {
					return "", fmt.Errorf("failed to calculate MD5: %w", err)
				}
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

func (d *Downloader) decodeBase64MD5(base64MD5 string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(base64MD5)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(decoded), nil
}
