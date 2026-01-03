package dlkit_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lsgox/dlkit"
)

func doDownload(t *testing.T, d *dlkit.Downloader, ctx context.Context, url, dst string) error {
	result, err := d.Download(ctx, url)
	if err != nil {
		return err
	}
	return result.SaveTo(dst)
}

func makeData(size int) []byte {
	b := make([]byte, size)
	_, _ = rand.Read(b)
	return b
}

func rangeServer(t *testing.T, data []byte) *httptest.Server {
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))

		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			http.ServeContent(w, r, "file.bin", time.Now(), bytes.NewReader(data))
			return
		}
		var start, end int
		n, err := fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
		if err != nil || n < 1 {
			fmt.Sscanf(rangeHdr, "bytes=%d-", &start)
			end = len(data) - 1
		}
		if end <= 0 || end >= len(data) {
			end = len(data) - 1
		}
		if start >= len(data) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if start == 0 && end == len(data)-1 {
			http.ServeContent(w, r, "file.bin", time.Now(), bytes.NewReader(data))
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		_, err = w.Write(data[start : end+1])
		if err != nil {
			t.Logf("rangeServer write error: %v", err)
		}
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}

func noRangeServer(t *testing.T, data []byte) *httptest.Server {
	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		_, _ = w.Write(data)
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}

func headFailServer(t *testing.T, data []byte) *httptest.Server {
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		_, _ = w.Write(data)
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}

func TestDownload_WithRangeSupport(t *testing.T) {
	data := makeData(512 * 1024) // 512KB
	srv := rangeServer(t, data)
	defer srv.Close()

	d := dlkit.NewDownloader()
	d.Workers(4)

	ctx := context.Background()
	dst := filepath.Join(t.TempDir(), "out_range.bin")
	if err := doDownload(t, d, ctx, srv.URL+"/file.bin", dst); err != nil {
		t.Fatalf("download failed: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read result failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("downloaded content mismatch: %d vs %d", len(got), len(data))
	}
}

func TestDownload_NoRangeServer(t *testing.T) {
	data := makeData(128 * 1024) // 128KB
	srv := noRangeServer(t, data)
	defer srv.Close()

	d := dlkit.NewDownloader()
	d.Workers(2)

	ctx := context.Background()
	dst := filepath.Join(t.TempDir(), "out_norange.bin")
	if err := doDownload(t, d, ctx, srv.URL+"/file.bin", dst); err != nil {
		t.Fatalf("download failed: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read result failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("downloaded content mismatch: %d vs %d", len(got), len(data))
	}
}

func TestDownload_HEADFailsFallbackToGET(t *testing.T) {
	data := makeData(64 * 1024) // 64KB
	srv := headFailServer(t, data)
	defer srv.Close()

	d := dlkit.NewDownloader()
	ctx := context.Background()
	dst := filepath.Join(t.TempDir(), "out_headfail.bin")
	if err := doDownload(t, d, ctx, srv.URL+"/file.bin", dst); err != nil {
		t.Fatalf("download failed: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read result failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("downloaded content mismatch: %d vs %d", len(got), len(data))
	}
}
