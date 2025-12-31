package dlkit

import (
	"fmt"
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

func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) - (minutes * 60)
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) - (hours * 60)
	seconds := int(d.Seconds()) - (int(d.Minutes()) * 60)
	return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
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
