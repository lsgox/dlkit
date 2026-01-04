# dlkit

Go 语言下载库，支持多线程并发下载、断点续传、文件校验等功能。

## 特性

- **多线程并发下载**：支持自定义并发数
- **断点续传**：支持从断点继续下载
- **分块下载**：支持 Range 请求
- **文件校验**：支持 MD5 和 ETag 校验
- **进度监控**：支持进度回调
- **自适应分块**：根据文件大小自动调整分块策略
- **自定义配置**：支持自定义 HTTP 客户端、临时目录等

## 安装

```bash
go get github.com/lsgox/dlkit
```

## 快速开始

```go
package main

import (
    "context"
    "log"
    "github.com/lsgox/dlkit"
)

func main() {
    dl := dlkit.NewDownloader()
    dl.Workers(5)
    dl.Chunk(10 * 1024 * 1024)
    dl.EnableResume(true)
    
    dl.Progress(func(p *dlkit.Progress) {
        log.Printf("进度: %.2f%%, 速度: %s/s, 已下载: %s/%s",
            p.Percentage,
            dlkit.FormatSpeed(p.Speed),
            dlkit.FormatBytes(p.Downloaded),
            dlkit.FormatBytes(p.TotalSize))
    })
    
    ctx := context.Background()
    result, err := dl.Download(ctx, "https://example.com/file.zip")
    if err != nil {
        log.Fatal(err)
    }
    defer result.CleanupTempFiles()
    
    if err := result.SaveTo("./downloaded-file.zip"); err != nil {
        log.Fatal(err)
    }
}
```

## API 文档

### Downloader

#### 创建下载器

```go
dl := dlkit.NewDownloader()
```

#### 配置方法

支持链式调用：

```go
dl := dlkit.NewDownloader().
    Workers(5).
    Chunk(10 * 1024 * 1024).
    EnableResume(true).
    Temp("/tmp/dlkit").
    ForceNewConnection(false).
    HTTPClient(customClient).
    Transport(customTransport)
```

#### 回调函数

```go
// 进度回调
dl.Progress(func(p *dlkit.Progress) {
    // p.TotalSize: 总大小
    // p.Downloaded: 已下载大小
    // p.Percentage: 完成百分比
    // p.Speed: 下载速度（字节/秒）
    // p.ElapsedTime: 已用时间
})

// 文件信息回调
dl.OnFileInfo(func(info *dlkit.FileInfo) error {
    return nil
})

// 分块进度回调
dl.ChunkProgress(func(index int, progress *dlkit.Progress) {})

// 自适应分块配置
dl.AdaptiveChunk(dlkit.AdaptiveChunk(dlkit.DefaultChunkConfig()))
```

#### 下载

```go
result, err := dl.Download(ctx, "https://example.com/file.zip")
if err != nil {
    // 处理错误
}
```

### DownloadResult

```go
type DownloadResult struct {
    FilePath string      // 下载文件的路径
    FileInfo *FileInfo   // 文件信息
    TempPath string      // 临时文件路径
    TempDir  string      // 临时目录
}

// 保存到指定路径
err := result.SaveTo("./output.zip")

// 清理临时文件
err := result.CleanupTempFiles()

// 清理所有文件（包括下载的文件）
err := result.Cleanup()
```

### 工具函数

```go
// 格式化字节数
dlkit.FormatBytes(1024)  // "1.00 KB"

// 格式化速度
dlkit.FormatSpeed(1024.0)  // "1.00 KB/s"

// 估算剩余时间
eta := dlkit.EstimateRemainingTime(progress)

// 检查错误类型
if dlkit.IsStatusCodeError(err) {
    // HTTP 状态码错误
}
if dlkit.IsChecksumMismatch(err) {
    // 校验和不匹配
}
if dlkit.IsFileSizeMismatch(err) {
    // 文件大小不匹配
}
```

## 高级用法

### 自适应分块

```go
config := &dlkit.ChunkConfig{
    MinChunkSize:       1 * 1024 * 1024,
    MaxChunkSize:       10 * 1024 * 1024,
    OptimalConcurrency: 5,
    MinConcurrency:     4,
    MaxConcurrency:     10,
    ChunksPerWorker:    2,
}

dl.AdaptiveChunk(dlkit.AdaptiveChunk(config))
```

### 自定义 HTTP 客户端

```go
client := &http.Client{
    Timeout: 60 * time.Second,
    Transport: &http.Transport{
        MaxIdleConns:        100,
        MaxIdleConnsPerHost: 50,
        IdleConnTimeout:     90 * time.Second,
    },
}

dl.HTTPClient(client)
```

### 文件校验

服务器返回 MD5 或 ETag 时会自动校验：

```go
dl.OnFileInfo(func(info *dlkit.FileInfo) error {
    return nil
})
```

## 注意事项

1. **断点续传**
   - 需要服务器支持 Range 请求（`Accept-Ranges: bytes`），否则降级为单线程下载
   - 如果本地文件被修改过，断点续传可能失败，建议删除已存在的文件重新下载

2. **并发数**
   - 默认 5 个并发，可根据实际情况调整
   - 并发数过多可能导致服务器限流
   - 设置为 1 时禁用分块下载

3. **分块大小**
   - 默认 10MB，分块过小影响效率，过大增加内存占用

4. **临时文件**
   - 下载完成后需调用 `CleanupTempFiles()` 清理
   - 建议使用 `defer result.CleanupTempFiles()`

5. **文件校验**
   - 服务器提供 MD5 或 ETag 时自动校验
   - 校验失败返回 `ErrChecksumMismatch`，文件大小不匹配返回 `ErrFileSizeMismatch`

6. **错误处理**
   - HTTP 状态码错误返回 `StatusCodeError`
   - 建议检查错误类型并做相应处理

## 许可证

查看 [LICENSE](LICENSE) 文件了解详情。
