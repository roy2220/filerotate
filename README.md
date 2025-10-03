# filerotate

[![Go Reference](https://pkg.go.dev/badge/github.com/roy2220/filerotate.svg)](https://pkg.go.dev/github.com/roy2220/filerotate)
[![Coverage](./.badges/coverage.svg)](#)

Go package **`filerotate`** provides an `io.WriteCloser` implementation that handles **time-based and size-based file rotation**, optionally combined with **intelligent buffering** for improved I/O performance.

-----

## Basic Usage

The core functionality is accessed through the `OpenFile` function, which returns an `io.WriteCloser` that automatically manages the underlying log file.

```go
package main

import (
    "fmt"
    "io"
    "time"

    "github.com/roy2220/filerotate"
)

func main() {
    options := filerotate.Options{
        // Mandatory: Use strftime format for substitution.
        // This pattern creates a new file every day, e.g., "logs/app-2023-10-02.log"
        FilePathPattern: "logs/app-%Y-%m-%d.log",

        // Optional: Rotate the file if it exceeds 100MB
        FileSizeLimit: 100 * 1024 * 1024,

        // Optional: Enable buffering (default 8MB)
        BufferSize: 8 * 1024 * 1024,
    }

    wc, err := filerotate.OpenFile(options)
    if err != nil {
        panic(err)
    }
    defer wc.Close()

    // Use it like any standard io.Writer
    for i := 0; i < 1000; i++ {
        _, err := wc.Write([]byte(fmt.Sprintf("Log entry %d at %s\n", i, time.Now().Format(time.RFC3339))))
        if err != nil {
            fmt.Println("Write error:", err)
            break
        }
        // In a real application, you might write less frequently or use a logging library.
        time.Sleep(10 * time.Millisecond)
    }
}
```

-----

## Configuration (`Options`)

The `filerotate.Options` struct controls both file rotation and I/O buffering.

### File Rotation Settings

| Option | Type | Description |
| :--- | :--- | :--- |
| **`FilePathPattern`** | `string` | **Mandatory.** The file naming pattern using strftime format (e.g., `"logs/app-%Y-%m-%d.log"`). Rotation occurs when the current time generates a different path. |
| **`FileSizeLimit`** | `int64` | The maximum size (in bytes) of a single file. Set to a non-positive value (e.g., `0`) to **disable size-based rotation**. Rotated files are appended with an index, e.g., `app-2023-10-02.log.1`, `app-2023-10-02.log.2`, etc. |

### Buffering Settings

If `BufferSize` is set to a negative value (e.g., `-1`), buffering is disabled, and writes go directly to the rotating file.

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| **`BufferSize`** | `int` | `8MB` | The internal buffer size in bytes. Set to **`-1` to disable buffering**. |
| **`FlushInterval`** | `time.Duration`| `1s` | How often the background routine automatically flushes the buffer. |
| **`LargeWriteThreshold`**| `float64`| $\approx 0.618$| A ratio (0.0 to 1.0) of `BufferSize`. If a single write is larger than this threshold, it **bypasses the buffer** and writes directly to the file to prevent blocking the buffer with a single large operation. |
| **`MaxIdleBufferAge`**| `time.Duration`| `3s` | The maximum time the buffer can be empty before the auto-flusher goroutine stops to conserve resources. It restarts on the next write. |
| **`LogFlushErr`** | `func(error)` | *(Silent)* | A function to handle errors that occur during the background automatic flushing. |

### Testing Hooks

The package provides hooks for dependency injection, useful for testing:

| Option | Type | Description |
| :--- | :--- | :--- |
| **`Clock`** | `clock.Clock` | An interface for time operations. Use to inject a mock clock. |
| **`Fs`** | `afero.Fs` | An interface for filesystem operations. Use to inject a mock filesystem (e.g., an in-memory FS). |
| **`Go`** | `func(func())` | A function to start background goroutines. Use to capture goroutines for deterministic testing. |

## License
MIT
