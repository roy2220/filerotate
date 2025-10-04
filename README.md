# filerotate

[![Go Reference](https://pkg.go.dev/badge/github.com/roy2220/filerotate.svg)](https://pkg.go.dev/github.com/roy2220/filerotate)
[![Coverage](./.badges/coverage.svg)](#)

Go package **`filerotate`** provides an `io.WriteCloser` implementation that handles **time-based and size-based file rotation**, optionally combined with a **self-managing, high-performance write buffer**.

---

## Features

* **Time-Based Rotation:** Uses `strftime` patterns (e.g., `%Y-%m-%d`) to automatically rotate files based on time.
* **Size-Based Rotation:** Rotates files when they exceed a specified size limit, appending an index (e.g., `.1`, `.2`).
* **High-Performance Buffering:** An optional, self-tuning write buffer that can automatically free memory when idle.
* **Symbolic Link Tracking:** Optionally maintains a symbolic link that always points to the currently active log file.

---

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
        // This pattern creates a new file every day, e.g., "logs/app/2023-10-02.log"
        FilePathPattern: "logs/app/%Y-%m-%d.log",

        // Optional: Maintain a symbolic link to the currently active log file.
        SymbolicLinkPath: "logs/app.log",

        // Optional: Rotate the file if it exceeds 100MB
        FileSizeLimit: 100 * 1024 * 1024,

        // Optional: Enable buffering (default 8MB for 0 value)
        // Set to a negative value (e.g., -1) to disable buffering.
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
| **`FilePathPattern`** | `string` | **Mandatory.** The file naming pattern using strftime format (e.g., `"logs/app/%Y-%m-%d.log"`). Time-based rotation occurs when the current time generates a different path. |
| **`SymbolicLinkPath`** | `string` | **Optional.** The path to a symbolic link that will always point to the most recently active log file. Leave empty to disable. |
| **`FileSizeLimit`** | `int64` | The maximum size (in bytes) of a single file. Set to a non-positive value (e.g., `0`) to **disable size-based rotation**. Rotated files are appended with an index, e.g., `app-2023-10-02.log.1`, `app-2023-10-02.log.2`, etc. |

### Buffering Settings

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| **`BufferSize`** | `int` | `8MB` | The internal buffer size in bytes. Set to a **negative value (e.g., `-1`) to disable buffering**. |
| **`LargeWriteThreshold`**| `float64`| $\approx 0.618$ | A ratio (0.0 to 1.0) of `BufferSize`. If a single write is larger than the calculated threshold, it **bypasses the buffer** and writes directly to the file. This prevents a single large operation from blocking the buffer. |
| **`FlushInterval`**| `time.Duration`| `1s` | How often the background routine automatically flushes the buffer to the file. |
| **`IdleBufferTimeout`**| `time.Duration`| `3s` | The maximum time the buffer can be idle (no new writes) before the auto-flusher goroutine stops and **releases the buffer's memory** (`nil`s the internal slice) to conserve resources. It restarts automatically on the next write. |
| **`LogInternalError`**| `func(error)` | *(Logs to standard output)* | A callback function to handle errors that occur during background operations (e.g., auto-flushing failures, file close errors). If `nil`, a default logger that writes to standard output is used. |

### Testing Hooks

The package provides hooks for dependency injection, useful for deterministic testing:

| Option | Type | Description |
| :--- | :--- | :--- |
| **`Clock`** | `clock.Clock` | An interface for time operations. Use to inject a mock clock. |
| **`Fs`** | `afero.Fs` | An interface for filesystem operations. Use to inject a mock filesystem (e.g., an in-memory FS). |
| **`Go`** | `func(func())` | A function to start background goroutines. Use to capture goroutines for deterministic testing. |

-----

## License

MIT
