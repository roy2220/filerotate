# filerotate

[![Go Reference](https://pkg.go.dev/badge/github.com/roy2220/filerotate.svg)](https://pkg.go.dev/github.com/roy2220/filerotate)
[![Coverage](./.badges/coverage.svg)](#)

Go package **`filerotate`** provides an `io.WriteCloser` that handles **time-based and size-based file rotation**, optionally combined with a **self-managing write buffer**.

> [!NOTE]
> Except for the documentation, every line of code is human-crafted. **No AI slop included**.

---

## Features

* **Time-based rotation:** `strftime` patterns (e.g., `%Y-%m-%d`) decide the file name.
* **Size-based rotation:** Starts a new file once the size limit is reached, appending an index (`.1`, `.2`, ...).
* **Buffering:** Optional write buffer; its memory is released automatically when writes stop.
* **Resume after restart:** If files for the current period already exist (`app.log`, `app.log.1`, `app.log.2`, ...), the latest one is reopened in append mode instead of creating a new file.
* **Symbolic link:** Optionally points to the active file.
* **Newline handling:** Optionally appends `\n` to writes that lack one, optimized with `writev(2)` to avoid extra copying.

## Why a self-managing write buffer matters

Wrapping a `bufio.Writer` around a rotating file looks simple, but it breaks
down in one important case: when the buffer fills up, `bufio.Writer` may split a
single log line across two flushes. If a rotation happens in between, the head
of the line ends up in one file and the tail in the next.

The built-in buffer avoids this. It flushes whole writes only and checks for
rotation between them, so a log line is never cut into two files. It also
flushes periodically, is safe for concurrent use, and releases its memory when
idle.

---

## Usage

```go
package main

import (
    "fmt"
    "time"

    "github.com/roy2220/filerotate"
)

func main() {
    wc, err := filerotate.OpenFile(filerotate.Options{
        // Mandatory. One file per day, e.g. "logs/app/2023-10-02.log".
        FilePathPattern: "logs/app/%Y-%m-%d.log",

        // Optional: a symbolic link that always points to the active file
        // (by its absolute path).
        SymbolicLinkPath: "logs/app.log",

        // Optional: rotate once a file reaches 100MiB.
        FileSizeLimit: 100 * 1024 * 1024,

        // Optional: append '\n' to writes that lack one (optimized, no extra copying).
        EnsureNewline: true,

        // Optional: buffer size (default 1MiB; negative disables buffering).
        BufferSize: 256 * 1024,
    })
    if err != nil { /* Handle err */ }

    defer func() {
        // Close flushes buffered data, so its error matters.
        err := wc.Close()
        if err != nil { /* Handle err */ }
    }()

    for i := 0; i < 100; i++ {
        // No trailing "\n" needed: EnsureNewline adds it.
        _, err := fmt.Fprintf(wc, "Log entry %d at %s", i, time.Now().Format(time.RFC3339))
        if err != nil { /* Handle err */ }
    }
}
```

Always call `Close` when you are done: it flushes buffered data and releases
resources. A writer that is never closed may lose buffered data.

---

## Options

### General

| Option | Type | Description |
| :--- | :--- | :--- |
| `FilePathPattern` | `string` | **Mandatory.** `strftime` pattern for the file path. A new file is used whenever the rendered path changes. Missing directories are created automatically. |
| `SymbolicLinkPath` | `string` | Path of a symbolic link that always points to the active file. Empty disables it. |
| `FileSizeLimit` | `int64` | Size in bytes after which writes go to a new file. A file may slightly exceed it. Zero or negative disables size-based rotation. |
| `EnsureNewline` | `bool` | Append `\n` to each write unless it already ends with one. This costs almost nothing: the newline may be gathered with the data via `writev(2)`. |
| `LogInternalError` | `func(error)` | Called with errors that cannot be returned to the caller, such as background flush failures. Defaults to the standard logger. |

### Buffering

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `BufferSize` | `int` | `1MiB` | Buffer size in bytes. Negative disables buffering; zero uses the default. |
| `MinDirectWriteSize` | `int` | `64KiB` | Writes at least this large skip the buffer and go straight to the file. Zero or negative uses the default. |
| `FlushInterval` | `time.Duration` | `1s` | How often buffered data is flushed automatically. Zero or negative uses the default. |
| `MaxIdleBufferAge` | `int` | `3` | Number of consecutive idle flush intervals after which the buffer's memory is released. Zero or negative uses the default. |

---

## License

MIT
