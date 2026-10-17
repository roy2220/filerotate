package filerotate_test

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/roy2220/filerotate"
	"github.com/stretchr/testify/require"
)

func openTempFile(t testing.TB, options Options) (io.WriteCloser, string) {
	tempDir := t.TempDir()
	if options.FilePathPattern != "" {
		options.FilePathPattern = filepath.Join(tempDir, options.FilePathPattern)
	}
	if options.SymbolicLinkPath != "" {
		options.SymbolicLinkPath = filepath.Join(tempDir, options.SymbolicLinkPath)
	}
	if options.Registry() == nil {
		options.SetRegistry(new(sync.Map))
	}
	wc, err := OpenFile(options)
	require.NoError(t, err)
	t.Cleanup(func() { wc.Close() })
	return wc, tempDir
}

func openFileInDir(t testing.TB, options Options, dir string) io.WriteCloser {
	if options.FilePathPattern != "" {
		options.FilePathPattern = filepath.Join(dir, options.FilePathPattern)
	}
	if options.SymbolicLinkPath != "" {
		options.SymbolicLinkPath = filepath.Join(dir, options.SymbolicLinkPath)
	}
	if options.Registry() == nil {
		options.SetRegistry(new(sync.Map))
	}
	wc, err := OpenFile(options)
	require.NoError(t, err)
	t.Cleanup(func() { wc.Close() })
	return wc
}

func lookupWriteCloser(wc io.WriteCloser, registry *sync.Map) bool {
	found := false
	registry.Range(func(key, _ any) bool {
		found = key == wc
		return !found
	})
	return found
}

func countWriteClosers(registry *sync.Map) int {
	n := 0
	registry.Range(func(key, _ any) bool {
		n++
		return true
	})
	return n
}

func fillRandomData(b []byte) {
	var seed []byte
	for range 4 {
		seed = binary.BigEndian.AppendUint64(seed, uint64(time.Now().UnixNano()))
	}
	io.ReadFull(rand.NewChaCha8([32]byte(seed)), b)
}

func resetFileOffset(t testing.TB, wc io.WriteCloser) {
	var f *os.File
	switch wc := wc.(type) {
	case *WriteCloser:
		f = wc.File()
	case *BufferedWriteCloser:
		f = wc.File()
	default:
		t.Fatal("unreachable")
	}
	_, err := f.Seek(0, io.SeekStart)
	require.NoError(t, err)
}

func TestOptions_ApplyDefaults(t *testing.T) {
	t.Run("apply defaults for zero value", func(t *testing.T) {
		var options Options
		options.ApplyDefaults()
		require.Equal(t, 1024*1024, options.BufferSize)
		require.Equal(t, 64*1024, options.MinDirectWriteSize)
		require.Equal(t, 1*time.Second, options.FlushInterval)
		require.Equal(t, 3, options.MaxIdleBufferAge)
	})

	t.Run("keep set values", func(t *testing.T) {
		options := Options{
			BufferSize:         1024*1024 + 1,
			MinDirectWriteSize: 64*1024 + 2,
			FlushInterval:      1*time.Second + 3,
			MaxIdleBufferAge:   3 + 4,
		}
		options.ApplyDefaults()
		require.Equal(t, 1024*1024+1, options.BufferSize)
		require.Equal(t, 64*1024+2, options.MinDirectWriteSize)
		require.Equal(t, 1*time.Second+3, options.FlushInterval)
		require.Equal(t, 3+4, options.MaxIdleBufferAge)
	})
}

func TestOpenFileFailed(t *testing.T) {
	_, err := OpenFile(Options{})
	require.ErrorContains(t, err, "no file path pattern")

	_, err = OpenFile(Options{FilePathPattern: "%0"})
	require.ErrorContains(t, err, "invalid file path pattern")
}

func TestWriteCloser_Write(t *testing.T) {
	t.Run("write empty data", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern: "app-%Y-%m-%d_%H-%M.log",
				BufferSize:      -1,
			})

			n, err := wc.Write([]byte(""))
			require.NoError(t, err)
			require.Equal(t, 0, n)

			filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
			require.FileExists(t, filePath)
		})
	})

	t.Run("rotate file for time", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern: "app-%Y-%m-%d_%H-%M.log",
				BufferSize:      -1,
			})

			for i := range 10 {
				n, err := wc.Write([]byte("hello world\n"))
				require.NoError(t, err)
				require.Equal(t, 12, n)

				filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
				data, err := os.ReadFile(filePath)
				require.NoError(t, err)

				expectedData := strings.Repeat("hello world\n", 1+(i%3))
				require.Equalf(t, expectedData, string(data), "i=%d", i)

				time.Sleep(20 * time.Second)
			}
		})
	})

	t.Run("rotate file for size", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern: "app-%Y-%m-%d_%H-%M.log",
				FileSizeLimit:   30,
				BufferSize:      -1,
			})

			for i := range 10 {
				n, err := wc.Write([]byte("hello world\n"))
				require.NoError(t, err)
				require.Equal(t, 12, n)

				filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
				if n := i / 3; n >= 1 {
					filePath += "." + strconv.Itoa(n)
				}
				data, err := os.ReadFile(filePath)
				require.NoError(t, err)

				expectedData := strings.Repeat("hello world\n", 1+(i%3))
				require.Equalf(t, expectedData, string(data), "i=%d", i)
			}
		})
	})

	t.Run("update symbolic link", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern:  "foo/app-%Y-%m-%d_%H-%M.log",
				SymbolicLinkPath: "foo/app.log",
				BufferSize:       -1,
			})

			for i := range 10 {
				n, err := wc.Write([]byte("hello world\n"))
				require.NoError(t, err)
				require.Equal(t, 12, n)

				filePath, err := os.Readlink(filepath.Join(tempDir, "foo/app.log"))
				require.NoError(t, err)

				expectedFilePath := filepath.Join(tempDir, time.Now().Format("foo/app-2006-01-02_15-04.log"))
				require.Equalf(t, expectedFilePath, filePath, "i=%d", i)

				time.Sleep(20 * time.Second)
			}
		})
	})

	t.Run("ensure newline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern: "app-%Y-%m-%d_%H-%M.log",
				EnsureNewline:   true,
				BufferSize:      -1,
			})

			for range 3 {
				n, err := wc.Write([]byte(""))
				require.NoError(t, err)
				require.Equal(t, 0, n)

				n, err = wc.Write([]byte("hello world"))
				require.NoError(t, err)
				require.Equal(t, 11, n)
			}

			filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
			data, err := os.ReadFile(filePath)
			require.NoError(t, err)

			expectedData := strings.Repeat("\nhello world\n", 3)
			require.Equal(t, expectedData, string(data))
		})
	})

	t.Run("open latest file", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern: "app-%Y-%m-%d_%H-%M.log",
				BufferSize:      -1,
			})

			for i := range 3 {
				filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
				if i >= 1 {
					filePath += "." + strconv.Itoa(i)
				}
				err := os.WriteFile(filePath, []byte("test\n"), 0644)
				require.NoError(t, err)
			}

			n, err := wc.Write([]byte("hello world\n"))
			require.NoError(t, err)
			require.Equal(t, 12, n)

			filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log")+".2")
			data, err := os.ReadFile(filePath)
			require.NoError(t, err)
			require.Equal(t, "test\nhello world\n", string(data))
		})
	})

	t.Run("rotate file for size after existing files", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern: "app-%Y-%m-%d_%H-%M.log",
				FileSizeLimit:   10,
				BufferSize:      -1,
			})

			for i := range 3 {
				filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
				filePath += "." + strconv.Itoa(1+i)
				err := os.WriteFile(filePath, []byte("test\n"), 0644)
				require.NoError(t, err)
			}

			for i := range 2 {
				_, err := fmt.Fprintf(wc, "hello world %d\n", 1+i)
				require.NoError(t, err)
			}

			filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log")+".4")
			data, err := os.ReadFile(filePath)
			require.NoError(t, err)
			require.Equal(t, "hello world 2\n", string(data))
		})
	})

	t.Run("rotate file for time after existing files", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern: "app-%Y-%m-%d_%H-%M.log",
				BufferSize:      -1,
			})

			for i := range 3 {
				filePath := filepath.Join(tempDir, time.Now().Add(time.Minute).Format("app-2006-01-02_15-04.log"))
				if i >= 1 {
					filePath += "." + strconv.Itoa(i)
				}
				err := os.WriteFile(filePath, []byte("test\n"), 0644)
				require.NoError(t, err)
			}

			for i := range 2 {
				_, err := fmt.Fprintf(wc, "hello world %d\n", 1+i)
				require.NoError(t, err)
				if i == 0 {
					time.Sleep(time.Minute)
				}
			}

			filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log")+".3")
			data, err := os.ReadFile(filePath)
			require.NoError(t, err)
			require.Equal(t, "hello world 2\n", string(data))
		})
	})

	t.Run("write after close", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, _ := openTempFile(t, Options{
				FilePathPattern: "app-%Y-%m-%d_%H-%M.log",
				BufferSize:      -1,
			})

			err := wc.Close()
			require.NoError(t, err)

			n, err := wc.Write([]byte("test"))
			require.ErrorContains(t, err, "closed")
			require.Equal(t, 0, n)
		})
	})
}

func TestWriteCloser_Close(t *testing.T) {
	t.Run("close without open file", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			registry := new(sync.Map)

			wc, _ := openTempFile(t, *(&Options{
				FilePathPattern: "app-%Y-%m-%d_%H.log",
				BufferSize:      -1,
			}).SetRegistry(registry))

			require.True(t, lookupWriteCloser(wc, registry))

			err := wc.Close()
			require.NoError(t, err)

			require.False(t, lookupWriteCloser(wc, registry))

			err = wc.Close()
			require.ErrorContains(t, err, "closed")
		})
	})

	t.Run("close open file", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			registry := new(sync.Map)

			wc, _ := openTempFile(t, *(&Options{
				FilePathPattern: "app-%Y-%m-%d_%H.log",
				BufferSize:      -1,
			}).SetRegistry(registry))

			_, err := wc.Write([]byte("test\n"))
			require.NoError(t, err)

			file := wc.(*WriteCloser).File()
			require.NotNil(t, file)

			err = wc.Close()
			require.NoError(t, err)

			err = file.Close()
			require.ErrorContains(t, err, "file already closed")
		})
	})
}

func TestRunOutdatedFileCloser(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := new(sync.Map)

		wc, tempDir := openTempFile(t, *(&Options{
			FilePathPattern: "app-%Y-%m-%d_%H.log",
			BufferSize:      -1,
		}).SetRegistry(registry))

		go func() {
			RunOutdatedFileCloser(t.Context(), registry)
		}()

		n, err := wc.Write([]byte("hello world\n"))
		require.NoError(t, err)
		require.Equal(t, 12, n)

		filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15.log"))
		require.Equal(t, filePath, wc.(*WriteCloser).File().Name())

		time.Sleep(59 * time.Minute)
		require.Equal(t, filePath, wc.(*WriteCloser).File().Name())

		time.Sleep(time.Minute)
		synctest.Wait()
		require.Nil(t, wc.(*WriteCloser).File())

		time.Sleep(3 * time.Minute)
	})
}

func TestBufferedWriteCloser_Write(t *testing.T) {
	t.Run("write empty data", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern: "app-%Y-%m-%d_%H-%M.log",
				FlushInterval:   5 * time.Second,
			})

			n, err := wc.Write([]byte(""))
			require.NoError(t, err)
			require.Equal(t, 0, n)

			filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
			require.NoFileExists(t, filePath)

			time.Sleep(5 * time.Second)
			synctest.Wait()

			require.FileExists(t, filePath)
		})
	})

	t.Run("direct write bypasses buffer #1", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern:    "app-%Y-%m-%d_%H-%M.log",
				BufferSize:         100,
				MinDirectWriteSize: 10,
				FlushInterval:      time.Hour,
			})

			for i := range 3 {
				p := append([]byte("123456789AB")[:8+i], '\n')
				n, err := wc.Write(p)
				require.NoError(t, err)
				require.Equal(t, len(p), n)

				filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
				if i == 0 {
					require.NoFileExists(t, filePath)
				} else {
					data, err := os.ReadFile(filePath)
					require.NoError(t, err)
					if i == 1 {
						require.Equal(t, "12345678\n123456789\n", string(data))
					} else {
						require.Equal(t, "12345678\n123456789\n123456789A\n", string(data))
					}
				}

			}
		})
	})

	t.Run("direct write bypasses buffer #2", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern:    "app-%Y-%m-%d_%H-%M.log",
				BufferSize:         10,
				MinDirectWriteSize: 100,
				FlushInterval:      time.Hour,
			})

			for i := range 3 {
				p := append([]byte("123456789AB")[:8+i], '\n')
				n, err := wc.Write(p)
				require.NoError(t, err)
				require.Equal(t, len(p), n)

				filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
				if i == 0 {
					require.NoFileExists(t, filePath)
				} else {
					data, err := os.ReadFile(filePath)
					require.NoError(t, err)
					if i == 1 {
						require.Equal(t, "12345678\n123456789\n", string(data))
					} else {
						require.Equal(t, "12345678\n123456789\n123456789A\n", string(data))
					}
				}

			}
		})
	})

	t.Run("flush buffer when full", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern:    "app-%Y-%m-%d_%H-%M.log",
				BufferSize:         10,
				MinDirectWriteSize: 10,
				FlushInterval:      time.Hour,
			})

			n, err := wc.Write([]byte("hello "))
			require.NoError(t, err)
			require.Equal(t, 6, n)

			filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
			require.NoFileExists(t, filePath)

			n, err = wc.Write([]byte("world!"))
			require.NoError(t, err)
			require.Equal(t, 6, n)

			data, err := os.ReadFile(filePath)
			require.NoError(t, err)
			require.Equal(t, "hello ", string(data))
		})
	})

	t.Run("flush buffer after interval", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern:    "app-%Y-%m-%d_%H-%M.log",
				BufferSize:         100,
				MinDirectWriteSize: 10,
				FlushInterval:      1 * time.Second,
			})

			for i := range 2 {
				var p []byte
				if i == 0 {
					p = []byte("1234\n")
				} else {
					p = []byte("abcd\n")
				}
				n, err := wc.Write(p)
				require.NoError(t, err)
				require.Equal(t, len(p), n)
			}

			filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
			require.NoFileExists(t, filePath)

			time.Sleep(1 * time.Second)
			synctest.Wait()

			data, err := os.ReadFile(filePath)
			require.NoError(t, err)
			require.Equal(t, "1234\nabcd\n", string(data))
		})
	})

	t.Run("ensure newline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, tempDir := openTempFile(t, Options{
				FilePathPattern:    "app-%Y-%m-%d_%H-%M.log",
				BufferSize:         10,
				MinDirectWriteSize: 10,
				FlushInterval:      time.Hour,
				EnsureNewline:      true,
			})

			for i := range 2 {
				p := []byte("123456789AB")[:8+i]
				n, err := wc.Write(p)
				require.NoError(t, err)
				require.Equal(t, len(p), n)

				filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
				if i == 0 {
					require.NoFileExists(t, filePath)
				} else {
					data, err := os.ReadFile(filePath)
					require.NoError(t, err)
					require.Equal(t, "12345678\n123456789\n", string(data))
				}
			}
		})
	})

	t.Run("write after close", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			wc, _ := openTempFile(t, Options{
				FilePathPattern:    "app-%Y-%m-%d_%H-%M.log",
				BufferSize:         100,
				MinDirectWriteSize: 10,
			})

			err := wc.Close()
			require.NoError(t, err)

			n, err := wc.Write([]byte("test"))
			require.ErrorContains(t, err, "closed")
			require.Equal(t, 0, n)
		})
	})

	t.Run("restart auto flusher when idle", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var goroutineCount atomic.Int32

			wc, tempDir := openTempFile(t, *(&Options{
				FilePathPattern:    "app-%Y-%m-%d_%H-%M.log",
				BufferSize:         100,
				MinDirectWriteSize: 10,
				FlushInterval:      1 * time.Second,
				MaxIdleBufferAge:   5,
			}).SetGo(func(f func()) {
				goroutineCount.Add(1)
				go func() {
					f()
					goroutineCount.Add(-1)
				}()
			}))

			n, err := wc.Write([]byte("hello "))
			require.NoError(t, err)
			require.Equal(t, 6, n)

			time.Sleep(5 * time.Second)
			synctest.Wait()

			require.Equal(t, 1, int(goroutineCount.Load()))

			time.Sleep(1 * time.Second)
			synctest.Wait()
			require.Equal(t, 0, int(goroutineCount.Load()))

			n, err = wc.Write([]byte("world!"))
			require.NoError(t, err)
			require.Equal(t, 6, n)

			time.Sleep(1 * time.Second)
			synctest.Wait()

			require.Equal(t, 1, int(goroutineCount.Load()))

			filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15-04.log"))
			data, err := os.ReadFile(filePath)
			require.NoError(t, err)
			require.Equal(t, "hello world!", string(data))
		})
	})
}

func TestBufferedWriteCloser_Close(t *testing.T) {
	t.Run("close without open file", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			registry := new(sync.Map)

			wc, _ := openTempFile(t, *(&Options{
				FilePathPattern:    "app-%Y-%m-%d_%H.log",
				BufferSize:         100,
				MinDirectWriteSize: 10,
				FlushInterval:      1 * time.Hour,
				MaxIdleBufferAge:   5,
			}).SetRegistry(registry))

			require.Equal(t, 1, countWriteClosers(registry))

			err := wc.Close()
			require.NoError(t, err)

			require.Equal(t, 0, countWriteClosers(registry))

			err = wc.Close()
			require.ErrorContains(t, err, "closed")
		})
	})

	t.Run("close after empty write", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			registry := new(sync.Map)

			wc, tempDir := openTempFile(t, *(&Options{
				FilePathPattern:    "app-%Y-%m-%d_%H.log",
				BufferSize:         100,
				MinDirectWriteSize: 10,
				FlushInterval:      1 * time.Hour,
				MaxIdleBufferAge:   5,
			}).SetRegistry(registry))

			_, err := wc.Write([]byte(""))
			require.NoError(t, err)

			filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15.log"))
			require.NoFileExists(t, filePath)

			err = wc.Close()
			require.NoError(t, err)

			require.FileExists(t, filePath)
		})
	})

	t.Run("flush buffer on close", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			registry := new(sync.Map)
			var goroutineCount atomic.Int32

			wc, tempDir := openTempFile(t, *(&Options{
				FilePathPattern:    "app-%Y-%m-%d_%H.log",
				BufferSize:         100,
				MinDirectWriteSize: 100,
				FlushInterval:      1 * time.Hour,
				MaxIdleBufferAge:   5,
			}).
				SetRegistry(registry).
				SetGo(func(f func()) {
					goroutineCount.Add(1)
					go func() {
						f()
						goroutineCount.Add(-1)
					}()
				}))

			_, err := wc.Write([]byte("hello world!"))
			require.NoError(t, err)

			require.Equal(t, 1, int(goroutineCount.Load()))

			filePath := filepath.Join(tempDir, time.Now().Format("app-2006-01-02_15.log"))
			require.NoFileExists(t, filePath)

			err = wc.Close()
			require.NoError(t, err)

			require.Equal(t, 0, int(goroutineCount.Load()))

			data, err := os.ReadFile(filePath)
			require.NoError(t, err)
			require.Equal(t, "hello world!", string(data))
		})
	})
}

func TestComprehensive(t *testing.T) {
	const N = 10
	const M = 200

	wc, tempDir := openTempFile(t, Options{
		FilePathPattern:  "app_%Y-%m-%d-%H-%M-%S.log",
		SymbolicLinkPath: "app.log",
		FileSizeLimit:    10000,
		BufferSize:       150,
		FlushInterval:    30 * time.Millisecond,
		EnsureNewline:    true,
	})

	line := []byte("The quick brown fox jumps over the lazy dog\n")
	lines := [...][]byte{
		line,
		bytes.Repeat(line, 2),
		bytes.Repeat(line, 3),
	}
	var wg sync.WaitGroup
	for range N {
		wg.Go(func() {
			for m := M; m >= 1; {
				i := rand.IntN(min(m, len(lines)))
				m -= i + 1
				p := lines[i]
				l := len(p) - rand.IntN(2)
				_, err := wc.Write(p[:l])
				require.NoError(t, err)
				time.Sleep(time.Duration(5+rand.IntN(10)) * time.Millisecond)
			}
		})
	}
	wg.Wait()

	err := wc.Close()
	require.NoError(t, err)

	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	rand.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })
	hash := md5.New()
	var filePaths []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		filePath := filepath.Join(tempDir, entry.Name())
		fileInfo, err := entry.Info()
		require.NoError(t, err)
		t.Logf("filePath=%q fileSize=%v", filePath, fileInfo.Size())

		f, err := os.Open(filePath)
		require.NoError(t, err)
		_, err = io.Copy(hash, f)
		require.NoError(t, err)
		f.Close()

		filePaths = append(filePaths, filePath)
	}

	expectedHash := md5.New()
	for range N * M {
		_, err := expectedHash.Write(line)
		require.NoError(t, err)
	}

	require.Equal(t, expectedHash.Sum(nil), hash.Sum(nil))

	sort.Strings(filePaths)
	expectedLinkedFilePath := filePaths[len(filePaths)-1]
	latestFilePath, err := os.Readlink(filepath.Join(tempDir, "app.log"))
	require.NoError(t, err)
	require.Equal(t, expectedLinkedFilePath, latestFilePath)
}

func BenchmarkWriteCloser_Write(b *testing.B) {
	const maxFileSize = 300 * 1024 * 1024

	SetOpenFileFlagModifier(b, func(flag int) int {
		return flag & ^os.O_APPEND
	})

	tempDir := b.TempDir()
	data := make([]byte, 1024*1024)
	fillRandomData(make([]byte, 1024*1024))

	for _, writeSize := range [...]int{
		96 * 1024,
		64 * 1024,
		48 * 1024,
		32 * 1024,
		16 * 1024,
		4 * 1024,
		1000,
		500,
		100,
	} {
		b.Run(fmt.Sprintf("WS=%v", writeSize), func(b *testing.B) {
			for _, buffering := range [...]bool{false, true} {
				bufferSize := 0         // use default
				minDirectWriteSize := 0 // use default
				if !buffering {
					bufferSize = -1
				}

				b.Run(fmt.Sprintf("B=%v", buffering), func(b *testing.B) {
					wc := openFileInDir(b, Options{
						FilePathPattern:    "app_%Y-%m-%d.log",
						EnsureNewline:      true,
						BufferSize:         bufferSize,
						MinDirectWriteSize: minDirectWriteSize,
					}, tempDir)

					data := data[:writeSize]
					n := 0
					b.SetBytes(int64(writeSize))
					b.ReportAllocs()

					for b.Loop() {
						nn, err := wc.Write(data)
						if err != nil {
							b.Fail()
						}
						n += nn
						if n >= maxFileSize {
							resetFileOffset(b, wc)
							n = 0
						}
					}
				})
			}
		})
	}
}
