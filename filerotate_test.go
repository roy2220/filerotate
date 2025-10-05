package filerotate_test

import (
	"bytes"
	"crypto/md5"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/roy2220/filerotate"
	. "github.com/roy2220/filerotate"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_OpenFile(t *testing.T) {
	_, err := OpenFile(filerotate.Options{})
	require.ErrorContains(t, err, "no file path pattern")
}

func Test_Write(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	t0 := time.Date(2001, 2, 3, 4, 5, 6, 0, loc)

	t.Run(
		"writes data to the file with the current timestamp in its name",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern: "/log/%Y-%m-%d-%H.log",
				BufferSize:      -1,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				err := wc.Close()
				assert.NoError(t, err)
			})

			n, err := wc.Write([]byte("abc\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			data, err := afero.ReadFile(fs, "/log/2001-02-03-04.log")
			require.NoError(t, err)
			require.Equal(t, "abc\n", string(data))

			n, err = wc.Write([]byte("def\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			data, err = afero.ReadFile(fs, "/log/2001-02-03-04.log")
			require.NoError(t, err)
			require.Equal(t, "abc\ndef\n", string(data))

			clock.Add(time.Hour)

			n, err = wc.Write([]byte("ghi\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			data, err = afero.ReadFile(fs, "/log/2001-02-03-05.log")
			require.NoError(t, err)
			require.Equal(t, "ghi\n", string(data))

			n, err = wc.Write([]byte("jkl\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			data, err = afero.ReadFile(fs, "/log/2001-02-03-05.log")
			require.NoError(t, err)
			require.Equal(t, "ghi\njkl\n", string(data))
		},
	)

	t.Run(
		"rotates the file when it exceeds the size limit",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern:  "/log/%Y-%m-%d-%H.log",
				SymbolicLinkPath: "/log/test.log",
				FileSizeLimit:    6,
				BufferSize:       -1,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				err := wc.Close()
				assert.NoError(t, err)
			})

			n, err := wc.Write([]byte("abc\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			n, err = wc.Write([]byte("def\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			n, err = wc.Write([]byte("ghi\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			n, err = wc.Write([]byte("jkl\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			clock.Add(time.Hour)

			n, err = wc.Write([]byte("mnopqr\n"))
			require.NoError(t, err)
			require.Equal(t, 7, n)

			n, err = wc.Write([]byte("stuvwx\n"))
			require.NoError(t, err)
			require.Equal(t, 7, n)

			n, err = wc.Write([]byte("zzz\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			data, err := afero.ReadFile(fs, "/log/2001-02-03-04.log")
			require.NoError(t, err)
			require.Equal(t, "abc\ndef\n", string(data))

			data, err = afero.ReadFile(fs, "/log/2001-02-03-04.log.1")
			require.NoError(t, err)
			require.Equal(t, "ghi\njkl\n", string(data))

			data, err = afero.ReadFile(fs, "/log/2001-02-03-05.log")
			require.NoError(t, err)
			require.Equal(t, "mnopqr\n", string(data))

			data, err = afero.ReadFile(fs, "/log/2001-02-03-05.log.1")
			require.NoError(t, err)
			require.Equal(t, "stuvwx\n", string(data))

			data, err = afero.ReadFile(fs, "/log/2001-02-03-05.log.2")
			require.NoError(t, err)
			require.Equal(t, "zzz\n", string(data))
		},
	)

	t.Run(
		"skips existing rotated files",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			err := afero.WriteFile(fs, "/log/2001-02-03-04.log", []byte("0\n"), 0644)
			require.NoError(t, err)
			err = afero.WriteFile(fs, "/log/2001-02-03-04.log.1", []byte("1\n"), 0644)
			require.NoError(t, err)
			err = afero.WriteFile(fs, "/log/2001-02-03-04.log.2", []byte("2\n"), 0644)
			require.NoError(t, err)

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern: "/log/%Y-%m-%d-%H.log",
				FileSizeLimit:   6,
				BufferSize:      -1,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				err := wc.Close()
				assert.NoError(t, err)
			})

			n, err := wc.Write([]byte("abc\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			data, err := afero.ReadFile(fs, "/log/2001-02-03-04.log.2")
			require.NoError(t, err)
			require.Equal(t, "2\nabc\n", string(data))
		},
	)

	t.Run(
		"skips existing rotated files and rotates the file when it exceeds the size limit",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			err := afero.WriteFile(fs, "/log/2001-02-03-04.log", []byte("0\n"), 0644)
			require.NoError(t, err)
			err = afero.WriteFile(fs, "/log/2001-02-03-04.log.1", []byte("1\n"), 0644)
			require.NoError(t, err)
			err = afero.WriteFile(fs, "/log/2001-02-03-04.log.2", []byte("123456\n"), 0644)
			require.NoError(t, err)

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern: "/log/%Y-%m-%d-%H.log",
				FileSizeLimit:   6,
				BufferSize:      -1,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				err := wc.Close()
				assert.NoError(t, err)
			})

			n, err := wc.Write([]byte("abc\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			data, err := afero.ReadFile(fs, "/log/2001-02-03-04.log.3")
			require.NoError(t, err)
			require.Equal(t, "abc\n", string(data))
		},
	)

	t.Run(
		"flushes buffered data to the file at regular intervals",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern: "/log/%Y-%m-%d-%H.log",
				BufferSize:      1000,
				FlushInterval:   time.Minute,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				err := wc.Close()
				assert.NoError(t, err)
			})

			n, err := wc.Write([]byte("abc\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			time.Sleep(25 * time.Millisecond)

			ok, err := afero.Exists(fs, "/log/2001-02-03-04.log")
			require.False(t, ok)

			clock.Add(10 * time.Minute)
			time.Sleep(25 * time.Millisecond)

			data, err := afero.ReadFile(fs, "/log/2001-02-03-04.log")
			require.NoError(t, err)
			require.Equal(t, "abc\n", string(data))

			n, err = wc.Write([]byte("def\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			time.Sleep(25 * time.Millisecond)
			clock.Add(10 * time.Minute)
			time.Sleep(25 * time.Millisecond)

			data, err = afero.ReadFile(fs, "/log/2001-02-03-04.log")
			require.NoError(t, err)
			require.Equal(t, "abc\ndef\n", string(data))
		},
	)

	t.Run(
		"writes large data directly to the file, bypassing the buffer",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern:     "/log/%Y-%m-%d-%H.log",
				BufferSize:          100,
				LargeWriteThreshold: 0.05,
				FlushInterval:       time.Minute,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				err := wc.Close()
				assert.NoError(t, err)
			})

			n, err := wc.Write([]byte("abcde\n"))
			require.NoError(t, err)
			require.Equal(t, 6, n)

			data, err := afero.ReadFile(fs, "/log/2001-02-03-04.log")
			require.NoError(t, err)
			require.Equal(t, "abcde\n", string(data))
		},
	)

	t.Run(
		"writes large data directly to the file, bypassing the buffer",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern:     "/log/%Y-%m-%d-%H.log",
				BufferSize:          10,
				LargeWriteThreshold: 0.6,
				FlushInterval:       time.Minute,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				err := wc.Close()
				assert.NoError(t, err)
			})

			n, err := wc.Write([]byte("abcd\n"))
			require.NoError(t, err)
			require.Equal(t, 5, n)

			n, err = wc.Write([]byte("efg\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			ok, err := afero.Exists(fs, "/log/2001-02-03-04.log")
			require.False(t, ok)

			n, err = wc.Write([]byte("hijkl\n"))
			require.NoError(t, err)
			require.Equal(t, 6, n)

			data, err := afero.ReadFile(fs, "/log/2001-02-03-04.log")
			require.NoError(t, err)
			require.Equal(t, "abcd\nefg\nhijkl\n", string(data))
		},
	)

	t.Run(
		"buffer should not be flushed if some data is buffered before the large write",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern:     "/log/%Y-%m-%d-%H.log",
				BufferSize:          100,
				LargeWriteThreshold: 0.05,
				FlushInterval:       time.Minute,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				err := wc.Close()
				assert.NoError(t, err)
			})

			n, err := wc.Write([]byte("a\n"))
			require.NoError(t, err)
			require.Equal(t, 2, n)

			n, err = wc.Write([]byte("bcdef\n"))
			require.NoError(t, err)
			require.Equal(t, 6, n)

			ok, err := afero.Exists(fs, "/log/2001-02-03-04.log")
			require.False(t, ok)
		},
	)

	t.Run(
		"flushes buffered data if buffer is unsufficient for the new write",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern:     "/log/%Y-%m-%d-%H.log",
				BufferSize:          10,
				LargeWriteThreshold: 1.0,
				FlushInterval:       time.Minute,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				err := wc.Close()
				assert.NoError(t, err)
			})

			n, err := wc.Write([]byte("abcd\n"))
			require.NoError(t, err)
			require.Equal(t, 5, n)

			n, err = wc.Write([]byte("efg\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			ok, err := afero.Exists(fs, "/log/2001-02-03-04.log")
			require.False(t, ok)

			n, err = wc.Write([]byte("hijk\n"))
			require.NoError(t, err)
			require.Equal(t, 5, n)

			data, err := afero.ReadFile(fs, "/log/2001-02-03-04.log")
			require.NoError(t, err)
			require.Equal(t, "abcd\nefg\n", string(data))

			time.Sleep(25 * time.Millisecond)
			clock.Add(10 * time.Minute)
			time.Sleep(25 * time.Millisecond)

			data, err = afero.ReadFile(fs, "/log/2001-02-03-04.log")
			require.NoError(t, err)
			require.Equal(t, "abcd\nefg\nhijk\n", string(data))
		},
	)

	t.Run(
		"stops the flush goroutine when buffer is idle for too long",
		func(t *testing.T) {
			var goroutineCount atomic.Int64
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Go: func(f func()) {
					goroutineCount.Add(1)
					go func() {
						defer goroutineCount.Add(-1)
						f()
					}()
				},
				Clock: clock,
				Fs:    fs,

				FilePathPattern:  "/log/%Y-%m-%d-%H.log",
				BufferSize:       1000,
				FlushInterval:    time.Minute,
				MaxIdleBufferAge: 3,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				err := wc.Close()
				assert.NoError(t, err)
			})

			n, err := wc.Write([]byte("abc\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			time.Sleep(25 * time.Millisecond)
			clock.Add(1*time.Minute + time.Second)
			time.Sleep(25 * time.Millisecond)

			require.Equal(t, int64(1), goroutineCount.Load())

			time.Sleep(25 * time.Millisecond)
			clock.Add(4*time.Minute + time.Second)
			time.Sleep(25 * time.Millisecond)

			require.Equal(t, int64(0), goroutineCount.Load())

			n, err = wc.Write([]byte("def\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			time.Sleep(25 * time.Millisecond)

			require.Equal(t, int64(1), goroutineCount.Load())
		},
	)

	t.Run(
		"flushes even when there's no data to flush",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern: "/log/%Y-%m-%d-%H.log",
				BufferSize:      1000,
				FlushInterval:   time.Minute,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				err := wc.Close()
				assert.NoError(t, err)
			})

			n, err := wc.Write([]byte(""))
			require.NoError(t, err)
			require.Equal(t, 0, n)

			time.Sleep(25 * time.Millisecond)

			ok, err := afero.Exists(fs, "/log/2001-02-03-04.log")
			require.False(t, ok)

			clock.Add(10 * time.Minute)
			time.Sleep(25 * time.Millisecond)

			data, err := afero.ReadFile(fs, "/log/2001-02-03-04.log")
			require.NoError(t, err)
			require.Equal(t, "", string(data))
		},
	)
}

func Test_Close(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	t0 := time.Date(2001, 2, 3, 4, 5, 6, 0, loc)

	t.Run(
		"returns an error when writing to a closed WriteCloser",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern: "/log/%Y-%m-%d-%H.log",
				BufferSize:      -1,
			})
			require.NoError(t, err)

			err = wc.Close()
			require.NoError(t, err)

			_, err = wc.Write([]byte("abc\n"))
			require.ErrorContains(t, err, "closed")

			err = wc.Close()
			require.ErrorContains(t, err, "closed")

		},
	)

	t.Run(
		"returns an error when writing to a closed WriteCloser",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern: "/log/%Y-%m-%d-%H.log",
				BufferSize:      1000,
				FlushInterval:   time.Minute,
			})
			require.NoError(t, err)

			err = wc.Close()
			require.NoError(t, err)

			_, err = wc.Write([]byte("abc\n"))
			require.ErrorContains(t, err, "closed")

			err = wc.Close()
			require.ErrorContains(t, err, "closed")

		},
	)

	t.Run(
		"flushes buffered data and stops the flush goroutine",
		func(t *testing.T) {
			var goroutineCount atomic.Int64
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Go: func(f func()) {
					goroutineCount.Add(1)
					go func() {
						defer goroutineCount.Add(-1)
						f()
					}()
				},
				Clock: clock,
				Fs:    fs,

				FilePathPattern: "/log/%Y-%m-%d-%H.log",
				BufferSize:      1000,
				FlushInterval:   time.Minute,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				if wc != nil {
					err := wc.Close()
					assert.NoError(t, err)
				}
			})

			n, err := wc.Write([]byte("abc\n"))
			require.NoError(t, err)
			require.Equal(t, 4, n)

			time.Sleep(25 * time.Millisecond)

			err = wc.Close()
			wc = nil
			require.NoError(t, err)

			data, err := afero.ReadFile(fs, "/log/2001-02-03-04.log")
			require.NoError(t, err)
			require.Equal(t, "abc\n", string(data))

			require.Equal(t, int64(0), goroutineCount.Load())
		},
	)

	t.Run(
		"flushes even when there's no data to flush",
		func(t *testing.T) {
			clock := clock.NewMock()
			clock.Set(t0)
			fs := afero.NewMemMapFs()

			wc, err := OpenFile(Options{
				Clock: clock,
				Fs:    fs,

				FilePathPattern: "/log/%Y-%m-%d-%H.log",
				BufferSize:      1000,
				FlushInterval:   time.Minute,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				if wc != nil {
					err := wc.Close()
					assert.NoError(t, err)
				}
			})

			n, err := wc.Write([]byte(""))
			require.NoError(t, err)
			require.Equal(t, 0, n)

			err = wc.Close()
			wc = nil
			require.NoError(t, err)

			data, err := afero.ReadFile(fs, "/log/2001-02-03-04.log")
			require.NoError(t, err)
			require.Equal(t, "", string(data))
		},
	)
}

func Test_Comprehensive(t *testing.T) {
	const N = 10
	const M = 200

	dirPath := t.TempDir()
	symbolicLinkPath := filepath.Join(dirPath, "app.log")
	wc, err := OpenFile(Options{
		FilePathPattern:  filepath.Join(dirPath, "app_%Y-%m-%d-%H-%M-%S.log"),
		SymbolicLinkPath: symbolicLinkPath,
		FileSizeLimit:    10000,
		BufferSize:       100,
		FlushInterval:    30 * time.Millisecond,
	})
	require.NoError(t, err)

	line := []byte("The quick brown fox jumps over the lazy dog\n")
	var wg sync.WaitGroup
	for range N {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range M {
				_, err := wc.Write(line)
				require.NoError(t, err)
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	err = wc.Close()
	assert.NoError(t, err)

	entries, err := os.ReadDir(dirPath)
	require.NoError(t, err)
	hash := md5.New()
	var filePaths []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		filePath := filepath.Join(dirPath, entry.Name())
		fileInfo, err := entry.Info()
		require.NoError(t, err)
		t.Logf("filePath=%q fileSize=%v", filePath, fileInfo.Size())
		data, err := os.ReadFile(filePath)
		require.NoError(t, err)
		hash.Write(data)
		filePaths = append(filePaths, filePath)
	}

	expectedHashSum := md5.Sum(bytes.Repeat(line, N*M))
	require.Equal(t, expectedHashSum[:], hash.Sum(nil))

	sort.Strings(filePaths)
	expectedLinkedFilePath := filePaths[len(filePaths)-1]
	lastFilePath, err := os.Readlink(symbolicLinkPath)
	require.NoError(t, err)
	require.Equal(t, expectedLinkedFilePath, lastFilePath)
}
