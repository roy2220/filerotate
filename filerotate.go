package filerotate

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/spf13/afero"
)

const (
	defaultBufferSize          = 8 * 1024 * 1024
	defaultFlushInterval       = 1 * time.Second
	defaultMaxIdleBufferAge    = 3 * time.Second
	defaultLargeWriteThreshold = 1 / math.Phi
)

// Options defines the configuration for file rotation and buffering.
type Options struct {
	// FilePathPattern specifies the pattern for file naming.
	// It uses time.Format style for date/time substitution.
	// For example: "logs/app-2006-01-02.log"
	FilePathPattern string

	// FileSizeLimit is the maximum size (in bytes) a single log file can reach
	// before rotation occurs. A non-positive value disables size-based rotation.
	FileSizeLimit int64

	// The options below serve for buffering.

	// BufferSize is the size (in bytes) of the internal buffer.
	// A zero value uses a default size of 8MB.
	// If a buffered writer is not desired, set it to a negative value (e.g., -1).
	BufferSize int

	// LargeWriteThreshold is a ratio (0.0 to 1.0) of BufferSize.
	// If a single write operation is larger than this threshold, it will bypass the buffer
	// and write directly to the underlying file.
	// A non-positive value uses a default threshold based on the golden ratio (0.618).
	LargeWriteThreshold float64

	// FlushInterval specifies how often the buffer should be automatically flushed.
	// A non-positive value uses a default interval of 1 second.
	FlushInterval time.Duration

	// MaxIdleBufferAge specifies the maximum time the buffer can remain empty
	// before the auto-flushing background routine stops to conserve resources.
	// A non-positive value uses a default age of 3 seconds.
	MaxIdleBufferAge time.Duration

	// LogFlushErr is a function to handle errors that occur during background
	// automatic flushing. If nil, errors are silently ignored.
	LogFlushErr func(error)

	// The options below serve for testing.

	// Go specifies the function to use for starting background goroutines.
	Go func(func())

	// Clock specifies the clock interface to use for time-related operations.
	// If nil, the real system clock is used.
	Clock clock.Clock

	// Fs specifies the filesystem interface to use.
	// If nil, the local OS filesystem is used.
	Fs afero.Fs
}

type fileManager struct {
	filePathPattern string
	fileSizeLimit   int64
	clock           clock.Clock
	fs              afero.Fs

	lock      sync.Mutex
	isClosed  bool
	filePath0 string
	fileIndex int
	fileSize  int64
	file      afero.File
}

// OpenFile creates a new io.WriteCloser with file rotation and optional buffering
// based on the provided Options.
func OpenFile(options Options) (io.WriteCloser, error) {
	if options.FilePathPattern == "" {
		return nil, errors.New("filerotate: no file path pattern")
	}
	if options.Go == nil {
		options.Go = func(f func()) { go f() }
	}
	if options.Clock == nil {
		options.Clock = clock.New()
	}
	if options.Fs == nil {
		options.Fs = afero.NewOsFs()
	}
	wc := io.WriteCloser(&fileManager{
		filePathPattern: options.FilePathPattern,
		fileSizeLimit:   options.FileSizeLimit,
		clock:           options.Clock,
		fs:              options.Fs,
	})
	if options.BufferSize >= 0 {
		wc = newBufferedWriteCloser(
			wc,
			options.BufferSize,
			options.LargeWriteThreshold,
			options.FlushInterval,
			options.MaxIdleBufferAge,
			options.LogFlushErr,
			options.Go,
			options.Clock,
		)
	}
	return wc, nil
}

var ErrClosed = errors.New("filerotate: closed")

func (m *fileManager) Write(p []byte) (int, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isClosed {
		return 0, ErrClosed
	}

	if err := m.openFileLocked(); err != nil {
		return 0, err
	}

	n, err := m.file.Write(p)
	m.fileSize += int64(n)
	return n, err
}

func (m *fileManager) openFileLocked() error {
	filePath0 := m.clock.Now().Format(m.filePathPattern)
	if m.filePath0 != filePath0 {
		lastFileIndex := -1
		var lastFilePath string
		var lastFileInfo os.FileInfo
		for fileIndex, filePath := 0, filePath0; ; {
			fileInfo, err := m.fs.Stat(filePath)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return err
			}
			lastFileIndex, lastFilePath, lastFileInfo = fileIndex, filePath, fileInfo
			fileIndex++
			filePath = filePath0 + "." + strconv.Itoa(fileIndex)
		}

		var fileIndex int
		var filePath string
		var fileSize int64
		if lastFileIndex == -1 {
			filePath = filePath0
		} else {
			fileIndex = lastFileIndex
			filePath = lastFilePath
			fileSize = lastFileInfo.Size()
		}
		file, err := m.openFile(filePath)
		if err != nil {
			return err
		}

		m.filePath0 = filePath0
		m.fileIndex = fileIndex
		m.fileSize = fileSize
		if m.file != nil {
			m.file.Close()
		}
		m.file = file
	}

	if m.fileSizeLimit >= 1 && m.fileSize >= m.fileSizeLimit {
		fileIndex := m.fileIndex + 1
		filePath := m.filePath0 + "." + strconv.Itoa(fileIndex)
		file, err := m.openFile(filePath)
		if err != nil {
			return err
		}

		m.fileIndex = fileIndex
		m.fileSize = 0
		m.file.Close()
		m.file = file
	}

	return nil
}

func (m *fileManager) openFile(filePath string) (afero.File, error) {
	if err := m.fs.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, err
	}
	file, err := m.fs.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (m *fileManager) Close() error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isClosed {
		return ErrClosed
	}

	var err error
	if m.file != nil {
		err = m.file.Close()
	}
	m.isClosed = true
	return err
}

type bufferedWriteCloser struct {
	wc                io.WriteCloser
	bufferSize        int
	minLargeWriteSize int
	flushInterval     time.Duration
	maxIdleBufferAge  time.Duration
	logFlushErr       func(error)
	go1               func(func())
	clock             clock.Clock

	backgroundCtx context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup

	lock                 sync.Mutex
	isClosing            bool
	isClosed             bool
	hasPendingWrites     bool
	pendingData          []byte
	autoFlusherIsRunning bool
}

func newBufferedWriteCloser(
	wc io.WriteCloser,
	bufferSize int,
	largeWriteThreshold float64,
	flushInterval time.Duration,
	maxIdleBufferAge time.Duration,
	logFlushErr func(error),
	go1 func(func()),
	clock clock.Clock,
) io.WriteCloser {
	if bufferSize < 1 {
		bufferSize = defaultBufferSize
	}
	if largeWriteThreshold <= 0.0 {
		largeWriteThreshold = defaultLargeWriteThreshold
	} else if largeWriteThreshold > 1.0 {
		largeWriteThreshold = 1.0
	}
	minLargeWriteSize := int(math.Ceil(float64(bufferSize) * largeWriteThreshold))
	if flushInterval < 1 {
		flushInterval = defaultFlushInterval
	}
	if maxIdleBufferAge < 1 {
		maxIdleBufferAge = defaultMaxIdleBufferAge
	}
	if logFlushErr == nil {
		logFlushErr = func(error) {}
	}

	backgroundCtx, cancel := context.WithCancel(context.Background())

	return &bufferedWriteCloser{
		wc:                wc,
		bufferSize:        bufferSize,
		minLargeWriteSize: minLargeWriteSize,
		flushInterval:     flushInterval,
		maxIdleBufferAge:  maxIdleBufferAge,
		logFlushErr:       logFlushErr,
		go1:               go1,
		clock:             clock,
		backgroundCtx:     backgroundCtx,
		cancel:            cancel,
	}
}

func (wc *bufferedWriteCloser) Write(p []byte) (int, error) {
	wc.lock.Lock()
	defer wc.lock.Unlock()

	if wc.isClosed {
		return 0, ErrClosed
	}

	n := len(p)
	if n <= wc.bufferSize-len(wc.pendingData) { // remaining buffer space is sufficient
		if len(wc.pendingData) == 0 && n >= wc.minLargeWriteSize {
			return wc.wc.Write(p)
		}
	} else { // remaining buffer space is insufficient
		if len(wc.pendingData) >= 1 {
			if err := wc.flushLocked(); err != nil {
				return 0, err
			}
		}
		if n >= wc.minLargeWriteSize {
			return wc.wc.Write(p)
		}
	}

	wc.hasPendingWrites = true
	if wc.pendingData == nil {
		wc.pendingData = make([]byte, 0, wc.bufferSize)
	}
	wc.pendingData = append(wc.pendingData, p...)
	wc.runAutoFlusherLocked()

	return n, nil
}

func (wc *bufferedWriteCloser) runAutoFlusherLocked() {
	if wc.isClosing {
		return
	}
	if wc.autoFlusherIsRunning {
		return
	}

	wc.wg.Add(1)
	wc.go1(func() {
		defer wc.wg.Done()

		wc.autoFlush()
	})
	wc.autoFlusherIsRunning = true
}

func (wc *bufferedWriteCloser) autoFlush() {
	ticker := wc.clock.Ticker(wc.flushInterval)
	defer ticker.Stop()

	var idleBufferAge time.Duration
	ok := true
	for ok {
		select {
		case <-ticker.C:
		case <-wc.backgroundCtx.Done():
			return
		}

		ok = func() bool {
			var flushErr error
			wc.lock.Lock()
			defer func() {
				wc.lock.Unlock()
				if flushErr != nil {
					wc.logFlushErr(flushErr)
				}
			}()

			if wc.hasPendingWrites {
				idleBufferAge = 0
				flushErr = wc.flushLocked()
			} else {
				idleBufferAge += wc.flushInterval
				if idleBufferAge > wc.maxIdleBufferAge {
					wc.pendingData = nil
					wc.autoFlusherIsRunning = false
					return false
				}
			}
			return true
		}()
	}
}

func (wc *bufferedWriteCloser) flushLocked() error {
	n, err := wc.wc.Write(wc.pendingData)
	if err != nil {
		if n >= 1 {
			copy(wc.pendingData, wc.pendingData[n:])
			wc.pendingData = wc.pendingData[:len(wc.pendingData)-n]
		}
		return err
	}

	wc.hasPendingWrites = false
	wc.pendingData = wc.pendingData[:0]
	return nil
}

func (wc *bufferedWriteCloser) Close() error {
	wc.lock.Lock()
	wc.isClosing = true
	wc.lock.Unlock()

	wc.cancel()
	wc.wg.Wait()

	var flushErr error
	wc.lock.Lock()
	defer func() {
		wc.lock.Unlock()
		if flushErr != nil {
			wc.logFlushErr(flushErr)
		}
	}()

	if wc.isClosed {
		return ErrClosed
	}

	if wc.hasPendingWrites {
		flushErr = wc.flushLocked()
		wc.pendingData = nil
	}

	err := wc.wc.Close()
	wc.isClosed = true
	return err
}
