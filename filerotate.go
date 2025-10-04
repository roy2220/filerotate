package filerotate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/lestrrat-go/strftime"
	"github.com/spf13/afero"
)

// Options defines the configuration for file rotation and buffering.
type Options struct {
	// FilePathPattern specifies the pattern for file naming.
	// It uses strftime format for date/time substitution.
	// For example: "logs/app-%Y-%m-%d.log"
	FilePathPattern string

	// SymbolicLinkPath is the path for a symbolic link that points to the currently
	// active log file. An empty string disables the symbolic link feature.
	// This link is atomically updated after each file rotation.
	SymbolicLinkPath string

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

	// IdleBufferTimeout specifies the maximum duration the buffer can remain idle
	// (without new writes) before the auto-flusher goroutine stops and releases
	// the buffer memory. When the buffer has been idle for this duration, the
	// internal buffer slice is set to nil to free memory. A non-positive value
	// uses a default timeout of 3 seconds.
	IdleBufferTimeout time.Duration

	// LogInternalError specifies a callback function for handling internal errors
	// that occur during background operations (such as auto-flushing failures or
	// file close errors). These errors cannot be returned to the caller directly
	// since they happen asynchronously. If nil, a default logger is used that
	// writes to the standard log with the prefix "filerotate internal error: ".
	LogInternalError func(error)

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

const (
	defaultBufferSize          = 8 * 1024 * 1024
	defaultFlushInterval       = 1 * time.Second
	defaultIdleBufferTimeout   = 3 * time.Second
	defaultLargeWriteThreshold = 1 / math.Phi
)

func defaultLogInternalError(err error) { log.Printf("filerotate internal error: %v", err) }

func (o Options) applyDefaults() Options {
	if o.BufferSize == 0 {
		o.BufferSize = defaultBufferSize
	}
	if o.LargeWriteThreshold <= 0.0 {
		o.LargeWriteThreshold = defaultLargeWriteThreshold
	} else if o.LargeWriteThreshold > 1.0 {
		o.LargeWriteThreshold = 1.0
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = defaultFlushInterval
	}
	if o.IdleBufferTimeout <= 0 {
		o.IdleBufferTimeout = defaultIdleBufferTimeout
	}
	if o.LogInternalError == nil {
		o.LogInternalError = defaultLogInternalError
	}
	if o.Go == nil {
		o.Go = func(f func()) { go f() }
	}
	if o.Clock == nil {
		o.Clock = clock.New()
	}
	if o.Fs == nil {
		o.Fs = afero.NewOsFs()
	}
	return o
}

// OpenFile creates a new io.WriteCloser with file rotation and optional buffering
// based on the provided Options.
func OpenFile(options Options) (io.WriteCloser, error) {
	options = options.applyDefaults()
	if options.FilePathPattern == "" {
		return nil, errors.New("filerotate: no file path pattern")
	}
	var err error
	options.FilePathPattern, err = filepath.Abs(options.FilePathPattern)
	if err != nil {
		return nil, fmt.Errorf("filerotate: get absolute path: %v", err)
	}
	if options.SymbolicLinkPath != "" {
		options.SymbolicLinkPath, err = filepath.Abs(options.SymbolicLinkPath)
		if err != nil {
			return nil, fmt.Errorf("filerotate: get absolute path: %v", err)
		}
	}
	filePathPattern, err := strftime.New(options.FilePathPattern)
	if err != nil {
		return nil, fmt.Errorf("filerotate: invalid file path pattern: %v", err)
	}

	wc := io.WriteCloser(&fileManager{
		filePathPattern:  filePathPattern,
		symbolicLinkPath: options.SymbolicLinkPath,
		fileSizeLimit:    options.FileSizeLimit,
		logInternalError: options.LogInternalError,
		clock:            options.Clock,
		fs:               options.Fs,
	})
	if options.BufferSize >= 1 {
		wc = newBufferedWriteCloser(
			wc,
			options.BufferSize,
			options.LargeWriteThreshold,
			options.FlushInterval,
			options.IdleBufferTimeout,
			options.LogInternalError,
			options.Go,
			options.Clock,
		)
	}
	return wc, nil
}

// ErrClosed is returned when an operation is attempted on a WriteCloser that has already been closed.
var ErrClosed = errors.New("filerotate: closed")

type fileManager struct {
	filePathPattern  *strftime.Strftime
	symbolicLinkPath string
	fileSizeLimit    int64
	logInternalError func(error)
	clock            clock.Clock
	fs               afero.Fs

	lock         sync.Mutex
	isClosed     bool
	baseFilePath string
	fileIndex    int
	filePath     string
	fileSize     int64
	file         afero.File
}

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
	oldFilePath := m.filePath
	now := m.clock.Now()

	if baseFilePath := m.filePathPattern.FormatString(now); m.baseFilePath != baseFilePath {
		lastFileIndex := -1
		var lastFilePath string
		var lastFileInfo os.FileInfo
		for fileIndex, filePath := 0, baseFilePath; ; {
			fileInfo, err := m.fs.Stat(filePath)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					break
				}
				return err
			}
			lastFileIndex, lastFilePath, lastFileInfo = fileIndex, filePath, fileInfo
			fileIndex++
			filePath = baseFilePath + "." + strconv.Itoa(fileIndex)
		}

		var fileIndex int
		var filePath string
		var fileSize int64
		if lastFileIndex == -1 {
			filePath = baseFilePath
		} else {
			fileIndex = lastFileIndex
			filePath = lastFilePath
			fileSize = lastFileInfo.Size()
		}
		file, err := m.openFile(filePath)
		if err != nil {
			return err
		}

		m.baseFilePath = baseFilePath
		m.fileIndex = fileIndex
		m.filePath = filePath
		m.fileSize = fileSize
		if m.file != nil {
			if err = m.file.Close(); err != nil {
				m.logInternalError(fmt.Errorf("close file: %w", err))
			}
		}
		m.file = file
	}

	if m.fileSizeLimit >= 1 && m.fileSize >= m.fileSizeLimit {
		fileIndex := m.fileIndex + 1
		filePath := m.baseFilePath + "." + strconv.Itoa(fileIndex)
		file, err := m.openFile(filePath)
		if err != nil {
			return err
		}

		m.fileIndex = fileIndex
		m.filePath = filePath
		m.fileSize = 0
		if err = m.file.Close(); err != nil {
			m.logInternalError(fmt.Errorf("close file: %w", err))
		}
		m.file = file
	}

	if m.symbolicLinkPath != "" && m.filePath != oldFilePath {
		if err := m.updateSymbolicLink(m.symbolicLinkPath, m.filePath, now); err != nil {
			m.logInternalError(err)
		}
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

func (m *fileManager) updateSymbolicLink(symbolicLinkPath string, filePath string, now time.Time) error {
	linker, ok := m.fs.(afero.Linker)
	if !ok {
		return fmt.Errorf("symbolic link not supported by %v", m.fs.Name())
	}
	if err := m.fs.MkdirAll(filepath.Dir(symbolicLinkPath), 0755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}
	tempSymbolicLinkPath := symbolicLinkPath + "." + strconv.FormatInt(now.UnixNano(), 10) + ".tmp"
	if err := linker.SymlinkIfPossible(filePath, tempSymbolicLinkPath); err != nil {
		return fmt.Errorf("create symbolic link: %w", err)
	}
	if err := m.fs.Rename(tempSymbolicLinkPath, symbolicLinkPath); err != nil {
		m.fs.Remove(tempSymbolicLinkPath)
		return fmt.Errorf("rename symbolic link: %w", err)
	}
	return nil
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
	idleBufferTimeout time.Duration
	logInternalError  func(error)
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
	idleBufferTimeout time.Duration,
	logInternalError func(error),
	go1 func(func()),
	clock clock.Clock,
) io.WriteCloser {
	minLargeWriteSize := int(math.Ceil(float64(bufferSize) * largeWriteThreshold))
	backgroundCtx, cancel := context.WithCancel(context.Background())
	return &bufferedWriteCloser{
		wc:                wc,
		bufferSize:        bufferSize,
		minLargeWriteSize: minLargeWriteSize,
		flushInterval:     flushInterval,
		idleBufferTimeout: idleBufferTimeout,
		logInternalError:  logInternalError,
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

	idleBufferTime := wc.clock.Now()
	for {
		var now time.Time
		select {
		case now = <-ticker.C:
		case <-wc.backgroundCtx.Done():
			return
		}

		if ok := func() bool {
			wc.lock.Lock()
			defer wc.lock.Unlock()

			if wc.hasPendingWrites {
				idleBufferTime = now
				if err := wc.flushLocked(); err != nil {
					wc.logInternalError(err)
				}
			} else {
				if now.Sub(idleBufferTime) >= wc.idleBufferTimeout {
					wc.pendingData = nil
					wc.autoFlusherIsRunning = false
					return false
				}
			}
			return true
		}(); !ok {
			return
		}
	}
}

func (wc *bufferedWriteCloser) flushLocked() error {
	n, err := wc.wc.Write(wc.pendingData)
	if err != nil {
		if n >= 1 {
			copy(wc.pendingData, wc.pendingData[n:])
			wc.pendingData = wc.pendingData[:len(wc.pendingData)-n]
		}
		return fmt.Errorf("commit writes: %w", err)
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

	wc.lock.Lock()
	defer wc.lock.Unlock()

	if wc.isClosed {
		return ErrClosed
	}

	if wc.hasPendingWrites {
		if err := wc.flushLocked(); err != nil {
			wc.logInternalError(err)
		}
		wc.pendingData = nil
	}

	err := wc.wc.Close()
	wc.isClosed = true
	return err
}
