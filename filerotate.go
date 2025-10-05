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

const (
	defaultBufferSize          = 8 * 1024 * 1024
	defaultFlushInterval       = 1 * time.Second
	defaultMaxIdleBufferAge    = 3
	defaultLargeWriteThreshold = 1 / math.Phi
)

var (
	defaultLogInternalError = func(err error) { log.Println(err) }
	defaultGo               = func(f func()) { go f() }
	defaultClock            = clock.New()
	defaultFs               = afero.NewOsFs()
	defaultRegistry         = &sync.Map{}
)

func init() {
	go closeOutdatedFiles(context.Background(), defaultClock, defaultRegistry)
}

func closeOutdatedFiles(ctx context.Context, clock clock.Clock, registry *sync.Map) {
	ticker := clock.Ticker(1 * time.Minute)

	for {
		var now time.Time
		select {
		case now = <-ticker.C:
		case <-ctx.Done():
			return
		}

		registry.Range(func(key, _ any) bool {
			key.(*fileManager).CloseOutdatedFile(now)
			return true
		})
	}

}

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

	// MaxIdleBufferAge specifies the maximum number of consecutive flush intervals
	// that the buffer can remain idle (without new writes) before the auto-flusher
	// stops and releases the buffer memory.
	// This mechanism prevents the buffer from consuming memory indefinitely during
	// periods of inactivity. The auto-flusher will restart automatically on the next
	// write operation.
	// A non-positive value uses a default of 3.
	MaxIdleBufferAge int

	// LogInternalError specifies a callback function for handling internal errors
	// that occur during background operations (such as auto-flushing failures or
	// file close errors). These errors cannot be returned to the caller directly
	// since they happen asynchronously. If nil, a default logger is used that
	// writes to the standard log.
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

	// Registry is a pointer to a sync.Map that keeps track of internal instances.
	Registry *sync.Map
}

func (o *Options) applyDefaults() {
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
	if o.MaxIdleBufferAge <= 0 {
		o.MaxIdleBufferAge = defaultMaxIdleBufferAge
	}
	if o.LogInternalError == nil {
		o.LogInternalError = defaultLogInternalError
	}
	if o.Go == nil {
		o.Go = defaultGo
	}
	if o.Clock == nil {
		o.Clock = defaultClock
	}
	if o.Fs == nil {
		o.Fs = defaultFs
	}
	if o.Registry == nil {
		o.Registry = defaultRegistry
	}
}

// OpenFile creates a new io.WriteCloser with file rotation and optional buffering
// based on the provided Options.
func OpenFile(options Options) (io.WriteCloser, error) {
	options.applyDefaults()
	if options.FilePathPattern == "" {
		return nil, errors.New("filerotate: no file path pattern")
	}
	var err error
	options.FilePathPattern, err = filepath.Abs(options.FilePathPattern)
	if err != nil {
		return nil, fmt.Errorf("filerotate: get absolute path: %w", err)
	}
	if options.SymbolicLinkPath != "" {
		options.SymbolicLinkPath, err = filepath.Abs(options.SymbolicLinkPath)
		if err != nil {
			return nil, fmt.Errorf("filerotate: get absolute path: %w", err)
		}
	}
	filePathPattern, err := strftime.New(options.FilePathPattern)
	if err != nil {
		return nil, fmt.Errorf("filerotate: invalid file path pattern: %w", err)
	}

	wc := io.WriteCloser(newFileManager(
		filePathPattern,
		options.SymbolicLinkPath,
		options.FileSizeLimit,
		options.LogInternalError,
		options.Clock,
		options.Fs,
		options.Registry,
	))
	if options.BufferSize >= 1 {
		wc = newBufferedWriteCloser(
			wc,
			options.BufferSize,
			options.LargeWriteThreshold,
			options.FlushInterval,
			options.MaxIdleBufferAge,
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
	registry         *sync.Map

	lock         sync.Mutex
	isClosed     bool
	baseFilePath string
	fileIndex    int
	filePath     string
	fileSize     int64
	file         afero.File
	fileIsIdle   bool
}

func newFileManager(
	filePathPattern *strftime.Strftime,
	symbolicLinkPath string,
	fileSizeLimit int64,
	logInternalError func(error),
	clock clock.Clock,
	fs afero.Fs,
	registry *sync.Map,
) *fileManager {
	m := &fileManager{
		filePathPattern:  filePathPattern,
		symbolicLinkPath: symbolicLinkPath,
		fileSizeLimit:    fileSizeLimit,
		logInternalError: logInternalError,
		clock:            clock,
		fs:               fs,
		registry:         registry,
	}
	registry.Store(m, struct{}{})
	return m
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
	m.fileIsIdle = false
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
				return fmt.Errorf("filerotate: get file info: %w", err)
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
		file, err := openFile(m.fs, filePath)
		if err != nil {
			return err
		}

		m.baseFilePath = baseFilePath
		m.fileIndex = fileIndex
		m.filePath = filePath
		m.fileSize = fileSize
		if m.file != nil {
			if err := m.file.Close(); err != nil {
				m.logInternalError(fmt.Errorf("filerotate: close file: %w", err))
			}
		}
		m.file = file
	}

	if m.fileSizeLimit >= 1 && m.fileSize >= m.fileSizeLimit {
		fileIndex := m.fileIndex + 1
		filePath := m.baseFilePath + "." + strconv.Itoa(fileIndex)
		file, err := openFile(m.fs, filePath)
		if err != nil {
			return err
		}

		m.fileIndex = fileIndex
		m.filePath = filePath
		m.fileSize = 0
		if err := m.file.Close(); err != nil {
			m.logInternalError(fmt.Errorf("filerotate: close file: %w", err))
		}
		m.file = file
	}

	if m.symbolicLinkPath != "" && m.filePath != oldFilePath {
		if err := updateSymbolicLink(m.fs, m.symbolicLinkPath, m.filePath, now); err != nil {
			m.logInternalError(err)
		}
	}

	return nil
}

func openFile(fs afero.Fs, filePath string) (afero.File, error) {
	if err := fs.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, fmt.Errorf("filerotate: create directory: %w", err)
	}
	file, err := fs.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("filerotate: open file: %w", err)
	}
	return file, nil
}

func updateSymbolicLink(fs afero.Fs, symbolicLinkPath string, filePath string, now time.Time) error {
	linker, ok := fs.(afero.Linker)
	if !ok {
		return fmt.Errorf("filerotate: symbolic link not supported by %v", fs.Name())
	}
	if err := fs.MkdirAll(filepath.Dir(symbolicLinkPath), 0755); err != nil {
		return fmt.Errorf("filerotate: create directory: %w", err)
	}
	tempSymbolicLinkPath := symbolicLinkPath + "." + strconv.FormatInt(now.UnixNano(), 36) + ".tmp"
	if err := linker.SymlinkIfPossible(filePath, tempSymbolicLinkPath); err != nil {
		return fmt.Errorf("filerotate: create symbolic link: %w", err)
	}
	if err := fs.Rename(tempSymbolicLinkPath, symbolicLinkPath); err != nil {
		fs.Remove(tempSymbolicLinkPath)
		return fmt.Errorf("filerotate: rename symbolic link: %w", err)
	}
	return nil
}

func (m *fileManager) CloseOutdatedFile(now time.Time) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isClosed {
		return
	}
	if m.file == nil {
		return
	}
	if !m.fileIsIdle {
		m.fileIsIdle = true
		return
	}
	if m.baseFilePath == m.filePathPattern.FormatString(now) {
		return
	}

	if err := m.file.Close(); err != nil {
		m.logInternalError(fmt.Errorf("filerotate: close file: %w", err))
	}

	m.baseFilePath = ""
	m.fileIndex = 0
	m.filePath = ""
	m.fileSize = 0
	m.file = nil
	m.fileIsIdle = false
}

func (m *fileManager) Close() error {
	m.registry.Delete(m)

	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isClosed {
		return ErrClosed
	}

	var err error
	if m.file != nil {
		if err := m.file.Close(); err != nil {
			err = fmt.Errorf("filerotate: close file: %w", err)
		}
	}
	m.isClosed = true
	return err
}

type bufferedWriteCloser struct {
	wc                io.WriteCloser
	bufferSize        int
	minLargeWriteSize int
	flushInterval     time.Duration
	maxIdleBufferAge  int
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
	maxIdleBufferAge int,
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
		maxIdleBufferAge:  maxIdleBufferAge,
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

	idleBufferAge := 0
	for {
		select {
		case <-ticker.C:
		case <-wc.backgroundCtx.Done():
			return
		}

		if ok := func() bool {
			wc.lock.Lock()
			defer wc.lock.Unlock()

			if wc.hasPendingWrites {
				idleBufferAge = 0
				if err := wc.flushLocked(); err != nil {
					wc.logInternalError(err)
				}
			} else {
				idleBufferAge++
				if idleBufferAge > wc.maxIdleBufferAge {
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

	wc.lock.Lock()
	defer wc.lock.Unlock()

	if wc.isClosed {
		return ErrClosed
	}

	var err1 error
	if wc.hasPendingWrites {
		err1 = wc.flushLocked()
		wc.pendingData = nil
	}

	err2 := wc.wc.Close()
	wc.isClosed = true
	return errors.Join(err1, err2)
}
