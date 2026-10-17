// Package filerotate provides an io.WriteCloser implementation that handles
// time-based and size-based file rotation, optionally combined with a
// self-managing, high-performance write buffer.
package filerotate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/lestrrat-go/strftime"
	"github.com/roy2220/writev"
)

const (
	defaultBufferSize         = 1 * 1024 * 1024
	defaultMinDirectWriteSize = 64 * 1024
	defaultFlushInterval      = 1 * time.Second
	defaultMaxIdleBufferAge   = 3
)

var (
	defaultLogInternalError = func(err error) { log.Println(err) }
	defaultGo               = func(f func()) { go f() }
	defaultRegistry         = &sync.Map{}
)

// Options defines the configuration for file rotation and buffering.
type Options struct {
	// FilePathPattern is the file path pattern in strftime format, e.g.
	// "logs/app-%Y-%m-%d.log". A new file is used whenever the rendered
	// path changes. Missing directories are created automatically.
	FilePathPattern string

	// SymbolicLinkPath is the path of a symbolic link that always points to
	// the active file. An empty string disables it.
	SymbolicLinkPath string

	// FileSizeLimit is the size (in bytes) after which writes go to a
	// new file. A file may slightly exceed it. A zero or negative value
	// disables size-based rotation.
	FileSizeLimit int64

	// EnsureNewline appends '\n' to every write unless it already ends with
	// one. This costs almost nothing: the newline may be gathered with the
	// data via writev(2).
	EnsureNewline bool

	// LogInternalError is called with errors that cannot be returned to the
	// caller, such as background flush failures. If nil, errors are logged
	// via the standard logger.
	LogInternalError func(error)

	// The options below are for buffering.

	// BufferSize is the size (in bytes) of the write buffer. A negative
	// value disables buffering. A zero value uses the default of 1MiB.
	BufferSize int

	// MinDirectWriteSize is the minimum size (in bytes) of a write that
	// skips the buffer and goes straight to the file. A zero or negative
	// value uses the default of 64KiB.
	MinDirectWriteSize int

	// FlushInterval is how often buffered data is flushed automatically.
	// A zero or negative value uses the default of 1 second.
	FlushInterval time.Duration

	// MaxIdleBufferAge is the number of consecutive idle flush intervals
	// after which the buffer's memory is released. A zero or negative value
	// uses the default of 3.
	MaxIdleBufferAge int

	// The options below are for testing only.
	go_      func(func())
	registry *sync.Map
}

func (o *Options) applyDefaults() {
	if o.BufferSize == 0 {
		o.BufferSize = defaultBufferSize
	}
	if o.MinDirectWriteSize < 1 {
		o.MinDirectWriteSize = defaultMinDirectWriteSize
	}
	if o.FlushInterval < 1 {
		o.FlushInterval = defaultFlushInterval
	}
	if o.MaxIdleBufferAge < 1 {
		o.MaxIdleBufferAge = defaultMaxIdleBufferAge
	}
	if o.LogInternalError == nil {
		o.LogInternalError = defaultLogInternalError
	}
	if o.go_ == nil {
		o.go_ = defaultGo
	}
	if o.registry == nil {
		o.registry = defaultRegistry
	}
}

// OpenFile creates a new io.WriteCloser with file rotation and optional
// buffering based on the provided Options.
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

	writeCloser := newWriteCloser(
		filePathPattern,
		options.SymbolicLinkPath,
		options.FileSizeLimit,
		options.EnsureNewline,
		options.LogInternalError,
		options.registry,
	)
	if options.BufferSize < 1 {
		return writeCloser, nil
	}

	return newBufferedWriteCloser(
		writeCloser,
		options.BufferSize,
		options.MinDirectWriteSize,
		options.FlushInterval,
		options.MaxIdleBufferAge,
		options.EnsureNewline,
		options.LogInternalError,
		options.go_,
	), nil
}

// ErrClosed is returned when an operation is attempted on a WriteCloser that
// has already been closed.
var ErrClosed = errors.New("filerotate: closed")

type writeCloser struct {
	filePathPattern  *strftime.Strftime
	symbolicLinkPath string
	fileSizeLimit    int64
	ensureNewline    bool
	logInternalError func(error)
	registry         *sync.Map

	lock                sync.Mutex
	isClosed            bool
	baseFilePathBytes   []byte
	baseFilePathTime    int64
	baseFilePath        string
	fileIndex           int
	filePath            string
	fileSize            int64
	file                *os.File
	fileHasRecentWrites bool
	writer              writev.Writer
	buffers             [3][]byte
}

func newWriteCloser(
	filePathPattern *strftime.Strftime,
	symbolicLinkPath string,
	fileSizeLimit int64,
	ensureNewline bool,
	logInternalError func(error),
	registry *sync.Map,
) *writeCloser {
	m := &writeCloser{
		filePathPattern:  filePathPattern,
		symbolicLinkPath: symbolicLinkPath,
		fileSizeLimit:    fileSizeLimit,
		ensureNewline:    ensureNewline,
		logInternalError: logInternalError,
		registry:         registry,
	}
	registry.Store(m, struct{}{})
	if registry == defaultRegistry {
		startDefaultOutdatedFileCloserOnce()
	}
	return m
}

var startDefaultOutdatedFileCloserOnce = sync.OnceFunc(func() {
	go runOutdatedFileCloser(context.Background(), defaultRegistry)
})

func runOutdatedFileCloser(ctx context.Context, registry *sync.Map) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		var now time.Time
		select {
		case now = <-ticker.C:
		case <-ctx.Done():
			return
		}

		registry.Range(func(key, _ any) bool {
			key.(*writeCloser).CloseOutdatedFile(now)
			return true
		})
	}
}

func (wc *writeCloser) Write(p []byte) (int, error) {
	n := len(p)
	appendNewline := wc.ensureNewline && !(n >= 1 && p[n-1] == '\n')
	return wc.writeBuffers(p, nil, appendNewline)
}

var newline = [1]byte{'\n'}

func (wc *writeCloser) writeBuffers(p1, p2 []byte, appendNewline bool) (int, error) {
	wc.lock.Lock()
	defer wc.lock.Unlock()

	if wc.isClosed {
		return 0, ErrClosed
	}

	if err := wc.rotateFileLocked(); err != nil {
		return 0, err
	}

	if appendNewline {
		wc.buffers = [3][]byte{p1, p2, newline[:]}
	} else {
		wc.buffers = [3][]byte{p1, p2}
	}
	n, err := wc.writer.WriteBuffers(wc.buffers[:])
	clear(wc.buffers[:])

	wc.fileSize += int64(n)
	wc.fileHasRecentWrites = true
	return min(n, len(p1)+len(p2)), err
}

func (wc *writeCloser) rotateFileLocked() error {
	oldFilePath := wc.filePath
	now := time.Now()

	if wc.file == nil {
		if err := wc.openLatestFileLocked(now); err != nil {
			return err
		}
	} else {
		if err := wc.rotateFileForTimeLocked(now); err != nil {
			return err
		}
	}

	if err := wc.rotateFileForSizeLocked(); err != nil {
		return err
	}

	if wc.symbolicLinkPath != "" && wc.filePath != oldFilePath {
		if err := updateSymbolicLink(wc.symbolicLinkPath, wc.filePath, now); err != nil {
			wc.logInternalError(err)
		}
	}

	return nil
}

func (wc *writeCloser) openLatestFileLocked(now time.Time) (returnedErr error) {
	baseFilePath := string(wc.renderBaseFilePathLocked(now))
	latestFileIndex := 0
	latestFilePath := baseFilePath
	var latestFileSize int64
	for fileIndex, filePath := 0, baseFilePath; ; {
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("filerotate: get file info: %w", err)
			}
			break
		}
		latestFileIndex, latestFilePath, latestFileSize = fileIndex, filePath, fileInfo.Size()
		fileIndex++
		filePath = baseFilePath + "." + strconv.Itoa(fileIndex)
	}
	latestFile, err := openFile(latestFilePath, false)
	if err != nil {
		return err
	}
	defer func() {
		if returnedErr != nil {
			latestFile.Close()
		}
	}()
	writer, err := writev.NewWriter(latestFile)
	if err != nil {
		return err
	}

	wc.baseFilePath = baseFilePath
	wc.fileIndex = latestFileIndex
	wc.filePath = latestFilePath
	wc.fileSize = latestFileSize
	wc.file = latestFile
	wc.fileHasRecentWrites = false
	wc.writer = writer
	return nil
}

func (wc *writeCloser) rotateFileForTimeLocked(now time.Time) (returnedErr error) {
	baseFilePathBytes := wc.renderBaseFilePathLocked(now)
	if wc.baseFilePath == string(baseFilePathBytes) {
		return nil
	}
	baseFilePath := string(baseFilePathBytes)

	newFileIndex := 0
	newFilePath := baseFilePath
	var newFile *os.File
	for {
		var err error
		newFile, err = openFile(newFilePath, true)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		// file already exists, that is unexpected, just try next index
		newFileIndex++
		newFilePath = baseFilePath + "." + strconv.Itoa(newFileIndex)
	}
	defer func() {
		if returnedErr != nil {
			newFile.Close()
			os.Remove(newFilePath)
		}
	}()
	writer, err := writev.NewWriter(newFile)
	if err != nil {
		return err
	}

	wc.baseFilePath = baseFilePath
	wc.fileIndex = newFileIndex
	wc.filePath = newFilePath
	wc.fileSize = 0
	if err := wc.file.Close(); err != nil {
		wc.logInternalError(fmt.Errorf("filerotate: close file: %w", err))
	}
	wc.file = newFile
	wc.fileHasRecentWrites = false
	wc.writer = writer
	return nil
}

func (wc *writeCloser) rotateFileForSizeLocked() (returnedErr error) {
	if !(wc.fileSizeLimit >= 1 && wc.fileSize >= wc.fileSizeLimit) {
		return nil
	}

	newFileIndex := wc.fileIndex + 1
	var newFilePath string
	var newFile *os.File
	for {
		newFilePath = wc.baseFilePath + "." + strconv.Itoa(newFileIndex)
		var err error
		newFile, err = openFile(newFilePath, true)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		// file already exists, that is unexpected, just try next index
		newFileIndex++
	}
	defer func() {
		if returnedErr != nil {
			newFile.Close()
			os.Remove(newFilePath)
		}
	}()
	writer, err := writev.NewWriter(newFile)
	if err != nil {
		return err
	}

	wc.fileIndex = newFileIndex
	wc.filePath = newFilePath
	wc.fileSize = 0
	if err := wc.file.Close(); err != nil {
		wc.logInternalError(fmt.Errorf("filerotate: close file: %w", err))
	}
	wc.file = newFile
	wc.fileHasRecentWrites = false
	wc.writer = writer
	return nil
}

var openFileFlagModifier = func(flag int) int { return flag }

func openFile(filePath string, ensureNewFile bool) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, fmt.Errorf("filerotate: create directory: %w", err)
	}
	flag := os.O_WRONLY | os.O_CREATE | os.O_APPEND
	if ensureNewFile {
		flag |= os.O_EXCL
	}
	flag = openFileFlagModifier(flag) // for testing
	file, err := os.OpenFile(filePath, flag, 0644)
	if err != nil {
		return nil, fmt.Errorf("filerotate: open file: %w", err)
	}
	return file, nil
}

func updateSymbolicLink(symbolicLinkPath string, filePath string, now time.Time) error {
	if err := os.MkdirAll(filepath.Dir(symbolicLinkPath), 0755); err != nil {
		return fmt.Errorf("filerotate: create directory: %w", err)
	}
	tempSymbolicLinkPath := symbolicLinkPath + "." + strconv.FormatInt(now.UnixNano(), 36) + ".tmp"
	if err := os.Symlink(filePath, tempSymbolicLinkPath); err != nil {
		return fmt.Errorf("filerotate: create symbolic link: %w", err)
	}
	if err := os.Rename(tempSymbolicLinkPath, symbolicLinkPath); err != nil {
		os.Remove(tempSymbolicLinkPath)
		return fmt.Errorf("filerotate: rename symbolic link: %w", err)
	}
	return nil
}

func (wc *writeCloser) CloseOutdatedFile(now time.Time) {
	wc.lock.Lock()
	defer wc.lock.Unlock()

	if wc.isClosed {
		return
	}
	if wc.file == nil {
		return
	}
	if wc.fileHasRecentWrites {
		wc.fileHasRecentWrites = false
		return
	}
	if wc.baseFilePath == string(wc.renderBaseFilePathLocked(now)) {
		return
	}

	if err := wc.file.Close(); err != nil {
		wc.logInternalError(fmt.Errorf("filerotate: close file: %w", err))
	}

	wc.baseFilePath = ""
	wc.fileIndex = 0
	wc.filePath = ""
	wc.fileSize = 0
	wc.file = nil
	wc.fileHasRecentWrites = false
	wc.writer = writev.Writer{}
}

func (wc *writeCloser) renderBaseFilePathLocked(now time.Time) []byte {
	if unixNow := now.Unix(); wc.baseFilePathTime != unixNow {
		wc.baseFilePathBytes = wc.filePathPattern.FormatBuffer(wc.baseFilePathBytes[:0], now)
		wc.baseFilePathTime = unixNow
	}
	return wc.baseFilePathBytes
}

func (wc *writeCloser) Close() error {
	wc.registry.Delete(wc)

	wc.lock.Lock()
	defer wc.lock.Unlock()

	if wc.isClosed {
		return ErrClosed
	}

	var err error
	if wc.file != nil {
		err = wc.file.Close()
		if err != nil {
			err = fmt.Errorf("filerotate: close file: %w", err)
		}
	}
	wc.isClosed = true
	return err
}

type bufferedWriteCloser struct {
	wc                 *writeCloser
	bufferSize         int
	minDirectWriteSize int
	flushInterval      time.Duration
	maxIdleBufferAge   int
	ensureNewline      bool
	logInternalError   func(error)
	go_                func(func())

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
	writeCloser *writeCloser,
	bufferSize int,
	minDirectWriteSize int,
	flushInterval time.Duration,
	maxIdleBufferAge int,
	ensureNewline bool,
	logInternalError func(error),
	go_ func(func()),
) io.WriteCloser {
	backgroundCtx, cancel := context.WithCancel(context.Background())
	return &bufferedWriteCloser{
		wc:                 writeCloser,
		bufferSize:         bufferSize,
		minDirectWriteSize: minDirectWriteSize,
		flushInterval:      flushInterval,
		ensureNewline:      ensureNewline,
		maxIdleBufferAge:   maxIdleBufferAge,
		logInternalError:   logInternalError,
		go_:                go_,
		backgroundCtx:      backgroundCtx,
		cancel:             cancel,
	}
}

func (wc *bufferedWriteCloser) Write(p []byte) (int, error) {
	n := len(p)
	appendNewline := wc.ensureNewline && !(n >= 1 && p[n-1] == '\n')
	if appendNewline {
		n++
	}

	wc.lock.Lock()
	defer wc.lock.Unlock()

	if wc.isClosed {
		return 0, ErrClosed
	}

	if n >= min(wc.minDirectWriteSize, wc.bufferSize) {
		// direct write
		return wc.flushAndWriteLocked(p, appendNewline)
	}

	if n > wc.bufferSize-len(wc.pendingData) {
		// remaining buffer is insufficient
		if _, err := wc.flushAndWriteLocked(nil, false); err != nil {
			return 0, err
		}
	}

	wc.hasPendingWrites = true
	if wc.pendingData == nil {
		wc.pendingData = make([]byte, 0, wc.bufferSize)
	}
	wc.pendingData = append(wc.pendingData, p...)
	if appendNewline {
		wc.pendingData = append(wc.pendingData, '\n')
	}
	wc.startAutoFlusherLocked()
	return len(p), nil
}

func (wc *bufferedWriteCloser) startAutoFlusherLocked() {
	if wc.autoFlusherIsRunning {
		return
	}
	if wc.isClosing {
		return
	}

	wc.wg.Add(1)
	wc.go_(func() {
		defer wc.wg.Done()

		wc.runAutoFlusher()
	})
	wc.autoFlusherIsRunning = true
}

func (wc *bufferedWriteCloser) runAutoFlusher() {
	ticker := time.NewTicker(wc.flushInterval)
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
				if _, err := wc.flushAndWriteLocked(nil, false); err != nil {
					wc.logInternalError(fmt.Errorf("filerotate: write file: %w", err))
				}
			} else {
				idleBufferAge++
				if idleBufferAge == wc.maxIdleBufferAge {
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

func (wc *bufferedWriteCloser) flushAndWriteLocked(p []byte, appendNewline bool) (int, error) {
	i := len(wc.pendingData)
	j, err := wc.wc.writeBuffers(wc.pendingData, p, appendNewline)
	if j >= i {
		wc.hasPendingWrites = false
		wc.pendingData = wc.pendingData[:0]
		return j - i, err
	}
	if j >= 1 {
		copy(wc.pendingData, wc.pendingData[j:])
		wc.pendingData = wc.pendingData[:i-j]
	}
	return 0, err
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
		_, err1 = wc.flushAndWriteLocked(nil, false)
	}
	wc.pendingData = nil

	err2 := wc.wc.Close()
	wc.isClosed = true
	return errors.Join(err1, err2)
}
