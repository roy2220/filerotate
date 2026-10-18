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
	defaultFlushThreshold      = 0.75
	defaultLargeWriteThreshold = 1 / math.Phi
	defaultMaxIdleBufferAge    = 3
)

var (
	defaultLogInternalError = func(err error) { log.Println(err) }
	defaultGo               = func(f func()) { go f() }
	defaultClock            = clock.New()
	defaultFs               = afero.NewOsFs()
	defaultRegistry         = &sync.Map{}
)

var startDefaultOutdatedFileCloserOnce = sync.OnceFunc(func() {
	go runOutdatedFileCloser(context.Background(), defaultClock, defaultRegistry)
})

func runOutdatedFileCloser(ctx context.Context, clock clock.Clock, registry *sync.Map) {
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
	// FilePathPattern specifies the pattern for file naming. It
	// uses strftime format for date/time substitution. For example:
	// "logs/app-%Y-%m-%d.log"
	FilePathPattern string

	// SymbolicLinkPath is the path for a symbolic link that points to the
	// currently active log file. An empty string disables the symbolic link
	// feature. This link is atomically updated after each file rotation.
	SymbolicLinkPath string

	// FileSizeLimit is the maximum size (in bytes) a single log file
	// can reach before rotation occurs. A non-positive value disables
	// size-based rotation.
	FileSizeLimit int64

	// EnsureNewline guarantees that every write operation to the file ends
	// with a newline character ('\n'), unless the written data already
	// does. This option is generally more efficient when used with a buffer
	// (BufferSize > 0), as manually appending a newline to the data before
	// calling Write() can lead to redundant memory allocation and copying.
	EnsureNewline bool

	// LogInternalError specifies a callback function for handling internal
	// errors that occur during background operations (such as automatic
	// flushing failures or file close errors). If nil, a default logger is
	// used that writes to the standard output.
	LogInternalError func(error)

	// The options below serve for buffering.

	// BufferSize is the size (in bytes) of the internal buffer. A zero
	// value uses a default size of 8MB If a buffered writer is not desired,
	// set it to a negative value (e.g., -1).
	BufferSize int

	// FlushThreshold is a ratio (0.0 to 1.0) of BufferSize. TODO
	FlushThreshold float64

	// FlushInterval specifies how often the buffer should be automatically
	// flushed. A non-positive value uses a default interval of 1 second.
	FlushInterval time.Duration

	// LargeWriteThreshold is a ratio (0.0 to 1.0) of BufferSize. If a
	// single write operation is larger than this threshold, it will bypass
	// the buffer and write directly to the underlying file. A non-positive
	// value uses a default threshold based on the golden ratio (0.618).
	LargeWriteThreshold float64

	// MaxIdleBufferAge specifies the maximum number of consecutive flush
	// intervals that the buffer can remain idle (without new writes)
	// before the buffer's memory gets reclaimed. This mechanism prevents
	// the buffer from consuming memory indefinitely during periods of
	// inactivity. A non-positive value uses a default of 3.
	MaxIdleBufferAge int

	// The options below serve for testing.

	// Go specifies the function to use for starting background goroutines.
	Go func(func())

	// Clock specifies the clock interface to use for time-related
	// operations. If nil, the real system clock is used.
	Clock clock.Clock

	// Fs specifies the filesystem interface to use. If nil, the local OS
	// filesystem is used.
	Fs afero.Fs

	// Registry is a pointer to a sync.Map that keeps track of internal
	// instances.
	Registry *sync.Map
}

func (o *Options) applyDefaults() {
	if o.BufferSize == 0 {
		o.BufferSize = defaultBufferSize
	}
	if o.FlushThreshold <= 0.0 {
		o.FlushThreshold = defaultFlushThreshold
	} else if o.FlushThreshold > 1.0 {
		o.FlushThreshold = 1.0
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = defaultFlushInterval
	}
	if o.LargeWriteThreshold <= 0.0 {
		o.LargeWriteThreshold = defaultLargeWriteThreshold
	} else if o.LargeWriteThreshold > 1.0 {
		o.LargeWriteThreshold = 1.0
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

	fileManager := newFileManager(
		filePathPattern,
		options.SymbolicLinkPath,
		options.FileSizeLimit,
		options.EnsureNewline,
		options.LogInternalError,
		options.Clock,
		options.Fs,
		options.Registry,
	)
	startDefaultOutdatedFileCloserOnce()
	if options.BufferSize < 1 {
		return fileManager, nil
	}
	return newBufferedFileManager(
		fileManager,
		options.BufferSize,
		options.FlushThreshold,
		options.FlushInterval,
		options.LargeWriteThreshold,
		options.MaxIdleBufferAge,
		options.EnsureNewline,
		options.LogInternalError,
		options.Go,
		options.Clock,
	), nil
}

// ErrClosed is returned when an operation is attempted on a WriteCloser that
// has already been closed.
var ErrClosed = errors.New("filerotate: closed")

type fileManager struct {
	filePathPattern  *strftime.Strftime
	symbolicLinkPath string
	fileSizeLimit    int64
	ensureNewline    bool
	logInternalError func(error)
	clock            clock.Clock
	fs               afero.Fs
	registry         *sync.Map

	lock                sync.Mutex
	isClosed            bool
	baseFilePath        string
	fileIndex           int
	filePath            string
	fileSize            int64
	file                afero.File
	fileHasRecentWrites bool
}

func newFileManager(
	filePathPattern *strftime.Strftime,
	symbolicLinkPath string,
	fileSizeLimit int64,
	ensureNewline bool,
	logInternalError func(error),
	clock clock.Clock,
	fs afero.Fs,
	registry *sync.Map,
) *fileManager {
	m := &fileManager{
		filePathPattern:  filePathPattern,
		symbolicLinkPath: symbolicLinkPath,
		fileSizeLimit:    fileSizeLimit,
		ensureNewline:    ensureNewline,
		logInternalError: logInternalError,
		clock:            clock,
		fs:               fs,
		registry:         registry,
	}
	registry.Store(m, struct{}{})
	return m
}

var newline = [1]byte{'\n'}

func (m *fileManager) Write(p []byte) (int, error) {
	if n := len(p); m.ensureNewline && !(n >= 1 && p[n-1] == '\n') {
		nn, err := m.writep(p, newline[:])
		return min(nn, n), err
	}
	return m.writep(p, nil)
}

func (m *fileManager) writep(first, second []byte) (int, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isClosed {
		return 0, ErrClosed
	}

	if err := m.rotateFileLocked(); err != nil {
		return 0, err
	}

	var (
		n   int
		err error
	)
	if file, ok := m.file.(*os.File); ok {
		n, err = writep(file, first, second)
	} else {
		n, err = m.file.Write(first)
		if err == nil {
			var n2 int
			n2, err = m.file.Write(second)
			n += n2
		}
	}
	m.fileSize += int64(n)
	m.fileHasRecentWrites = true
	return n, err
}

func (m *fileManager) rotateFileLocked() error {
	oldFilePath := m.filePath
	now := m.clock.Now()

	if m.file == nil {
		if err := m.openLastFileLocked(now); err != nil {
			return err
		}
	} else {
		if err := m.rotateFileForTimeLocked(now); err != nil {
			return err
		}
	}

	if err := m.rotateFileForSizeLocked(); err != nil {
		return err
	}

	if m.symbolicLinkPath != "" && m.filePath != oldFilePath {
		if err := updateSymbolicLink(m.fs, m.symbolicLinkPath, m.filePath, now); err != nil {
			m.logInternalError(err)
		}
	}

	return nil
}

func (m *fileManager) openLastFileLocked(now time.Time) error {
	baseFilePath := m.filePathPattern.FormatString(now)
	lastFileIndex := 0
	lastFilePath := baseFilePath
	var lastFileSize int64
	for curFileIndex, curFilePath := 0, baseFilePath; ; {
		curFileInfo, err := m.fs.Stat(curFilePath)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("filerotate: get file info: %w", err)
			}
			break
		}
		lastFileIndex, lastFilePath, lastFileSize = curFileIndex, curFilePath, curFileInfo.Size()
		curFileIndex++
		curFilePath = baseFilePath + "." + strconv.Itoa(curFileIndex)
	}
	lastFile, err := openFile(m.fs, lastFilePath, false)
	if err != nil {
		return err
	}

	m.baseFilePath = baseFilePath
	m.fileIndex = lastFileIndex
	m.filePath = lastFilePath
	m.fileSize = lastFileSize
	m.file = lastFile
	return nil
}

func (m *fileManager) rotateFileForTimeLocked(now time.Time) error {
	baseFilePath := m.filePathPattern.FormatString(now)
	if m.baseFilePath == baseFilePath {
		return nil
	}

	fileIndex := 0
	filePath := baseFilePath
	var file afero.File
	for {
		var err error
		file, err = openFile(m.fs, filePath, true)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		// file already exists, that is unexpected, just try next index
		fileIndex++
		filePath = baseFilePath + "." + strconv.Itoa(fileIndex)
	}

	m.baseFilePath = baseFilePath
	m.fileIndex = fileIndex
	m.filePath = filePath
	m.fileSize = 0
	if err := m.file.Close(); err != nil {
		m.logInternalError(fmt.Errorf("filerotate: close file: %w", err))
	}
	m.file = file
	return nil
}

func (m *fileManager) rotateFileForSizeLocked() error {
	if !(m.fileSizeLimit >= 1 && m.fileSize >= m.fileSizeLimit) {
		return nil
	}

	fileIndex := m.fileIndex + 1
	var filePath string
	var file afero.File
	for {
		filePath = m.baseFilePath + "." + strconv.Itoa(fileIndex)
		var err error
		file, err = openFile(m.fs, filePath, true)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		// file already exists, that is unexpected, just try next index
		fileIndex++
	}

	m.fileIndex = fileIndex
	m.filePath = filePath
	m.fileSize = 0
	if err := m.file.Close(); err != nil {
		m.logInternalError(fmt.Errorf("filerotate: close file: %w", err))
	}
	m.file = file
	return nil
}

func openFile(fs afero.Fs, filePath string, ensureNewFile bool) (afero.File, error) {
	if err := fs.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, fmt.Errorf("filerotate: create directory: %w", err)
	}
	flag := os.O_WRONLY | os.O_CREATE | os.O_APPEND
	if ensureNewFile {
		flag |= os.O_EXCL
	}
	file, err := fs.OpenFile(filePath, flag, 0644)
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
	if m.fileHasRecentWrites {
		m.fileHasRecentWrites = false
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
		err = m.file.Close()
		if err != nil {
			err = fmt.Errorf("filerotate: close file: %w", err)
		}
	}
	m.isClosed = true
	return err
}

type bufferedFileManager struct {
	m                 *fileManager
	bufferSize        int
	minFlushSize      int
	flushInterval     time.Duration
	minLargeWriteSize int
	maxIdleBufferAge  int
	ensureNewline     bool
	logInternalError  func(error)
	go_               func(func())
	clock             clock.Clock

	backgroundCtx context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup

	lock             sync.Mutex
	isClosing        bool
	isClosed         bool
	buffer           []byte
	bufferStates     [2]*bufferState
	flusherTrigger   chan struct{}
	flusherIsRunning bool
}

type bufferState struct {
	WG         sync.WaitGroup
	DataOffset int
	DataLength int
	HasData    bool
	Task       []any
}

type flushTask struct {
	WG  sync.WaitGroup
	Err error
}

type largeWriteTask struct {
	First  []byte
	Second []byte
	WG     sync.WaitGroup
	N      int
	Err    error
}

func newBufferedFileManager(
	fileManager *fileManager,
	bufferSize int,
	flushThreshold float64,
	flushInterval time.Duration,
	largeWriteThreshold float64,
	maxIdleBufferAge int,
	ensureNewline bool,
	logInternalError func(error),
	go_ func(func()),
	clock clock.Clock,
) io.WriteCloser {
	minFlushSize := int(math.Ceil(float64(bufferSize) * flushThreshold))
	minLargeWriteSize := int(math.Ceil(float64(bufferSize) * largeWriteThreshold))
	backgroundCtx, cancel := context.WithCancel(context.Background())
	var bufferStates [2]bufferState
	return &bufferedFileManager{
		m:                 fileManager,
		bufferSize:        bufferSize,
		minFlushSize:      minFlushSize,
		flushInterval:     flushInterval,
		minLargeWriteSize: minLargeWriteSize,
		maxIdleBufferAge:  maxIdleBufferAge,
		ensureNewline:     ensureNewline,
		logInternalError:  logInternalError,
		go_:               go_,
		clock:             clock,
		backgroundCtx:     backgroundCtx,
		cancel:            cancel,
		bufferStates:      [2]*bufferState{&bufferStates[0], &bufferStates[1]},
		flusherTrigger:    make(chan struct{}, 1),
	}
}

func (m *bufferedFileManager) Write(p []byte) (int, error) {
	n := len(p)
	appendNewline := m.ensureNewline && !(n >= 1 && p[n-1] == '\n')
	var nn int
	if appendNewline {
		nn = n + 1
	} else {
		nn = n
	}

	lock := &m.lock
	lock.Lock()
	defer func() {
		if lock != nil {
			lock.Unlock()
		}
	}()

	if m.isClosed {
		return 0, ErrClosed
	}
	bufferState := m.bufferStates[0]

	if nn >= m.minLargeWriteSize {
		// large write
		task := &largeWriteTask{First: p}
		if appendNewline {
			task.Second = newline[:]
		}
		task.WG.Add(1)
		bufferState.Task = append(bufferState.Task, task)
		m.startFlusherLocked()
		lock.Unlock()
		lock = nil

		m.triggerFlusher()
		task.WG.Wait()
		return min(task.N, n), task.Err
	}

	for nn > m.bufferSize-bufferState.DataLength {
		// remaining buffer space is insufficient
		task := &flushTask{}
		task.WG.Add(1)
		bufferState.Task = append(bufferState.Task, task)
		m.startFlusherLocked()
		lock.Unlock()
		lock = nil

		m.triggerFlusher()
		task.WG.Wait()
		if task.Err != nil {
			return 0, task.Err
		}

		lock = &m.lock
		lock.Lock()

		if m.isClosed {
			return 0, ErrClosed
		}
		bufferState = m.bufferStates[0]
	}

	// remaining buffer space is sufficient
	b := m.buffer
	if b == nil {
		b = make([]byte, m.bufferSize)
		m.buffer = b
	}
	i := (bufferState.DataOffset + bufferState.DataLength) % m.bufferSize
	j := (bufferState.DataOffset + bufferState.DataLength + n) % m.bufferSize
	bufferState.WG.Add(1)
	bufferState.DataLength += nn
	bufferState.HasData = true // even if nn == 0
	m.startFlusherLocked()
	flushNow := bufferState.DataLength >= m.minFlushSize
	lock.Unlock()
	lock = nil

	if flushNow {
		m.triggerFlusher()
	}

	// copy data to buffer without locking
	p = p[copy(b[i:], p):]
	if len(p) >= 1 {
		copy(b, p)
	}
	if appendNewline {
		b[j] = '\n'
	}
	bufferState.WG.Done()

	return n, nil
}

func (m *bufferedFileManager) triggerFlusher() {
	select {
	case m.flusherTrigger <- struct{}{}:
	default:
	}
}

func (m *bufferedFileManager) startFlusherLocked() {
	if m.flusherIsRunning {
		return
	}
	if m.isClosing {
		return
	}

	m.wg.Add(1)
	m.go_(func() {
		defer m.wg.Done()

		m.runFlusher()
	})
	m.flusherIsRunning = true
}

func (m *bufferedFileManager) runFlusher() {
	ticker := m.clock.Ticker(m.flushInterval)
	defer ticker.Stop()

	idleBufferAge := 0
	for {
		isTimedOut := false
		select {
		case <-m.flusherTrigger:
		case <-ticker.C:
			isTimedOut = true
		case <-m.backgroundCtx.Done():
			return
		}

		m.lock.Lock()
		bufferState := m.bufferStates[0]
		if isTimedOut && !bufferState.HasData && bufferState.Task == nil {
			idleBufferAge++
			if idleBufferAge > m.maxIdleBufferAge {
				m.buffer = nil
				bufferState.DataOffset = 0
				bufferState.DataLength = 0
				m.flusherIsRunning = false
				m.lock.Unlock()
				return
			}
			m.lock.Unlock()
			continue
		}
		bufferState, frozenBufferState := m.swapBufferStatesLocked()
		m.lock.Unlock()

		if err := m.processTasks(bufferState, frozenBufferState); err != nil {
			m.logInternalError(err)
		}

		idleBufferAge = 0
	}
}

func (m *bufferedFileManager) swapBufferStatesLocked() (*bufferState, *bufferState) {
	bs0 := m.bufferStates[0]
	bs1 := m.bufferStates[1]
	bs1.DataOffset = bs0.DataOffset
	bs1.DataLength = bs0.DataLength
	bs1.HasData = bs0.HasData
	m.bufferStates[0] = bs1
	m.bufferStates[1] = bs0
	return bs1, bs0
}

func (m *bufferedFileManager) processTasks(bufferState, frozenBufferState *bufferState) error {
	err := m.flush(bufferState, frozenBufferState)
	if frozenBufferState.Task == nil {
		return err
	}
	for _, task := range frozenBufferState.Task {
		switch task := task.(type) {
		case *flushTask:
			task.Err = err
			task.WG.Done()
		case *largeWriteTask:
			if err == nil {
				task.N, task.Err = m.m.writep(task.First, task.Second)
				err = task.Err
			} else {
				task.Err = err
			}
			task.WG.Done()
		default:
			panic("unreachable")
		}
	}
	frozenBufferState.Task = nil
	// err will be handled by task submitters
	return nil
}

func (m *bufferedFileManager) flush(bufferState, frozenBufferState *bufferState) error {
	if !frozenBufferState.HasData {
		return nil
	}
	frozenBufferState.WG.Wait()
	var (
		n   int
		err error
	)
	if i := frozenBufferState.DataOffset + frozenBufferState.DataLength; i <= m.bufferSize {
		n, err = m.m.writep(m.buffer[frozenBufferState.DataOffset:i], nil)
	} else {
		n, err = m.m.writep(m.buffer[frozenBufferState.DataOffset:], m.buffer[:i-m.bufferSize])
	}
	if bufferState != nil {
		m.lock.Lock()
		bufferState.DataOffset = (bufferState.DataOffset + n) % m.bufferSize
		bufferState.DataLength -= n
		bufferState.HasData = bufferState.DataLength >= 1
		m.lock.Unlock()
	}
	return err
}

func (m *bufferedFileManager) Close() error {
	m.lock.Lock()
	m.isClosing = true
	m.lock.Unlock()

	m.cancel()
	m.wg.Wait()

	m.lock.Lock()
	defer m.lock.Unlock()

	if m.isClosed {
		return ErrClosed
	}

	err1 := m.processTasks(nil, m.bufferStates[0])
	m.buffer = nil

	err2 := m.m.Close()
	m.isClosed = true

	return errors.Join(err1, err2)
}
