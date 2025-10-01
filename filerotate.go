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
)

const (
	defaultBufferSize          = 8 * 1024 * 1024
	defaultFlushInterval       = 1 * time.Second
	defaultMaxIdleBufferAge    = 3 * time.Second
	defaultLargeWriteThreshold = 1 / math.Phi
)

type Options struct {
	FilePathPattern string
	FileSizeLimit   int64

	// For buffering
	BufferSize          int
	LargeWriteThreshold float64
	FlushInterval       time.Duration
	MaxIdleBufferAge    time.Duration
	LogFlushErr         func(error)
}

type fileManager struct {
	filePathPattern string
	fileSizeLimit   int64

	lock      sync.Mutex
	isClosed  bool
	filePath0 string
	fileIndex int
	fileSize  int64
	file      *os.File
}

func OpenFile(options Options) (io.WriteCloser, error) {
	if options.FilePathPattern == "" {
		return nil, errors.New("filerotate: no file path pattern")
	}
	wc := io.WriteCloser(&fileManager{
		filePathPattern: options.FilePathPattern,
		fileSizeLimit:   options.FileSizeLimit,
	})
	if options.BufferSize >= 0 {
		wc = newBufferedWriteCloser(
			wc,
			options.BufferSize,
			options.LargeWriteThreshold,
			options.FlushInterval,
			options.MaxIdleBufferAge,
			options.LogFlushErr,
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
	filePath0 := time.Now().Format(m.filePathPattern)
	if m.filePath0 != filePath0 {
		lastFileIndex := -1
		var lastFilePath string
		var lastFileInfo os.FileInfo
		for fileIndex, filePath := 0, filePath0; ; {
			fileInfo, err := os.Stat(filePath)
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
		file, err := openFile(filePath)
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
		file, err := openFile(filePath)
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

func openFile(filePath string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
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
	wc               io.WriteCloser
	bufferSize       int
	largeWriteSize   int
	flushInterval    time.Duration
	maxIdleBufferAge time.Duration
	logFlushErr      func(error)

	backgroundCtx context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup

	lock                 sync.Mutex
	isClosing            bool
	isClosed             bool
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
) io.WriteCloser {
	if bufferSize < 1 {
		bufferSize = defaultBufferSize
	}
	if largeWriteThreshold <= 0.0 {
		largeWriteThreshold = defaultLargeWriteThreshold
	} else if largeWriteThreshold > 1.0 {
		largeWriteThreshold = 1.0
	}
	largeWriteSize := int(math.Ceil(float64(bufferSize) * largeWriteThreshold))
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
		wc:               wc,
		bufferSize:       bufferSize,
		largeWriteSize:   largeWriteSize,
		flushInterval:    flushInterval,
		maxIdleBufferAge: maxIdleBufferAge,
		logFlushErr:      logFlushErr,
		backgroundCtx:    backgroundCtx,
		cancel:           cancel,
	}
}

func (wc *bufferedWriteCloser) Write(p []byte) (int, error) {
	wc.lock.Lock()
	defer wc.lock.Unlock()

	if wc.isClosed {
		return 0, ErrClosed
	}

	n := len(p)
	if n == 0 {
		return 0, nil
	}

	if n <= wc.bufferSize-len(wc.pendingData) {
		if len(wc.pendingData) == 0 && n >= wc.largeWriteSize {
			return wc.wc.Write(p)
		}
	} else {
		if len(wc.pendingData) >= 1 {
			if err := wc.flushLocked(); err != nil {
				return 0, err
			}
		}
		if n >= wc.largeWriteSize {
			return wc.wc.Write(p)
		}
	}

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
	go func() {
		defer wc.wg.Done()

		wc.autoFlush()
	}()
	wc.autoFlusherIsRunning = true
}

func (wc *bufferedWriteCloser) autoFlush() {
	ticker := time.NewTicker(wc.flushInterval)
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

			if len(wc.pendingData) >= 1 {
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

	if len(wc.pendingData) >= 1 {
		flushErr = wc.flushLocked()
		wc.pendingData = nil
	}

	err := wc.wc.Close()
	wc.isClosed = true
	return err
}
