package filerotate

import (
	"os"
	"sync"
	"testing"
)

var RunOutdatedFileCloser = runOutdatedFileCloser

func (o *Options) ApplyDefaults() { o.applyDefaults() }

func (o *Options) SetGo(v func(func())) *Options    { o.go_ = v; return o }
func (o *Options) SetRegistry(v *sync.Map) *Options { o.registry = v; return o }

func (o *Options) Registry() *sync.Map { return o.registry }

type WriteCloser = writeCloser

func (wc *writeCloser) File() *os.File {
	wc.lock.Lock()
	defer wc.lock.Unlock()
	return wc.file
}

type BufferedWriteCloser = bufferedWriteCloser

func (wc *bufferedWriteCloser) File() *os.File { return wc.wc.File() }

func SetOpenFileFlagModifier(t testing.TB, v func(int) int) {
	openFileFlagModifier, v = v, openFileFlagModifier
	t.Cleanup(func() { openFileFlagModifier = v })
}
