package filerotate_test

import (
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/roy2220/filerotate"
	"github.com/roy2220/stracetest"
	"github.com/stretchr/testify/require"
)

func init() {
	stracetest.DefaultStraceArgs = []string{"--quiet=all", "--seccomp-bpf"}
}

func TestWriteCloser_WriteError(t *testing.T) {
	synctestT0 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).In(time.Local)

	stracetest.Run(t, "mkdir permission denied", func(t testing.TB, tempDir string) {
		wc := openFileInDir(t, Options{
			FilePathPattern: "logs/foo.log",
			BufferSize:      -1,
		}, tempDir)

		n, err := wc.Write([]byte("abc"))
		require.ErrorContains(t, err, "create directory")
		require.ErrorIs(t, err, syscall.EACCES)
		require.Equal(t, 0, n)
	},
		"--trace-path={{TempDir}}/logs",
		"--trace=mkdir,mkdirat",
		"--inject=mkdir,mkdirat:error=EACCES",
	)

	stracetest.Run(t, "open permission denied", func(t testing.TB, tempDir string) {
		wc := openFileInDir(t, Options{
			FilePathPattern: "foo.log",
			BufferSize:      -1,
		}, tempDir)

		n, err := wc.Write([]byte("abc"))
		require.ErrorContains(t, err, "open file")
		require.ErrorIs(t, err, syscall.EACCES)
		require.Equal(t, 0, n)
	},
		"--trace-path={{TempDir}}/foo.log",
		"--trace=open,openat,openat2",
		"--inject=open,openat,openat2:error=EACCES",
	)

	stracetest.Run(t, "symlink parent mkdir permission denied", func(t testing.TB, tempDir string) {
		var loggedErr error
		wc := openFileInDir(t, Options{
			FilePathPattern:  "foo.log",
			SymbolicLinkPath: "links/foo.log",
			LogInternalError: func(err error) {
				loggedErr = err
			},
			BufferSize: -1,
		}, tempDir)

		n, err := wc.Write([]byte("abc"))
		require.NoError(t, err)
		require.Equal(t, 3, n)

		require.ErrorContains(t, loggedErr, "create directory")
		require.ErrorIs(t, loggedErr, syscall.EACCES)
	},
		"--trace-path={{TempDir}}/links",
		"--trace=mkdir,mkdirat",
		"--inject=mkdir,mkdirat:error=EACCES",
	)

	stracetest.Run(t, "symlink create permission denied", func(t testing.TB, tempDir string) {
		var loggedErr error
		wc := openFileInDir(t, Options{
			FilePathPattern:  "foo.log",
			SymbolicLinkPath: "links/foo.log",
			LogInternalError: func(err error) {
				loggedErr = err
			},
			BufferSize: -1,
		}, tempDir)

		n, err := wc.Write([]byte("abc"))
		require.NoError(t, err)
		require.Equal(t, 3, n)

		require.ErrorContains(t, loggedErr, "create symbolic link")
		require.ErrorIs(t, loggedErr, syscall.EACCES)
	},
		"--trace=symlink,symlinkat",
		"--inject=symlink,symlinkat:error=EACCES",
	)

	stracetest.Run(t, "symlink rename permission denied", func(t testing.TB, tempDir string) {
		var loggedErr error
		wc := openFileInDir(t, Options{
			FilePathPattern:  "foo.log",
			SymbolicLinkPath: "links/foo.log",
			LogInternalError: func(err error) {
				loggedErr = err
			},
			BufferSize: -1,
		}, tempDir)

		n, err := wc.Write([]byte("abc"))
		require.NoError(t, err)
		require.Equal(t, 3, n)

		require.ErrorContains(t, loggedErr, "rename symbolic link")
		require.ErrorIs(t, loggedErr, syscall.EACCES)
	},
		"--trace-path={{TempDir}}/links/foo.log",
		"--trace=rename,renameat,renameat2",
		"--inject=rename,renameat,renameat2:error=EACCES",
	)

	stracetest.Run(t, "stat permission denied", func(t testing.TB, tempDir string) {
		wc := openFileInDir(t, Options{
			FilePathPattern: "foo.log",
			BufferSize:      -1,
		}, tempDir)

		n, err := wc.Write([]byte("abc"))
		require.ErrorContains(t, err, "get file info")
		require.ErrorIs(t, err, syscall.EACCES)
		require.Equal(t, 0, n)
	},
		"--trace-path={{TempDir}}/foo.log",
		"--trace=stat,lstat,newfstatat,statx",
		"--inject=stat,lstat,newfstatat,statx:error=EACCES",
	)

	stracetest.Run(t, "size rotate open permission denied", func(t testing.TB, tempDir string) {
		wc := openFileInDir(t, Options{
			FilePathPattern: "foo.log",
			FileSizeLimit:   3,
			BufferSize:      -1,
		}, tempDir)

		n, err := wc.Write([]byte("abc"))
		require.NoError(t, err)
		require.Equal(t, 3, n)

		n, err = wc.Write([]byte("def"))
		require.ErrorContains(t, err, "open file")
		require.ErrorIs(t, err, syscall.EACCES)
		require.Equal(t, 0, n)
	},
		"--trace-path={{TempDir}}/foo.log.1",
		"--trace=open,openat,openat2",
		"--inject=open,openat,openat2:error=EACCES",
	)

	stracetest.Run(t, "size rotate close failed", func(t testing.TB, tempDir string) {
		var loggedErr error
		wc := openFileInDir(t, Options{
			FilePathPattern: "foo.log",
			FileSizeLimit:   3,
			LogInternalError: func(err error) {
				loggedErr = err
			},
			BufferSize: -1,
		}, tempDir)

		n, err := wc.Write([]byte("abc"))
		require.NoError(t, err)
		require.Equal(t, 3, n)

		n, err = wc.Write([]byte("def"))
		require.NoError(t, err)
		require.Equal(t, 3, n)

		require.ErrorContains(t, loggedErr, "close file")
		require.ErrorIs(t, loggedErr, syscall.EIO)
	},
		"--trace-path={{TempDir}}/foo.log",
		"--trace=close",
		"--inject=close:error=EIO",
	)

	stracetest.Run(t, "time rotate open permission denied", func(t testing.TB, tempDir string) {
		synctest.Test(t.(*testing.T), func(t *testing.T) {
			wc := openFileInDir(t, Options{
				FilePathPattern: "app-%Y-%m-%d_%H-%M.log",
				BufferSize:      -1,
			}, tempDir)

			n, err := wc.Write([]byte("abc"))
			require.NoError(t, err)
			require.Equal(t, 3, n)

			time.Sleep(1 * time.Minute)

			n, err = wc.Write([]byte("def"))
			require.ErrorContains(t, err, "open file")
			require.ErrorIs(t, err, syscall.EACCES)
			require.Equal(t, 0, n)
		})
	},
		"--trace-path={{TempDir}}/"+synctestT0.Add(time.Minute).Format("app-2006-01-02_15-04.log"),
		"--trace=open,openat,openat2",
		"--inject=open,openat,openat2:error=EACCES",
	)

	stracetest.Run(t, "time rotate close failed", func(t testing.TB, tempDir string) {
		synctest.Test(t.(*testing.T), func(t *testing.T) {
			var loggedErr error
			wc := openFileInDir(t, Options{
				FilePathPattern: "app-%Y-%m-%d_%H-%M.log",
				LogInternalError: func(err error) {
					loggedErr = err
				},
				BufferSize: -1,
			}, tempDir)

			n, err := wc.Write([]byte("abc"))
			require.NoError(t, err)
			require.Equal(t, 3, n)

			time.Sleep(1 * time.Minute)

			n, err = wc.Write([]byte("def"))
			require.NoError(t, err)
			require.Equal(t, 3, n)

			require.ErrorContains(t, loggedErr, "close file")
			require.ErrorIs(t, loggedErr, syscall.EIO)
		})
	},
		"--trace-path={{TempDir}}/"+synctestT0.Format("app-2006-01-02_15-04.log"),
		"--trace=close",
		"--inject=close:error=EIO",
	)

	stracetest.Run(t, "write failed", func(t testing.TB, tempDir string) {
		wc := openFileInDir(t, Options{
			FilePathPattern: "foo.log",
			EnsureNewline:   true,
			BufferSize:      -1,
		}, tempDir)

		n, err := wc.Write([]byte("123456789"))
		require.ErrorIs(t, err, syscall.EIO)
		require.Equal(t, 0, n)
	},
		"--trace-path={{TempDir}}/foo.log",
		"--trace=writev",
		"--inject=writev:error=EIO",
	)
}

func TestWriteCloser_CloseError(t *testing.T) {
	stracetest.Run(t, "close failed", func(t testing.TB, tempDir string) {
		wc := openFileInDir(t, Options{
			FilePathPattern: "foo.log",
			EnsureNewline:   true,
			BufferSize:      -1,
		}, tempDir)

		n, err := wc.Write([]byte("abc"))
		require.NoError(t, err)
		require.Equal(t, 3, n)

		err = wc.Close()
		require.ErrorIs(t, err, syscall.EIO)
	},
		"--trace-path={{TempDir}}/foo.log",
		"--trace=close",
		"--inject=close:error=EIO",
	)
}

func TestBufferedWriteCloser_WriteError(t *testing.T) {
	stracetest.Run(t, "direct write failed", func(t testing.TB, tempDir string) {
		wc := openFileInDir(t, Options{
			FilePathPattern:    "foo.log",
			EnsureNewline:      true,
			BufferSize:         100,
			MinDirectWriteSize: 10,
		}, tempDir)

		n, err := wc.Write([]byte("123456789"))
		require.ErrorIs(t, err, syscall.EIO)
		require.Equal(t, 0, n)
	},
		"--trace-path={{TempDir}}/foo.log",
		"--trace=writev",
		"--inject=writev:error=EIO",
	)

	stracetest.Run(t, "buffer flush failed", func(t testing.TB, tempDir string) {
		wc := openFileInDir(t, Options{
			FilePathPattern:    "foo.log",
			EnsureNewline:      true,
			BufferSize:         10,
			MinDirectWriteSize: 10,
		}, tempDir)

		n, err := wc.Write([]byte("123456"))
		require.NoError(t, err)
		require.Equal(t, 6, n)

		n, err = wc.Write([]byte("abcdef"))
		require.ErrorIs(t, err, syscall.EIO)
		require.Equal(t, 0, n)
	},
		"--trace-path={{TempDir}}/foo.log",
		"--trace=writev",
		"--inject=writev:error=EIO",
	)

	stracetest.Run(t, "auto flush failed", func(t testing.TB, tempDir string) {
		synctest.Test(t.(*testing.T), func(t *testing.T) {
			var loggedErr error
			wc := openFileInDir(t, Options{
				FilePathPattern:    "foo.log",
				EnsureNewline:      true,
				BufferSize:         100,
				MinDirectWriteSize: 100,
				FlushInterval:      1 * time.Second,
				LogInternalError: func(err error) {
					loggedErr = err
				},
			}, tempDir)

			n, err := wc.Write([]byte("123456"))
			require.NoError(t, err)
			require.Equal(t, 6, n)

			time.Sleep(1 * time.Second)
			synctest.Wait()

			require.ErrorContains(t, loggedErr, "write file")
			require.ErrorIs(t, loggedErr, syscall.EIO)
		})
	},
		"--trace-path={{TempDir}}/foo.log",
		"--trace=writev",
		"--inject=writev:error=EIO",
	)
}
