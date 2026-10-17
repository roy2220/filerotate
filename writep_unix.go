//go:build unix

package filerotate

import (
	"io"
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

func writep(file *os.File, first, second []byte) (int, error) {
	fd := file.Fd()
	var iovecs [2]syscall.Iovec
	for n := 0; ; {
		l1, l2 := len(first), len(second)
		i := 0
		if l1 >= 1 {
			setIovec(&iovecs[i], first)
			i++
		}
		if l2 >= 1 {
			setIovec(&iovecs[i], second)
			i++
		}
		if i == 0 {
			return n, nil
		}

		nn, err := writev(fd, iovecs[:i])
		runtime.KeepAlive(file)
		if err != nil {
			return n, err
		}
		if nn == 0 {
			return n, io.ErrShortWrite
		}
		n += nn

		j := min(nn, l1)
		first = first[j:]
		nn -= j
		j = min(nn, l2)
		second = second[j:]
	}
}

func setIovec(iovec *syscall.Iovec, base []byte) {
	// for solaris, Iovec.Base is *int8
	*(**byte)(unsafe.Pointer(&iovec.Base)) = unsafe.SliceData(base)
	iovec.SetLen(len(base))
}

func writev(fd uintptr, iovecs []syscall.Iovec) (int, error) {
	for {
		r, _, e := syscall.Syscall(
			syscall.SYS_WRITEV,
			fd,
			uintptr(unsafe.Pointer(&iovecs[0])),
			uintptr(len(iovecs)),
		)
		if e == syscall.EINTR {
			continue
		}
		if e != 0 {
			return 0, e
		}
		return int(r), nil
	}
}
