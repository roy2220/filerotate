//go:build dragonfly || freebsd || linux || netbsd

package filerotate

import (
	"io"
	"os"
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
			iovecs[i].Base = unsafe.SliceData(first)
			iovecs[i].SetLen(l1)
			i++
		}
		if l2 >= 1 {
			iovecs[i].Base = unsafe.SliceData(second)
			iovecs[i].SetLen(l2)
			i++
		}
		if i == 0 {
			return n, nil
		}

		nn, err := writev(fd, iovecs[:i])
		if err != nil {
			return n, err
		}
		if nn == 0 {
			return 0, io.ErrShortWrite
		}
		n += nn

		j := min(nn, l1)
		first = first[j:]
		nn -= j
		j = min(nn, l2)
		second = second[j:]
	}
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
