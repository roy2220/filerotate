//go:build !unix

package filerotate

import (
	"os"
)

func writep(file *os.File, first, second []byte) (int, error) {
	return writepFallback(file, first, second)
}
