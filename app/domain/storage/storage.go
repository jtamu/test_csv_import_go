package storage

import "io"

type Storage interface {
	GetObject(string) (io.Reader, error)
}
