// Package codec is an interface for encoding messages
package codec

import (
	"bytes"
	"errors"
	"github.com/oxtoacart/bpool"
	"io"
	"sync"
)

var (
	ErrInvalidMessage = errors.New("invalid message")
)

// Takes in a connection/buffer and returns a new Codec
type NewCodec func(io.ReadWriteCloser) Codec

// Codec encodes/decodes various types of messages used within axon
// ReadHeader and ReadBody are called in pairs to read requests/responses
// from the connection. Close is called when finished with the
// connection. ReadBody may be called with a nil argument to force the
// body to be read and discarded.
type Codec interface {
	Reader
	Writer
	Close() error
	String() string
}

type Reader interface {
	Read(interface{}) error
}

type Writer interface {
	Write(interface{}) error
}

// Marshaler is a simple encoding interface used for the broker/transport
// where headers are not supported by the underlying implementation.
type Marshaler interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
	String() string
}

type ReadWriteCloser struct {
	sync.RWMutex
	wbuf *bytes.Buffer
	rbuf *bytes.Buffer
}

func NewReadWriteCloser(bp *bpool.SizedBufferPool) *ReadWriteCloser {
	return &ReadWriteCloser{
		RWMutex: sync.RWMutex{},
		wbuf:    bp.Get(),
		rbuf:    bp.Get(),
	}
}

func (rwc *ReadWriteCloser) Read(p []byte) (n int, err error) {
	rwc.RLock()
	defer rwc.RUnlock()
	return rwc.rbuf.Read(p)
}

func (rwc *ReadWriteCloser) Write(p []byte) (n int, err error) {
	rwc.Lock()
	defer rwc.Unlock()
	return rwc.wbuf.Write(p)
}

func (rwc *ReadWriteCloser) Close() error {
	return nil
}
