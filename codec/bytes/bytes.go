// Package bytes provides a bytes codec which does not encode or decode anything
package bytes

import (
	"fmt"
	"github.com/Just4Ease/axon/v2/codec"
	"io"
	"io/ioutil"
)

type Codec struct {
	Conn io.ReadWriteCloser
}

func (c *Codec) Read(b interface{}) error {
	// read bytes
	buf, err := ioutil.ReadAll(c.Conn)
	if err != nil {
		return err
	}

	switch v := b.(type) {
	case *[]byte:
		*v = buf
	default:
		return fmt.Errorf("failed to read body: %v is not type of *[]byte", b)
	}

	return nil
}

func (c *Codec) Write(b interface{}) error {
	var v []byte
	switch vb := b.(type) {
	case *[]byte:
		v = *vb
	case []byte:
		v = vb
	default:
		return fmt.Errorf("failed to write: %v is not type of *[]byte or []byte", b)
	}
	_, err := c.Conn.Write(v)
	return err
}

func (c *Codec) Close() error {
	return c.Conn.Close()
}

func (c *Codec) String() string {
	return "bytes"
}

func NewCodec(c io.ReadWriteCloser) codec.Codec {
	return &Codec{
		Conn: c,
	}
}
