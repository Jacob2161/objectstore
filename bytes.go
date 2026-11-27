package objectstore

import "bytes"

// newBytesReader wraps content as a seekable body.
func newBytesReader(content []byte) *bytes.Reader {
	return bytes.NewReader(content)
}
