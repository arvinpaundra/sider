package resp

import (
	"bufio"
	"errors"
	"io"
	"unsafe"
)

const (
	BULK_STRING = '$'
	ARRAY       = '*'
)

var (
	ErrMalformed = errors.New("malformed resp")
	ErrBadLength = errors.New("bad length")
)

type Reader struct {
	rd  *bufio.Reader
	buf []byte
}

type RValue struct {
	Str    string
	Values []RValue
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd:  bufio.NewReaderSize(rd, 32768), // 32KB buffer for pipelined workloads
		buf: make([]byte, 8192),
	}
}

func (r *Reader) Read() (RValue, error) {
	typ, err := r.rd.ReadByte()
	if err != nil {
		return RValue{}, err
	}

	switch typ {
	case BULK_STRING:
		return r.readBulkString()
	case ARRAY:
		return r.readArray()
	default:
		return RValue{}, ErrMalformed
	}
}

//go:inline
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.rd.ReadSlice('\n')
	if err != nil {
		return nil, err
	}

	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, ErrMalformed
	}

	return line[:len(line)-2], nil
}

//go:inline
func (r *Reader) skipCRLF() error {
	cr, err := r.rd.ReadByte()
	if err != nil {
		return err
	}
	if cr != '\r' {
		return ErrMalformed
	}

	lf, err := r.rd.ReadByte()
	if err != nil {
		return err
	}
	if lf != '\n' {
		return ErrMalformed
	}

	return nil
}

//go:inline
func (r *Reader) readLength() (int, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}

	if len(line) == 0 {
		return 0, ErrBadLength
	}

	start := 0
	negative := line[0] == '-'
	if negative {
		start = 1
	}

	length := 0
	for i := start; i < len(line); i++ {
		c := line[i]
		if c < '0' || c > '9' {
			return 0, ErrBadLength
		}
		length = length*10 + int(c-'0')
	}

	if negative {
		return -length, nil
	}
	return length, nil
}

func (r *Reader) readArray() (RValue, error) {
	length, err := r.readLength()
	if err != nil {
		return RValue{}, err
	}

	v := RValue{}
	v.Values = make([]RValue, length)

	for i := 0; i < length; i++ {
		value, err := r.Read()
		if err != nil {
			return RValue{}, err
		}

		v.Values[i] = value
	}

	return v, nil
}

func (r *Reader) readBulkString() (RValue, error) {
	length, err := r.readLength()
	if err != nil {
		return RValue{}, err
	}

	var buf []byte
	needsCopy := false

	if length <= len(r.buf) {
		buf = r.buf[:length]
		needsCopy = true // Must copy since r.buf is reused
	} else {
		buf = make([]byte, length)
		// No copy needed - we own this buffer
	}

	_, err = io.ReadFull(r.rd, buf)
	if err != nil {
		return RValue{}, err
	}

	v := RValue{}
	if needsCopy {
		// Safe copy for small strings
		v.Str = string(buf)
	} else {
		// Zero-copy for large strings - we own the buffer
		v.Str = unsafe.String(unsafe.SliceData(buf), len(buf))
	}

	if err = r.skipCRLF(); err != nil {
		return RValue{}, err
	}

	return v, nil
}
