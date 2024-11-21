package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	BULK_STRING = '$'
	ARRAY       = '*'
)

type Reader struct {
	rd *bufio.Reader
}

type RValue struct {
	Str    string
	Values []RValue
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{rd: bufio.NewReader(rd)}
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
		return RValue{}, fmt.Errorf("invalid type: %c", typ)
	}
}

func (r *Reader) readLine() ([]byte, error) {
	line := make([]byte, 0)

	for {
		b, err := r.rd.ReadByte()
		if err != nil {
			return nil, err
		}

		line = append(line, b)

		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], nil
}

func (r *Reader) readLength() (int, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}

	length, err := strconv.Atoi(string(line))
	if err != nil {
		return 0, err
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

	buf := make([]byte, length)

	r.rd.Read(buf)

	v := RValue{}
	v.Str = string(buf)

	// read the trailing CRLF
	r.readLine()

	return v, nil
}
