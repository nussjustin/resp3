package resp3

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
)

// Reader wraps an io.Reader and provides methods for reading the RESP protocol.
type Reader struct {
	// SingleReadSizeLimit defines the maximum size of blobs (either errors, strings or chunks) that can can be read,
	// excluding the type, line endings and, in case of blobs, the size. If the Reader encounters a value larger than
	// this limit, an error wrapping ErrSingleReadSizeLimitExceeded will be returned.
	// If SingleReadSizeLimit is 0, DefaultSingleReadSizeLimit is used instead.
	// A negative < 0 value disables the limit.
	SingleReadSizeLimit int

	br *bufio.Reader

	// ownbr holds a *bufio.Reader that is reused when calling Reset. This is used in cases the io.Reader given to
	// Reset is already a *bufio.Reader to avoid reusing the user given *bufio.Reader when calling Reset.
	ownbr *bufio.Reader
}

const (
	// DefaultSingleReadSizeLimit defines the default read limit for values used when Reader.SingleReadSizeLimit is 0.
	DefaultSingleReadSizeLimit = 1 << 25 // 32MiB
)

// NewReader returns a *Reader that uses the given io.Reader for reads.
//
// See Reset for more information on buffering on the given io.Reader works.
func NewReader(r io.Reader) *Reader {
	var rr Reader
	rr.Reset(r)
	return &rr
}

var errUnexpectedEOF = fmt.Errorf("%w: EOF", ErrUnexpectedEOL)

func wrapEOF(err error, msg string, args ...interface{}) error {
	if err != io.EOF {
		return err
	}
	if msg == "" {
		return errUnexpectedEOF
	}
	switch len(args) {
	case 0:
		return fmt.Errorf("%w: expected "+msg+", got EOF", ErrUnexpectedEOL)
	case 1:
		return fmt.Errorf("%w: expected "+msg+", got EOF", ErrUnexpectedEOL, args[0])
	default:
		return fmt.Errorf("%w: expected "+msg+", got EOF", append([]interface{}{ErrUnexpectedEOL}, args...)...)
	}
}

func (rr *Reader) checkReadSizeLimit(n int) error {
	l := rr.SingleReadSizeLimit
	if l == 0 {
		l = DefaultSingleReadSizeLimit
	}
	if l > 0 && l < n {
		return fmt.Errorf("%w: value of size %d exceeds configured limit", ErrSingleReadSizeLimitExceeded, n)
	}
	return nil
}

func (rr *Reader) consume(b []byte) bool {
	if rr.match(b) {
		_, _ = rr.br.Discard(len(b))
		return true
	}
	return false
}

func (rr *Reader) expect(t Type) error {
	g, err := rr.peek()
	if err != nil {
		return wrapEOF(err, "value of type %q", t)
	}
	if g != t {
		return fmt.Errorf("%w: expected %q, got %q", ErrUnexpectedType, t, g)
	}
	_, err = rr.br.Discard(1)
	return err
}

func (rr *Reader) match(b []byte) bool {
	g, err := rr.br.Peek(len(b))
	if err != nil || !bytes.Equal(g, b) {
		return false
	}
	return true
}

func (rr *Reader) peek() (Type, error) {
	b, err := rr.br.Peek(1)
	if err != nil {
		return TypeInvalid, err
	}
	if t := types[b[0]]; t != TypeInvalid {
		return t, nil
	}
	return TypeInvalid, fmt.Errorf("%w: %s", ErrInvalidType, b)
}

func (rr *Reader) readEOL() error {
	b, err := rr.br.Peek(len("\r\n"))
	if err != nil {
		return wrapEOF(err, "\\r\\n")
	}
	if len(b) != 2 || b[0] != '\r' || b[1] != '\n' {
		return fmt.Errorf("%w: expected \\r\\n, got %q", ErrUnexpectedEOL, string(b))
	}
	_, err = rr.br.Discard(len(b))
	return err
}

// Reset sets the underlying io.Reader tor and resets all internal state.
//
// If the given io.Reader is an *bufio.Reader it is used directly without additional buffering.
func (rr *Reader) Reset(r io.Reader) {
	if br, ok := r.(*bufio.Reader); ok {
		rr.br = br
		return
	}

	if rr.ownbr == nil {
		rr.ownbr = bufio.NewReader(r)
	} else {
		rr.ownbr.Reset(r)
	}

	rr.br = rr.ownbr
}

// Peek returns the Type of the next value.
//
// For backwards compatibility with RESP2, if the next value is either an array or
// an blob string with length -1, TypeNull will be returned. ReadNull also handles
// this case and will correctly parse the value, treating it as a normal null value.
func (rr *Reader) Peek() (Type, error) {
	t, err := rr.peek()
	if t == TypeArray || t == TypeBlobString {
		if rr.match([]byte{byte(t), '-', '1', '\r', '\n'}) {
			return TypeNull, nil
		}
	}
	return t, err
}

func (rr *Reader) readDouble() (float64, error) {
	var buf [32]byte
	b, err := rr.readLine(buf[:0])
	if err != nil {
		return 0, err
	}
	if len(b) == 0 {
		return 0, fmt.Errorf("%w: missing value", ErrUnexpectedEOL)
	}
	f, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s", ErrInvalidDouble, string(b))
	}
	return f, nil
}

func (rr *Reader) readNumber() (int64, error) {
	var i int
	var n int64
	var neg bool

loop:
	for i = 0; ; i++ {
		b, err := rr.br.ReadByte()
		if err != nil {
			return 0, wrapEOF(err, "number")
		}

		switch {
		case b == '-' && i == 0:
			neg = true
		case b >= '0' && b <= '9':
			n *= 10
			n += int64(b - '0')
		case b == '\r' || b == '\n':
			_ = rr.br.UnreadByte()
			break loop
		default:
			_ = rr.br.UnreadByte()
			return 0, fmt.Errorf("%w: invalid character %c", ErrInvalidNumber, b)
		}
	}

	if err := rr.readEOL(); err != nil {
		return 0, err
	}
	if i < 1 || (i == 1 && neg) {
		return 0, fmt.Errorf("%w: expected number, got empty value", ErrUnexpectedEOL)
	}

	if neg {
		n *= -1
	}
	return n, nil
}

func (rr *Reader) readChunkableBlob(t Type, dst []byte) ([]byte, bool, error) {
	if rr.consume([]byte{byte(t), '?', '\r', '\n'}) {
		return dst, true, nil
	}
	b, err := rr.readBlob(t, dst)
	return b, false, err
}

func (rr *Reader) readBlob(t Type, dst []byte) ([]byte, error) {
	if err := rr.expect(t); err != nil {
		return nil, err
	}
	n, err := rr.readNumber()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, fmt.Errorf("%w: got length %d", ErrInvalidBlobLength, n)
	}
	b, err := rr.readBlobBody(dst, int(n))
	return b, err
}

func (rr *Reader) readBlobBody(dst []byte, n int) ([]byte, error) {
	if err := rr.checkReadSizeLimit(n); err != nil {
		return nil, err
	}
	dst = ensureSpace(dst, n)
	for n > 0 {
		line, err := rr.br.Peek(n)
		if err != nil && err != bufio.ErrBufferFull {
			return nil, wrapEOF(err, "%d more bytes", n)
		}
		dst = append(dst, line...)
		n -= len(line)
		if _, err := rr.br.Discard(len(line)); err != nil {
			return nil, err
		}
	}
	if err := rr.readEOL(); err != nil {
		return nil, err
	}
	return dst, nil
}

func (rr *Reader) readLine(dst []byte) ([]byte, error) {
	slen := len(dst)
	for {
		line, err := rr.br.ReadSlice('\n')
		if err != nil && err != bufio.ErrBufferFull {
			return nil, wrapEOF(err, "")
		}
		if err := rr.checkReadSizeLimit(len(line) - len("\r\n") + len(dst) - slen); err != nil {
			return nil, err
		}
		dst = append(dst, line...)
		if line[len(line)-1] == '\n' {
			break
		}
	}
	if len(dst) < 2 || dst[len(dst)-2] != '\r' || dst[len(dst)-1] != '\n' {
		return nil, ErrUnexpectedEOL
	}
	return dst[:len(dst)-2], nil
}

func (rr *Reader) readSimple(t Type, dst []byte) ([]byte, error) {
	if err := rr.expect(t); err != nil {
		return nil, err
	}
	return rr.readLine(dst)
}

func ensureSpace(b []byte, n int) []byte {
	if m := cap(b) - len(b); m < n {
		newb := make([]byte, len(b), len(b)+n)
		copy(newb, b)
		return newb
	}
	return b
}

func (rr *Reader) readAggregateHeader(t Type) (int64, bool, error) {
	if rr.consume([]byte{byte(t), '?', '\r', '\n'}) {
		return -1, true, nil
	}
	if err := rr.expect(t); err != nil {
		return 0, false, err
	}
	n, err := rr.readNumber()
	if n < 0 || errors.Is(err, ErrInvalidNumber) {
		n, err = 0, ErrInvalidAggregateTypeLength
	}
	return n, false, err
}

// ReadArrayHeader reads an array header, returning the array length.
//
// If the array is chunked, n will be set to -1 and chunked will be set to true.
// If the next type in the response is not an array, ErrUnexpectedType is returned.
func (rr *Reader) ReadArrayHeader() (n int64, chunked bool, err error) {
	return rr.readAggregateHeader(TypeArray)
}

// ReadAttributeHeader reads an attribute header, returning the attribute size.
//
// If the array is chunked, n will be set to -1 and chunked will be set to true.
// If the next type in the response is not an attribute, ErrUnexpectedType is returned.
func (rr *Reader) ReadAttributeHeader() (n int64, chunked bool, err error) {
	return rr.readAggregateHeader(TypeAttribute)
}

// ReadBigNumber reads a big number from into n.
//
// If the next type in the response is not a big number, ErrUnexpectedType is returned.
func (rr *Reader) ReadBigNumber(n *big.Int) error {
	if err := rr.expect(TypeBigNumber); err != nil {
		return err
	}
	var buf [64]byte
	b, err := rr.readLine(buf[:0])
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return fmt.Errorf("%w: missing value", ErrUnexpectedEOL)
	}
	if _, ok := n.SetString(string(b), 10); !ok {
		return fmt.Errorf("%w: %s", ErrInvalidBigNumber, string(b))
	}
	return nil
}

// ReadBlobChunk reads a blob chunk into b, returning the resulting slice and a boolean indicating
// whether this was the last chunk.
//
// If the next type in the response is not blob chunk, ErrUnexpectedType is returned.
func (rr *Reader) ReadBlobChunk(b []byte) (bb []byte, last bool, err error) {
	if rr.consume([]byte{byte(TypeBlobChunk), '0', '\r', '\n'}) {
		return b, true, nil
	}
	b, err = rr.readBlob(TypeBlobChunk, b)
	return b, false, err
}

// ReadBlobChunks reads one or more blob chunks into b until the end of the blob,  appending
// all chunks to b and returning the resulting slice.
//
// If the next type in the response is not blob chunk, ErrUnexpectedType is returned.
func (rr *Reader) ReadBlobChunks(b []byte) ([]byte, error) {
	for {
		var last bool
		var err error
		if b, last, err = rr.ReadBlobChunk(b); err != nil {
			return nil, err
		} else if last {
			return b, nil
		}
	}
}

// ReadBlobError reads a blob error into b, returning the resulting slice.
//
// If the next type in the response is not blob error, ErrUnexpectedType is returned.
func (rr *Reader) ReadBlobError(b []byte) (bb []byte, chunked bool, err error) {
	return rr.readChunkableBlob(TypeBlobError, b)
}

// ReadBlobString reads a blob string into b, returning the resulting slice.
//
// If the next type in the response is not blob string, ErrUnexpectedType is returned.
func (rr *Reader) ReadBlobString(b []byte) (bb []byte, chunked bool, err error) {
	return rr.readChunkableBlob(TypeBlobString, b)
}

// ReadBoolean reads a boolean.
//
// If the next type in the response is not boolean, ErrUnexpectedType is returned.
func (rr *Reader) ReadBoolean() (bool, error) {
	if err := rr.expect(TypeBoolean); err != nil {
		return false, err
	}
	p, err := rr.br.Peek(len("t\r\n"))
	if err != nil {
		return false, wrapEOF(err, "")
	}
	if p[0] != 't' && p[0] != 'f' {
		return false, fmt.Errorf("%w: expected f or t, got %c", ErrInvalidBoolean, p[1])
	}
	// read here, since the call to discard may invalidate p
	b := p[0] == 't'
	if p[1] != '\r' || p[2] != '\n' {
		return false, fmt.Errorf("%w: expected \\r\\n, got %q", ErrUnexpectedEOL, string(p[1:]))
	}
	if _, err := rr.br.Discard(len(p)); err != nil {
		return false, err
	}
	return b, nil
}

// ReadDouble reads a double.
//
// If the next type in the response is not double, ErrUnexpectedType is returned.
func (rr *Reader) ReadDouble() (float64, error) {
	if err := rr.expect(TypeDouble); err != nil {
		return 0, err
	}
	return rr.readDouble()
}

// ReadEnd reads a stream end marker.
//
// If the next type in the response is not end, ErrUnexpectedType is returned.
func (rr *Reader) ReadEnd() error {
	if err := rr.expect(TypeEnd); err != nil {
		return err
	}
	return rr.readEOL()
}

// ReadMapHeader reads a map header, returning the map size.
//
// If the array is chunked, n will be set to -1 and chunked will be set to true.
// If the next type in the response is not a map, ErrUnexpectedType is returned.
func (rr *Reader) ReadMapHeader() (n int64, chunked bool, err error) {
	return rr.readAggregateHeader(TypeMap)
}

// ReadNull reads a stream end marker.
//
// For backwards compatibility with RESP2, if the next value is either an array or
// an blob string with length -1, ReadNull will treat the value as a normal null
// value.
//
// If the next type in the response is not null, ErrUnexpectedType is returned.
func (rr *Reader) ReadNull() error {
	ty, err := rr.peek()
	if err != nil {
		return wrapEOF(err, "value of type %q", TypeNull)
	}
	if ty == TypeArray || ty == TypeBlobString {
		if rr.consume([]byte{byte(ty), '-', '1', '\r', '\n'}) {
			return nil
		}
	}
	if err := rr.expect(TypeNull); err != nil {
		return err
	}
	return rr.readEOL()
}

// ReadNumber reads a number.
//
// If the next type in the response is not number, ErrUnexpectedType is returned.
func (rr *Reader) ReadNumber() (int64, error) {
	if err := rr.expect(TypeNumber); err != nil {
		return 0, err
	}
	return rr.readNumber()
}

// ReadPushHeader reads a push header, returning the push size.
//
// If the array is chunked, n will be set to -1 and chunked will be set to true.
// If the next type in the response is not a push, ErrUnexpectedType is returned.
func (rr *Reader) ReadPushHeader() (n int64, chunked bool, err error) {
	return rr.readAggregateHeader(TypePush)
}

// ReadSetHeader reads a set header, returning the set size.
//
// If the array is chunked, n will be set to -1 and chunked will be set to true.
// If the next type in the response is not a set, ErrUnexpectedType is returned.
func (rr *Reader) ReadSetHeader() (n int64, chunked bool, err error) {
	return rr.readAggregateHeader(TypeSet)
}

// ReadSimpleError reads a simple error into b, returning the resulting slice.
//
// If the next type in the response is not simple error, ErrUnexpectedType is returned.
func (rr *Reader) ReadSimpleError(b []byte) ([]byte, error) {
	return rr.readSimple(TypeSimpleError, b)
}

// ReadSimpleString reads a simple string into b, returning the resulting slice.
//
// If the next type in the response is not simple string, ErrUnexpectedType is returned.
func (rr *Reader) ReadSimpleString(b []byte) ([]byte, error) {
	return rr.readSimple(TypeSimpleString, b)
}

// ReadVerbatimString reads a verbatim string into b, returning the resulting slice
//
// If the next type in the response is not simple string, ErrUnexpectedType is returned.
func (rr *Reader) ReadVerbatimString(b []byte) ([]byte, error) {
	oldLen := len(b)
	b, err := rr.readBlob(TypeVerbatimString, b)
	if err != nil {
		return nil, err
	}
	if bs := b[oldLen:]; len(bs) < verbatimPrefixLength+1 || bs[verbatimPrefixLength] != ':' {
		p := bs
		if n := verbatimPrefixLength*verbatimPrefixLength + 1; len(p) >= n {
			p = p[:n]
		}
		return nil, fmt.Errorf("%w: %q", ErrInvalidVerbatimStringPrefix, string(p))
	}
	return b, nil
}
