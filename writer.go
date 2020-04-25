package resp3

import (
	"bytes"
	"io"
	"math"
	"math/big"
	"strconv"
)

// Writer wraps an io.Writer and provides methods for writing the RESP protocol.
type Writer struct {
	w   io.Writer
	buf []byte
}

// NewWriter returns a *Writer that uses the given io.Writer for writes.
func NewWriter(w io.Writer) *Writer {
	var rw Writer
	rw.Reset(w)
	return &rw
}

// Reset sets the underlying io.Writer to w and resets all internal state.
func (rw *Writer) Reset(w io.Writer) {
	rw.w = w
}

func (rw *Writer) writeAggregateHeader(t Type, n int64) error {
	if n == -1 {
		rw.buf = append(rw.buf[:0], byte(t), '?', '\r', '\n')
		_, err := rw.w.Write(rw.buf)
		return err
	}
	if n < 0 {
		return ErrInvalidAggregateTypeLength
	}
	return rw.writeNumber(t, n)
}

func (rw *Writer) writeBlobStreamHeader(t Type) error {
	rw.buf = append(rw.buf[:0], byte(t), '?', '\r', '\n')
	_, err := rw.w.Write(rw.buf)
	return err
}

func (rw *Writer) writeBlob(t Type, s []byte) error {
	rw.buf = rw.buf[:0]
	rw.buf = append(rw.buf, byte(t))
	rw.buf = strconv.AppendUint(rw.buf, uint64(len(s)), 10)
	rw.buf = append(rw.buf, '\r', '\n')
	rw.buf = append(rw.buf, s...)
	rw.buf = append(rw.buf, '\r', '\n')
	_, err := rw.w.Write(rw.buf)
	return err
}

func (rw *Writer) writeNumber(t Type, n int64) error {
	rw.buf = append(rw.buf[:0], byte(t))
	rw.buf = strconv.AppendInt(rw.buf, n, 10)
	rw.buf = append(rw.buf, '\r', '\n')
	_, err := rw.w.Write(rw.buf)
	return err
}

func (rw *Writer) writeSimple(t Type, s []byte) error {
	if bytes.ContainsAny(s, "\r\n") {
		return ErrInvalidSimpleValue
	}
	rw.buf = append(rw.buf[:0], byte(t))
	rw.buf = append(rw.buf, s...)
	rw.buf = append(rw.buf, '\r', '\n')
	_, err := rw.w.Write(rw.buf)
	return err
}

// WriteArrayHeader writes an array header for an array of length n.
//
// If n == -1, the header will denote a streamed aggregation.
// Otherwise if n is < -1, ErrInvalidAggregateTypeLength is returned.
func (rw *Writer) WriteArrayHeader(n int64) error {
	return rw.writeAggregateHeader(TypeArray, n)
}

// WriteAttributeHeader writes an attribute header for an attribute with n field-value items.
//
// If n == -1, the header will denote a streamed aggregation.
// Otherwise if n is < -1, ErrInvalidAggregateTypeLength is returned.
func (rw *Writer) WriteAttributeHeader(n int64) error {
	return rw.writeAggregateHeader(TypeAttribute, n)
}

// WriteBigNumber writes n using the RESP big number type.
func (rw *Writer) WriteBigNumber(n *big.Int) error {
	rw.buf = append(rw.buf[:0], byte(TypeBigNumber))
	rw.buf = n.Append(rw.buf, 10)
	rw.buf = append(rw.buf, '\r', '\n')
	_, err := rw.w.Write(rw.buf)
	return err
}

// WriteBlobChunk writes the byte slice s as blob string chunk.
func (rw *Writer) WriteBlobChunk(s []byte) error {
	if len(s) == 0 {
		rw.buf = append(rw.buf[:0], byte(TypeBlobChunk), '0', '\r', '\n')
		_, err := rw.w.Write(rw.buf)
		return err
	}
	return rw.writeBlob(TypeBlobChunk, s)
}

// WriteBlobErrorStreamHeader writes a blob error stream header.
func (rw *Writer) WriteBlobErrorStreamHeader() error {
	return rw.writeBlobStreamHeader(TypeBlobError)
}

// WriteBlobError writes the byte slice s as blob string.
func (rw *Writer) WriteBlobError(s []byte) error {
	return rw.writeBlob(TypeBlobError, s)
}

// WriteBlobStringStreamHeader writes a blob error stream header.
func (rw *Writer) WriteBlobStringStreamHeader() error {
	return rw.writeBlobStreamHeader(TypeBlobString)
}

// WriteBlobString writes the byte slice s as blob string.
func (rw *Writer) WriteBlobString(s []byte) error {
	return rw.writeBlob(TypeBlobString, s)
}

var boolFalseBytes = []byte("#f\r\n")
var boolTrueBytes = []byte("#t\r\n")

// WriteBoolean writes the boolean b using the RESP boolean type.
func (rw *Writer) WriteBoolean(b bool) error {
	if b {
		_, err := rw.w.Write(boolTrueBytes)
		return err
	}
	_, err := rw.w.Write(boolFalseBytes)
	return err
}

var doubleInfBytes = []byte(",inf\r\n")
var doubleNegativeInfBytes = []byte(",-inf\r\n")

// WriteDouble writes the number f using the RESP double type.
func (rw *Writer) WriteDouble(f float64) error {
	if math.IsInf(f, 1) {
		_, err := rw.w.Write(doubleInfBytes)
		return err
	}
	if math.IsInf(f, -1) {
		_, err := rw.w.Write(doubleNegativeInfBytes)
		return err
	}
	rw.buf = append(rw.buf[:0], byte(TypeDouble))
	rw.buf = strconv.AppendFloat(rw.buf, f, 'f', -1, 64)
	rw.buf = append(rw.buf, '\r', '\n')
	_, err := rw.w.Write(rw.buf)
	return err
}

var endBytes = []byte(".\r\n")

// WriteEnd writes a RESP end value.
func (rw *Writer) WriteEnd() error {
	_, err := rw.w.Write(endBytes)
	return err
}

// WriteMapHeader writes a map header for a map with n field-value items.
//
// If n == -1, the header will denote a streamed aggregation.
// Otherwise if n is < -1, ErrInvalidAggregateTypeLength is returned.
func (rw *Writer) WriteMapHeader(n int64) error {
	return rw.writeAggregateHeader(TypeMap, n)
}

var nullBytes = []byte("_\r\n")

// WriteNull writes a RESP null value.
func (rw *Writer) WriteNull() error {
	_, err := rw.w.Write(nullBytes)
	return err
}

// WriteNumber writes the number i using the RESP integer type.
func (rw *Writer) WriteNumber(n int64) error {
	return rw.writeNumber(TypeNumber, n)
}

// WritePushHeader writes a push header for a push array with n items.
//
// If n == -1, the header will denote a streamed aggregation.
// Otherwise if n is < -1, ErrInvalidAggregateTypeLength is returned.
func (rw *Writer) WritePushHeader(n int64) error {
	return rw.writeAggregateHeader(TypePush, n)
}

// WriteSetHeader writes a set header for a set with n items.
//
// If n == -1, the header will denote a streamed aggregation.
// Otherwise if n is < -1, ErrInvalidAggregateTypeLength is returned.
func (rw *Writer) WriteSetHeader(n int64) error {
	return rw.writeAggregateHeader(TypeSet, n)
}

// WriteSimpleError writes the byte slice s as a simple error.
// If s contains \r or \n, ErrInvalidSimpleValue is returned.
func (rw *Writer) WriteSimpleError(s []byte) error {
	return rw.writeSimple(TypeSimpleError, s)
}

// WriteSimpleString writes the byte slice s as a simple string.
// If s contains \r or \n, ErrInvalidSimpleValue is returned.
func (rw *Writer) WriteSimpleString(s []byte) error {
	return rw.writeSimple(TypeSimpleString, s)
}

const verbatimPrefixLength = 3

// WriteVerbatimString writes the byte slice s unvalidated as a verbatim string using p as prefix.
//
// If len(p) is not 3, ErrInvalidVerbatimStringPrefix will be returned.
func (rw *Writer) WriteVerbatimString(p string, s string) error {
	if len(p) != verbatimPrefixLength {
		return ErrInvalidVerbatimStringPrefix
	}
	rw.buf = append(rw.buf[:0], byte(TypeVerbatimString))
	rw.buf = strconv.AppendInt(rw.buf, int64(len(p)+1+len(s)), 10)
	rw.buf = append(rw.buf, '\r', '\n', p[0], p[1], p[2], ':')
	rw.buf = append(rw.buf, s...)
	rw.buf = append(rw.buf, '\r', '\n')
	_, err := rw.w.Write(rw.buf)
	return err
}
