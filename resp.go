package resp3

import (
	"errors"
	"fmt"
	"io"
)

var (
	// ErrSingleReadSizeLimitExceeded is returned when reading blob or simple values longer than the configured limit.
	ErrSingleReadSizeLimitExceeded = errors.New("single read size limit exceeded")

	// ErrInvalidAggregateTypeLength is returned when reading or writing an aggregate type header with invalid length.
	ErrInvalidAggregateTypeLength = errors.New("invalid aggregate type length")

	// ErrInvalidBigNumber is returned when decoding an invalid big number.
	ErrInvalidBigNumber = errors.New("invalid big number")

	// ErrInvalidBlobLength is returned when reading or writing a blob string with an invalid length.
	ErrInvalidBlobLength = errors.New("blob string length must be >= 0")

	// ErrInvalidBoolean is returned when decoding an invalid boolean.
	ErrInvalidBoolean = errors.New("invalid boolean")

	// ErrInvalidDouble is returned when decoding an invalid double.
	ErrInvalidDouble = errors.New("invalid double")

	// ErrInvalidNumber is returned when decoding an invalid number.
	ErrInvalidNumber = errors.New("invalid number")

	// ErrInvalidSimpleValue is returned when decoding or encoding a simple error/string that contains either \r or \n.
	ErrInvalidSimpleValue = errors.New("simple errors/strings must not contain \r, \n or both")

	// ErrInvalidType is returned when decoding an unknown type.
	ErrInvalidType = errors.New("invalid type")

	// ErrInvalidVerbatimString is returned when decoding or encoding a verbatim string prefix that has more or
	// less than 3 characters.
	ErrInvalidVerbatimString = errors.New("invalid verbatim string")

	// ErrOverflow is returned when decoding a number that overflows or underflows an int64.
	ErrOverflow = errors.New("number overflowed")

	// ErrUnexpectedEOL is returned when reading a line that does not end in \r\n.
	ErrUnexpectedEOL = errors.New("unexpected EOL")

	// ErrUnexpectedType is returned by Reader when encountering an unknown type.
	ErrUnexpectedType = errors.New("encountered unexpected RESP type")
)

// Type is an enum of the known RESP types with the values of the constants being the single-byte prefix characters.
type Type byte

const (
	// TypeInvalid is used to denote invalid RESP types.
	TypeInvalid Type = 0
	// TypeArray is the RESP protocol type for arrays.
	TypeArray Type = '*'
	// TypeAttribute is the RESP protocol type for attributes.
	TypeAttribute Type = '|'
	// TypeBigNumber is the RESP protocol type for big numbers.
	TypeBigNumber Type = '('
	// TypeBoolean is the RESP protocol type for booleans.
	TypeBoolean Type = '#'
	// TypeDouble is the RESP protocol type for double.
	TypeDouble Type = ','
	// TypeBlobChunk is the RESP protocol type for blob chunks.
	TypeBlobChunk Type = ';'
	// TypeBlobError is the RESP protocol type for blob errors.
	TypeBlobError Type = '!'
	// TypeBlobString is the RESP protocol type for blob strings.
	TypeBlobString Type = '$'
	// TypeEnd is the RESP protocol type for stream ends.
	TypeEnd Type = '.'
	// TypeInteger is the RESP protocol type for 64-bit signed integers.
	TypeInteger Type = ':'
	// TypeMap is the RESP protocol type for maps.
	TypeMap Type = '%'
	// TypeNull is the RESP protocol type for null.
	TypeNull Type = '_'
	// TypePush is the RESP protocol type for push data.
	TypePush Type = '>'
	// TypeSet is the RESP protocol type for sets.
	TypeSet Type = '~'
	// TypeSimpleError is the RESP protocol type for simple errors.
	TypeSimpleError Type = '-'
	// TypeSimpleString is the RESP protocol type for simple strings.
	TypeSimpleString Type = '+'
	// TypeVerbatimString is the RESP protocol type for verbatim strings.
	TypeVerbatimString Type = '='
)

var _ fmt.Stringer = TypeInvalid

var types = [256]Type{
	TypeArray:          TypeArray,
	TypeAttribute:      TypeAttribute,
	TypeBigNumber:      TypeBigNumber,
	TypeBoolean:        TypeBoolean,
	TypeDouble:         TypeDouble,
	TypeBlobError:      TypeBlobError,
	TypeBlobString:     TypeBlobString,
	TypeBlobChunk:      TypeBlobChunk,
	TypeEnd:            TypeEnd,
	TypeInteger:        TypeInteger,
	TypeMap:            TypeMap,
	TypeNull:           TypeNull,
	TypePush:           TypePush,
	TypeSet:            TypeSet,
	TypeSimpleError:    TypeSimpleError,
	TypeSimpleString:   TypeSimpleString,
	TypeVerbatimString: TypeVerbatimString,
}

// String implements the fmt.Stringer interface.
func (t Type) String() string {
	return string(t)
}

// ReadWriter embeds a Reader and a Writer in a single allocation for an io.ReadWriter.
//
// A single Reader and a single Writer method can be called concurrently, given the Read and Write methods of the
// underlying io.ReadWriter are safe for concurrent use.
type ReadWriter struct {
	Reader
	Writer
}

// NewReadWriter returns a new ReadWriter that uses the given io.ReadWriter.
func NewReadWriter(rw io.ReadWriter) *ReadWriter {
	var rrw ReadWriter
	rrw.Reset(rw)
	return &rrw
}

// Reset resets the embedded Reader and Writer to use the given io.ReadWriter.
//
// Reset must not be called concurrently with any other method.
func (rrw *ReadWriter) Reset(rw io.ReadWriter) {
	rrw.Reader.Reset(rw)
	rrw.Writer.Reset(rw)
}

// Marshaler can be implemented by types that can marshal themselves to RESP3 data.
type Marshaler interface {
	// MarshalRESP3 is called when trying to marshal the receiver into a stream of RESP3 data.
	//
	// The given writer must not be retained beyond the call of the method.
	MarshalRESP3(w *Writer) error
}

// Unmarshaler can be implemented by types that can unmarshal themselves from RESP3 data.
type Unmarshaler interface {
	// UnmarshalRESP3 is called when trying to unmarshal data from a stream of RESP3 data.
	//
	// The given reader must not be retained beyond the call of the method.
	UnmarshalRESP3(r *Reader) error
}
