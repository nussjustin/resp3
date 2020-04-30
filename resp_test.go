package resp3_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/nussjustin/resp3"
)

func assertBytesEqual(tb testing.TB, expected, actual []byte) {
	tb.Helper()
	if !bytes.Equal(expected, actual) {
		if len(expected) > 64 {
			expected = append(expected[:64], "... "+strconv.Itoa(len(expected)-64)+" more characters"...)
		}
		if len(actual) > 64 {
			actual = append(actual[:64], "... "+strconv.Itoa(len(actual)-64)+" more characters"...)
		}
		tb.Errorf("got %q, expected %q", expected, actual)
	}
}

func assertError(tb testing.TB, expected, actual error) {
	tb.Helper()
	if !errors.Is(actual, expected) {
		tb.Errorf("got error %q, expected error %q", actual, expected)
	}
}

func makeCopyAggregateFunc(name string,
	readHeader func(*resp3.Reader) (int64, bool, error),
	writeHeader func(*resp3.Writer, int64) error,
	writeStreamHeader func(*resp3.Writer) error) func(testing.TB, *resp3.ReadWriter, []byte) {
	return func(tb testing.TB, rw *resp3.ReadWriter, _ []byte) {
		n, chunked, err := readHeader(&rw.Reader)
		if err != nil {
			tb.Fatalf("failed to read %s header: %s", name, err)
		}
		if !chunked {
			if err := writeHeader(&rw.Writer, n); err != nil {
				tb.Fatalf("failed to write %s header with size %d: %s", name, n, err)
			}
			return
		}
		if err := writeStreamHeader(&rw.Writer); err != nil {
			tb.Fatalf("failed to write %s stream header: %s", name, err)
		}
	}
}

func makeCopyBlobFunc(name string,
	read func(*resp3.Reader, []byte) ([]byte, bool, error),
	write func(*resp3.Writer, []byte) error,
	writeStreamHeader func(*resp3.Writer) error) func(testing.TB, *resp3.ReadWriter, []byte) {
	return func(tb testing.TB, rw *resp3.ReadWriter, buf []byte) {
		s, chunked, err := read(&rw.Reader, buf)
		if err != nil {
			tb.Fatalf("failed to read %s: %s", name, err)
		}
		if !chunked {
			if err := write(&rw.Writer, s); err != nil {
				tb.Fatalf("failed to write %s %q: %s", name, s, err)
			}
			return
		}
		if err := writeStreamHeader(&rw.Writer); err != nil {
			tb.Fatalf("failed to write %s stream header: %s", name, err)
		}
		for {
			b, last, err := rw.Reader.ReadBlobChunk(nil)
			if err != nil {
				tb.Fatalf("failed to read %s chunk: %s", name, err)
			}
			if err := rw.Writer.WriteBlobChunk(b); err != nil {
				tb.Fatalf("failed to write %s chunk: %s", name, err)
			}
			if last {
				break
			}
		}
	}
}

func makeCopySimpleFunc(name string,
	read func(*resp3.Reader, []byte) ([]byte, error),
	write func(*resp3.Writer, []byte) error) func(testing.TB, *resp3.ReadWriter, []byte) {
	return func(tb testing.TB, rw *resp3.ReadWriter, buf []byte) {
		s, err := read(&rw.Reader, buf)
		if err != nil {
			tb.Fatalf("failed to read %s: %s", name, err)
		}
		if err := write(&rw.Writer, s); err != nil {
			tb.Fatalf("failed to write %s %q: %s", name, s, err)
		}
	}
}

var copyFuncs = [255]func(testing.TB, *resp3.ReadWriter, []byte){
	resp3.TypeInvalid: func(tb testing.TB, rw *resp3.ReadWriter, _ []byte) { tb.Fatal("found invalid type") },
	resp3.TypeArray: makeCopyAggregateFunc("array",
		(*resp3.Reader).ReadArrayHeader,
		(*resp3.Writer).WriteArrayHeader,
		(*resp3.Writer).WriteArrayStreamHeader),
	resp3.TypeAttribute: makeCopyAggregateFunc("attribute",
		(*resp3.Reader).ReadAttributeHeader,
		(*resp3.Writer).WriteAttributeHeader,
		(*resp3.Writer).WriteAttributeStreamHeader),
	resp3.TypeBigNumber: func(tb testing.TB, rw *resp3.ReadWriter, _ []byte) {
		n := new(big.Int)
		if err := rw.ReadBigNumber(n); err != nil {
			tb.Fatalf("failed to read big number: %s", err)
		}
		if err := rw.WriteBigNumber(n); err != nil {
			tb.Fatalf("failed to write big number %q: %s", n, err)
		}
	},
	resp3.TypeBoolean: func(tb testing.TB, rw *resp3.ReadWriter, _ []byte) {
		b, err := rw.ReadBoolean()
		if err != nil {
			tb.Fatalf("failed to read boolean: %s", err)
		}
		if err := rw.WriteBoolean(b); err != nil {
			tb.Fatalf("failed to write boolean %v: %s", b, err)
		}
	},
	resp3.TypeDouble: func(tb testing.TB, rw *resp3.ReadWriter, _ []byte) {
		f, err := rw.ReadDouble()
		if err != nil {
			tb.Fatalf("failed to read double: %s", err)
		}
		if err := rw.WriteDouble(f); err != nil {
			tb.Fatalf("failed to write double %v: %s", f, err)
		}
	},
	resp3.TypeBlobError: makeCopyBlobFunc("blob error",
		(*resp3.Reader).ReadBlobError,
		(*resp3.Writer).WriteBlobError,
		(*resp3.Writer).WriteBlobErrorStreamHeader),
	resp3.TypeBlobString: makeCopyBlobFunc("blob string",
		(*resp3.Reader).ReadBlobString,
		(*resp3.Writer).WriteBlobString,
		(*resp3.Writer).WriteBlobStringStreamHeader),
	resp3.TypeBlobChunk: func(tb testing.TB, rw *resp3.ReadWriter, buf []byte) {
		s, _, err := rw.ReadBlobChunk(buf)
		if err != nil {
			tb.Fatalf("failed to read blob chunk: %s", err)
		}
		if err := rw.WriteBlobChunk(s); err != nil {
			tb.Fatalf("failed to write blob chunk %q: %s", s, err)
		}
	},
	resp3.TypeEnd: func(tb testing.TB, rw *resp3.ReadWriter, _ []byte) {
		if err := rw.ReadEnd(); err != nil {
			tb.Fatalf("failed to read end: %s", err)
		}
		if err := rw.WriteEnd(); err != nil {
			tb.Fatalf("failed to write end: %s", err)
		}
	},
	resp3.TypeMap: makeCopyAggregateFunc("map",
		(*resp3.Reader).ReadMapHeader,
		(*resp3.Writer).WriteMapHeader,
		(*resp3.Writer).WriteMapStreamHeader),
	resp3.TypeNumber: func(tb testing.TB, rw *resp3.ReadWriter, _ []byte) {
		n, err := rw.ReadNumber()
		if err != nil {
			tb.Fatalf("failed to read number: %s", err)
		}
		if err := rw.WriteNumber(n); err != nil {
			tb.Fatalf("failed to write number %d: %s", n, err)
		}
	},
	resp3.TypeNull: func(tb testing.TB, rw *resp3.ReadWriter, _ []byte) {
		if err := rw.ReadNull(); err != nil {
			tb.Fatalf("failed to read null: %s", err)
		}
		if err := rw.WriteNull(); err != nil {
			tb.Fatalf("failed to write null: %s", err)
		}
	},
	resp3.TypePush: makeCopyAggregateFunc("push",
		(*resp3.Reader).ReadPushHeader,
		(*resp3.Writer).WritePushHeader,
		(*resp3.Writer).WritePushStreamHeader),
	resp3.TypeSet: makeCopyAggregateFunc("set",
		(*resp3.Reader).ReadSetHeader,
		(*resp3.Writer).WriteSetHeader,
		(*resp3.Writer).WriteSetStreamHeader),
	resp3.TypeSimpleError: makeCopySimpleFunc("simple error",
		(*resp3.Reader).ReadSimpleError,
		(*resp3.Writer).WriteSimpleError),
	resp3.TypeSimpleString: makeCopySimpleFunc("simple string",
		(*resp3.Reader).ReadSimpleString,
		(*resp3.Writer).WriteSimpleString),
	resp3.TypeVerbatimString: func(tb testing.TB, rw *resp3.ReadWriter, buf []byte) {
		b, err := rw.ReadVerbatimString(buf)
		if err != nil {
			tb.Fatalf("failed to read verbatim string: %s", err)
		}
		if err := rw.WriteVerbatimString(string(b[:3]), string(b[4:])); err != nil {
			tb.Fatalf("failed to write verbatim string %q: %s", string(b), err)
		}
	},
}

func copyReaderToWriter(tb testing.TB, rw *resp3.ReadWriter, buf []byte) {
	if buf == nil {
		buf = make([]byte, 4096)
	}
	for {
		ty, err := rw.Peek()
		if err == io.EOF {
			break
		}
		if err != nil {
			tb.Fatalf("failed to peek at next type: %s", err)
		}

		fn := copyFuncs[ty]
		if fn == nil {
			tb.Fatalf("found unknown type: %#v", ty)
		}
		fn(tb, rw, buf[:0])
	}
}

func TestTypeString(t *testing.T) {
	for ty := resp3.Type(0); ty < ^resp3.Type(0); ty++ {
		if ts := ty.String(); ts != fmt.Sprint(ty) {
			t.Fatalf("got %v, expected %v", ts, fmt.Sprint(ty))
		}
	}
}

type simpleReadWriter struct {
	io.Reader
	io.Writer
}

var testReadWriterInput = strings.Replace(`+
+OK
+OK hello world
-
-ERR
-ERR hello world
:-1000
:-100
:-10
:-1
:0
:1
:10
:100
:1000
$0

$1
a
$5
hello
$11
hello world
*0
*1
*10
.
_
%0
+0
|0
~0
>0
#t
#f
(123456789123456789123456789123456789
=7
foo:bar
,123.456
!5
ERROR
$?
;5
hello
;5
world
;1
!
;0
*?
%?
|?
~?
>?
`, "\n", "\r\n", -1)

func TestReadWriter(t *testing.T) {
	var out bytes.Buffer

	rw := resp3.NewReadWriter(&simpleReadWriter{
		Reader: strings.NewReader(testReadWriterInput),
		Writer: &out,
	})

	copyReaderToWriter(t, rw, nil)

	if outString := out.String(); testReadWriterInput != outString {
		t.Errorf("output differs from input")
		t.Logf("input:\n%s\n", &out)
		t.Logf("output:\n%s\n", &out)
	}
}

func BenchmarkReadWriter(b *testing.B) {
	in := strings.NewReader(testReadWriterInput)
	srw := &simpleReadWriter{
		Reader: in,
		Writer: ioutil.Discard,
	}

	rw := resp3.NewReadWriter(nil)

	buf := make([]byte, 4096)

	b.SetBytes(int64(len(testReadWriterInput)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		in.Reset(testReadWriterInput)
		rw.Reset(srw)

		copyReaderToWriter(b, rw, buf)
	}
}
