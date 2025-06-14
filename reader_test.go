package resp3_test

import (
	"bufio"
	"bytes"
	"io"
	"maps"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/nussjustin/resp3"
)

func assertReadResultEqual(tb testing.TB, expected, actual []byte, expectedErr, actualErr error) {
	tb.Helper()
	assertError(tb, expectedErr, actualErr)
	if expectedErr == nil && actualErr == nil {
		assertBytesEqual(tb, expected, actual)
	} else {
		assertBytesEqual(tb, nil, actual)
	}
}

func TestReaderReset(t *testing.T) {
	rr := resp3.NewReader(strings.NewReader(""))
	assertError(t, resp3.ErrUnexpectedEOL, rr.ReadEnd())
	rr.Reset(strings.NewReader(".\r\n"))
	assertError(t, nil, rr.ReadEnd())
	assertError(t, resp3.ErrUnexpectedEOL, rr.ReadEnd())
	rr.Reset(strings.NewReader(".\r\n"))
	assertError(t, nil, rr.ReadEnd())
	assertError(t, resp3.ErrUnexpectedEOL, rr.ReadEnd())
}

func TestReaderPeek(t *testing.T) {
	types := map[resp3.Type]bool{
		resp3.TypeArray:          true,
		resp3.TypeAttribute:      true,
		resp3.TypeBigNumber:      true,
		resp3.TypeBoolean:        true,
		resp3.TypeDouble:         true,
		resp3.TypeBlobError:      true,
		resp3.TypeBlobString:     true,
		resp3.TypeBlobChunk:      true,
		resp3.TypeEnd:            true,
		resp3.TypeInteger:        true,
		resp3.TypeMap:            true,
		resp3.TypeNull:           true,
		resp3.TypePush:           true,
		resp3.TypeSet:            true,
		resp3.TypeSimpleError:    true,
		resp3.TypeSimpleString:   true,
		resp3.TypeVerbatimString: true,
	}

	for i := byte(0); i < ^byte(0); i++ {
		rr, _ := newTestReader(string([]byte{i}))

		ty, err := rr.Peek()
		if types[resp3.Type(i)] {
			assertError(t, nil, err)
			if ty != resp3.Type(i) {
				t.Errorf("got %v, expected %v", ty, resp3.Type(i))
			}
		} else {
			assertError(t, resp3.ErrInvalidType, err)
		}
	}

	// special case for RESP3 compatibility
	for _, in := range []string{
		string(resp3.TypeArray) + "-1\r\n",
		string(resp3.TypeBlobString) + "-1\r\n",
	} {
		rr, _ := newTestReader(in)

		ty, err := rr.Peek()
		assertError(t, nil, err)
		if ty != resp3.TypeNull {
			t.Errorf("got %s, expected nil", ty)
		}
	}
}

func benchmarkPeek(in string) func(*testing.B) {
	return func(b *testing.B) {
		rr, reset := newTestReader(in)
		for b.Loop() {
			reset(in)
			_, _ = rr.Peek()
		}
	}
}

func BenchmarkReaderPeek(b *testing.B) {
	b.Run("Invalid", benchmarkPeek("/\r\n"))
	b.Run("NilArray", benchmarkPeek("*-1\r\n"))
	b.Run("NilBlobString", benchmarkPeek("$-1\r\n"))
	b.Run("Valid", benchmarkPeek("_\r\n"))
}

func TestReaderRead(t *testing.T) {
	t.Run("Array", testReadArray)
	t.Run("Attribute", testReadAttribute)
	t.Run("BigNumber", testReadBigNumber)
	t.Run("Boolean", testReadBoolean)
	t.Run("Double", testReadDouble)
	t.Run("BlobChunk", testReadBlobChunk)
	t.Run("BlobChunks", testReadBlobChunks)
	t.Run("BlobError", testReadBlobError)
	t.Run("BlobString", testReadBlobString)
	t.Run("End", testReadEnd)
	t.Run("Map", testReadMap)
	t.Run("Null", testReadNull)
	t.Run("Number", testReadNumber)
	t.Run("Push", testReadPush)
	t.Run("Set", testReadSet)
	t.Run("SimpleError", testReadSimpleError)
	t.Run("SimpleString", testReadSimpleString)
	t.Run("VerbatimString", testReadVerbatimString)
}

func newTestReader(s string) (rr *resp3.Reader, reset func(string)) {
	r := strings.NewReader(s)
	br := bufio.NewReader(r)
	rr = resp3.NewReader(br)
	return rr, func(s string) {
		r.Reset(s)
		br.Reset(r)
	}
}

func newTypePrefixFunc(ty resp3.Type) func(string) string {
	return func(s string) string {
		return string(ty) + s
	}
}

func runAggregateReadTest(t *testing.T, ty resp3.Type, readHeader func(*resp3.Reader) (int64, bool, error)) {
	p := newTypePrefixFunc(ty)
	for _, c := range []struct {
		in      string
		n       int64
		chunked bool
		err     error
	}{
		{err: resp3.ErrUnexpectedEOL},

		{in: "A", err: resp3.ErrInvalidType},
		{in: string(resp3.TypeBlobString), err: resp3.ErrUnexpectedType},
		{in: string(resp3.TypeInvalid), err: resp3.ErrInvalidType},

		{in: p(""), err: resp3.ErrUnexpectedEOL},
		{in: p("\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("a\r\n"), err: resp3.ErrInvalidAggregateTypeLength},
		{in: p("-2\r\n"), err: resp3.ErrInvalidAggregateTypeLength},
		{in: p("-1\r\n"), err: resp3.ErrInvalidAggregateTypeLength},

		{in: p("0\r\n")},
		{in: p("1\r\n"), n: 1},
		{in: p("2\r\n"), n: 2},

		{in: p("?\r\n"), n: -1, chunked: true},

		{in: p("184467440737095516151\r\n"), err: resp3.ErrOverflow},
	} {
		rr, _ := newTestReader(c.in)
		n, chunked, err := readHeader(rr)
		assertError(t, c.err, err)
		if n != c.n {
			t.Errorf("got n=%d, expected n=%d", n, c.n)
		}
		if chunked != c.chunked {
			t.Errorf("got chunked=%v, expected chunked=%v", chunked, c.chunked)
		}
	}
}

func runBlobReadTest(t *testing.T, ty resp3.Type, readBlob func(*resp3.Reader, []byte) ([]byte, bool, error)) {
	p := newTypePrefixFunc(ty)
	for _, c := range []struct {
		in    string
		limit int
		s     string
		err   error
	}{
		{err: resp3.ErrUnexpectedEOL},

		{in: "A", err: resp3.ErrInvalidType},
		{in: string(resp3.TypeArray), err: resp3.ErrUnexpectedType},
		{in: string(resp3.TypeInvalid), err: resp3.ErrInvalidType},

		{in: p(""), err: resp3.ErrUnexpectedEOL},
		{in: p("\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("-2\r\n"), err: resp3.ErrInvalidBlobLength},
		{in: p("-1\r\n"), err: resp3.ErrInvalidBlobLength},

		{in: p("\r\nhello\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("5\r\nhello\r\n"), s: "hello"},

		{in: p("5\r\nhello world\r\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("10\r\nhello\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("5\r\nhello"), err: resp3.ErrUnexpectedEOL},
		{in: p("5\r\nhello\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("5\r\nhello\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("5\r\nhello\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("5\r\nhello\r\r"), err: resp3.ErrUnexpectedEOL},

		{
			in: p("11000\r\n") + strings.Repeat("hello world", 1000) + "\r\n",
			s:  strings.Repeat("hello world", 1000),
		},

		{
			in: p(strconv.Itoa(resp3.DefaultSingleReadSizeLimit) + "\r\n" +
				strings.Repeat("a", resp3.DefaultSingleReadSizeLimit) + "\r\n"),
			s: strings.Repeat("a", resp3.DefaultSingleReadSizeLimit),
		},

		{
			in: p(strconv.Itoa(resp3.DefaultSingleReadSizeLimit+1) + "\r\n" +
				strings.Repeat("a", resp3.DefaultSingleReadSizeLimit+1) + "\r\n"),
			err: resp3.ErrSingleReadSizeLimitExceeded,
		},

		{
			in: p(strconv.Itoa(resp3.DefaultSingleReadSizeLimit+1) + "\r\n" +
				strings.Repeat("a", resp3.DefaultSingleReadSizeLimit+1) + "\r\n"),
			limit: -1,
			s:     strings.Repeat("a", resp3.DefaultSingleReadSizeLimit+1),
		},

		{
			in:    p("5\r\nhello\r\n"),
			limit: 5,
			s:     "hello",
		},

		{
			in:    p("5\r\nhello\r\n"),
			limit: 4,
			err:   resp3.ErrSingleReadSizeLimitExceeded,
		},

		{in: p("184467440737095516151\r\n"), err: resp3.ErrOverflow},
	} {
		withBuf := func(base []byte) {
			rr, _ := newTestReader(c.in)
			rr.SingleReadSizeLimit = c.limit
			buf, chunked, err := readBlob(rr, base)
			assertReadResultEqual(t, append(base, c.s...), buf, c.err, err)
			if chunked {
				t.Errorf("got chunked=%v, expected chunked=%v", chunked, false)
			}
		}
		withBuf(nil)
		withBuf([]byte("existing data"))
	}
}

func runStreamableBlobReadTest(
	t *testing.T,
	ty resp3.Type,
	readBlob func(*resp3.Reader, []byte) ([]byte, bool, error),
) {
	runBlobReadTest(t, ty, readBlob)

	p := newTypePrefixFunc(ty)
	{
		rr, _ := newTestReader(p("0\r\n"))
		b, chunked, err := readBlob(rr, nil)
		assertError(t, resp3.ErrUnexpectedEOL, err)
		if len(b) != 0 {
			t.Errorf("got %q, expected no data", string(b))
		}
		if chunked {
			t.Errorf("got chunked=%v, expected chunked=%v", chunked, false)
		}
	}
	{
		rr, _ := newTestReader(p("?\r\n"))
		b, chunked, err := readBlob(rr, nil)
		assertError(t, nil, err)
		if len(b) != 0 {
			t.Errorf("got %q, expected no data", string(b))
		}
		if !chunked {
			t.Errorf("got chunked=%v, expected chunked=%v", chunked, true)
		}
	}
}

func runEmptyReadTest(t *testing.T, ty resp3.Type, readEmpty func(*resp3.Reader) error) {
	p := newTypePrefixFunc(ty)
	for _, c := range []struct {
		in  string
		err error
	}{
		{err: resp3.ErrUnexpectedEOL},

		{in: "A", err: resp3.ErrInvalidType},
		{in: string(resp3.TypeArray), err: resp3.ErrUnexpectedType},
		{in: string(resp3.TypeInvalid), err: resp3.ErrInvalidType},

		{in: p(""), err: resp3.ErrUnexpectedEOL},
		{in: p("\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r\r"), err: resp3.ErrUnexpectedEOL},

		{in: p("\r\n")},

		{in: p(".\r\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("#\r\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("A\r\n"), err: resp3.ErrUnexpectedEOL},
	} {
		rr, _ := newTestReader(c.in)
		assertError(t, c.err, readEmpty(rr))
	}
}

func runSimpleReadTest(t *testing.T, ty resp3.Type, readSimple func(*resp3.Reader, []byte) ([]byte, error)) {
	p := newTypePrefixFunc(ty)
	for _, c := range []struct {
		in    string
		limit int
		s     string
		err   error
	}{
		{err: resp3.ErrUnexpectedEOL},

		{in: "A", err: resp3.ErrInvalidType},
		{in: string(resp3.TypeArray), err: resp3.ErrUnexpectedType},
		{in: string(resp3.TypeInvalid), err: resp3.ErrInvalidType},

		{in: p(""), err: resp3.ErrUnexpectedEOL},
		{in: p("\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r\r"), err: resp3.ErrUnexpectedEOL},

		{in: p("\r\n")},
		{in: p("OK\r\n"), s: "OK"},

		{
			in: p(strings.Repeat("hello world", 1000) + "\r\n"),
			s:  strings.Repeat("hello world", 1000),
		},

		{
			in: p(strings.Repeat("a", resp3.DefaultSingleReadSizeLimit) + "\r\n"),
			s:  strings.Repeat("a", resp3.DefaultSingleReadSizeLimit),
		},

		{
			in:  p(strings.Repeat("a", resp3.DefaultSingleReadSizeLimit+1) + "\r\n"),
			err: resp3.ErrSingleReadSizeLimitExceeded,
		},

		{
			in:    p(strings.Repeat("a", resp3.DefaultSingleReadSizeLimit+1) + "\r\n"),
			limit: -1,
			s:     strings.Repeat("a", resp3.DefaultSingleReadSizeLimit+1),
		},

		{
			in:    p("hello\r\n"),
			limit: 5,
			s:     "hello",
		},

		{
			in:    p("hello\r\n"),
			limit: 4,
			err:   resp3.ErrSingleReadSizeLimitExceeded,
		},
	} {
		withBuf := func(base []byte) {
			rr, _ := newTestReader(c.in)
			rr.SingleReadSizeLimit = c.limit
			buf, err := readSimple(rr, base)
			assertReadResultEqual(t, append(base, c.s...), buf, c.err, err)
		}
		withBuf(nil)
		withBuf([]byte("existing data"))
	}
}

func testReadArray(t *testing.T) {
	runAggregateReadTest(t, resp3.TypeArray, (*resp3.Reader).ReadArrayHeader)
}

func testReadAttribute(t *testing.T) {
	runAggregateReadTest(t, resp3.TypeAttribute, (*resp3.Reader).ReadAttributeHeader)
}

func testReadBigNumber(t *testing.T) {
	newBigInt := func(s string) *big.Int {
		n, _ := new(big.Int).SetString(s, 10)
		return n
	}
	p := newTypePrefixFunc(resp3.TypeBigNumber)
	for _, c := range []struct {
		in  string
		n   *big.Int
		err error
	}{
		{err: resp3.ErrUnexpectedEOL},

		{in: "A", err: resp3.ErrInvalidType},
		{in: string(resp3.TypeArray), err: resp3.ErrUnexpectedType},
		{in: string(resp3.TypeInvalid), err: resp3.ErrInvalidType},

		{in: p(""), err: resp3.ErrUnexpectedEOL},
		{in: p("\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("-10\r\n"), n: big.NewInt(-10)},
		{in: p("-1\r\n"), n: big.NewInt(-1)},
		{in: p("0\r\n"), n: big.NewInt(0)},
		{in: p("1\r\n"), n: big.NewInt(1)},
		{in: p("10\r\n"), n: big.NewInt(10)},
		{
			in: p("-123456789123456789123456789123456789\r\n"),
			n:  newBigInt("-123456789123456789123456789123456789"),
		},
		{
			in: p("123456789123456789123456789123456789\r\n"),
			n:  newBigInt("123456789123456789123456789123456789"),
		},
		{
			in: p("+123456789123456789123456789123456789\r\n"),
			n:  newBigInt("123456789123456789123456789123456789"),
		},
		{in: p("+1\r\n"), n: big.NewInt(1)},

		{in: p("A\r\n"), err: resp3.ErrInvalidBigNumber},
		{in: p("1a\r\n"), err: resp3.ErrInvalidBigNumber},
		{in: p("1.\r\n"), err: resp3.ErrInvalidBigNumber},
		{in: p("1.0\r\n"), err: resp3.ErrInvalidBigNumber},
		{in: p("1.01\r\n"), err: resp3.ErrInvalidBigNumber},
		{in: p("#\r\n"), err: resp3.ErrInvalidBigNumber},
		{in: p("-\r\n"), err: resp3.ErrInvalidBigNumber},
		{in: p("+\r\n"), err: resp3.ErrInvalidBigNumber},
	} {
		rr, _ := newTestReader(c.in)
		n := new(big.Int)
		err := rr.ReadBigNumber(n)
		assertError(t, c.err, err)
		if c.n != nil && c.n.Cmp(n) != 0 {
			t.Errorf("got %s, expected %s", n, c.n)
		}
	}
}

func testReadBoolean(t *testing.T) {
	p := newTypePrefixFunc(resp3.TypeBoolean)
	for _, c := range []struct {
		in  string
		b   bool
		err error
	}{
		{err: resp3.ErrUnexpectedEOL},

		{in: "A", err: resp3.ErrInvalidType},
		{in: string(resp3.TypeArray), err: resp3.ErrUnexpectedType},
		{in: string(resp3.TypeInvalid), err: resp3.ErrInvalidType},

		{in: p(""), err: resp3.ErrUnexpectedEOL},
		{in: p("\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("f\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("f\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("f\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("f\r\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("t\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("t\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("t\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("t\r\r"), err: resp3.ErrUnexpectedEOL},

		{in: p("f\r\n")},
		{in: p("t\r\n"), b: true},

		{in: p("#\r\n"), err: resp3.ErrInvalidBoolean},
		{in: p("A\r\n"), err: resp3.ErrInvalidBoolean},
		{in: p("F\r\n"), err: resp3.ErrInvalidBoolean},
		{in: p("T\r\n"), err: resp3.ErrInvalidBoolean},
		{in: p("Z\r\n"), err: resp3.ErrInvalidBoolean},
	} {
		rr, _ := newTestReader(c.in)
		b, err := rr.ReadBoolean()
		assertError(t, c.err, err)
		if b != c.b {
			t.Errorf("got %v, expected %v", b, c.b)
		}
	}
}

func testReadDouble(t *testing.T) {
	p := newTypePrefixFunc(resp3.TypeDouble)
	for _, c := range []struct {
		in  string
		f   float64
		err error
	}{
		{err: resp3.ErrUnexpectedEOL},

		{in: "A", err: resp3.ErrInvalidType},
		{in: string(resp3.TypeArray), err: resp3.ErrUnexpectedType},
		{in: string(resp3.TypeInvalid), err: resp3.ErrInvalidType},

		{in: p(""), err: resp3.ErrUnexpectedEOL},
		{in: p("\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("-1"), err: resp3.ErrUnexpectedEOL},
		{in: p("0"), err: resp3.ErrUnexpectedEOL},
		{in: p("1"), err: resp3.ErrUnexpectedEOL},
		{in: p("inf"), err: resp3.ErrUnexpectedEOL},
		{in: p("-inf"), err: resp3.ErrUnexpectedEOL},
		{in: p("+inf"), err: resp3.ErrUnexpectedEOL},

		{in: p("-1.2\r\n"), f: -1.2},
		{in: p("-1.0\r\n"), f: -1},
		{in: p("-1\r\n"), f: -1},
		{in: p("-0.01\r\n"), f: -0.01},
		{in: p("-0.1\r\n"), f: -0.1},
		{in: p("-0.0\r\n")},
		{in: p("0\r\n")},
		{in: p("0.0\r\n")},
		{in: p("0.01\r\n"), f: 0.01},
		{in: p("0.1\r\n"), f: 0.1},
		{in: p("1\r\n"), f: 1},
		{in: p("1.0\r\n"), f: 1},
		{in: p("1.2\r\n"), f: 1.2},

		{in: p("1.\r\n"), f: 1},
		{in: p("1.01\r\n"), f: 1.01},
		{in: p("+1\r\n"), f: 1},

		{in: p("inf\r\n"), f: math.Inf(1)},
		{in: p("+inf\r\n"), f: math.Inf(1)}, // not specified, but handled by ParseFloat
		{in: p("-inf\r\n"), f: math.Inf(-1)},

		{in: p("A\r\n"), err: resp3.ErrInvalidDouble},
		{in: p("1a\r\n"), err: resp3.ErrInvalidDouble},
		{in: p("#\r\n"), err: resp3.ErrInvalidDouble},
		{in: p("-\r\n"), err: resp3.ErrInvalidDouble},
		{in: p("+\r\n"), err: resp3.ErrInvalidDouble},
	} {
		rr, _ := newTestReader(c.in)
		f, err := rr.ReadDouble()
		assertError(t, c.err, err)
		if f != c.f {
			t.Errorf("got %f, expected %f", f, c.f)
		}
	}
}

func testReadBlobChunk(t *testing.T) {
	runBlobReadTest(t, resp3.TypeBlobChunk, (*resp3.Reader).ReadBlobChunk)

	p := newTypePrefixFunc(resp3.TypeBlobChunk)
	{
		rr, _ := newTestReader(p("0\r\n"))
		b, last, err := rr.ReadBlobChunk(nil)
		assertError(t, nil, err)
		if len(b) != 0 {
			t.Errorf("got %q, expected no data", string(b))
		}
		if !last {
			t.Errorf("got last=%v, expected last=%v", last, true)
		}
	}
}

func testReadBlobChunks(t *testing.T) {
	p := newTypePrefixFunc(resp3.TypeBlobChunk)
	for _, c := range []struct {
		in  string
		s   string
		err error
	}{
		{err: resp3.ErrUnexpectedEOL},

		{in: "A", err: resp3.ErrInvalidType},
		{in: string(resp3.TypeArray), err: resp3.ErrUnexpectedType},
		{in: string(resp3.TypeInvalid), err: resp3.ErrInvalidType},

		{in: p(""), err: resp3.ErrUnexpectedEOL},
		{in: p("\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("-2\r\n"), err: resp3.ErrInvalidBlobLength},
		{in: p("-1\r\n"), err: resp3.ErrInvalidBlobLength},

		{in: p("\r\nhello\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("0\r\n")},

		{in: p("5\r\nhello\r\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("5\r\nhello\r\n") + p("0\r\n"), s: "hello"},

		{in: p("5\r\nhello world\r\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("10\r\nhello\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("5\r\nhello"), err: resp3.ErrUnexpectedEOL},
		{in: p("5\r\nhello\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("5\r\nhello\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("5\r\nhello\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("5\r\nhello\r\r"), err: resp3.ErrUnexpectedEOL},

		{
			in: p("11000\r\n"+strings.Repeat("hello world", 1000)+"\r\n") + p("0\r\n"),
			s:  strings.Repeat("hello world", 1000),
		},
	} {
		withBuf := func(base []byte) {
			rr, _ := newTestReader(c.in)
			buf, err := rr.ReadBlobChunks(base)
			assertReadResultEqual(t, append(base, c.s...), buf, c.err, err)
		}
		withBuf(nil)
		withBuf([]byte("existing data"))
	}
}

func testReadBlobError(t *testing.T) {
	runStreamableBlobReadTest(t, resp3.TypeBlobError, (*resp3.Reader).ReadBlobError)
}

func testReadBlobString(t *testing.T) {
	runStreamableBlobReadTest(t, resp3.TypeBlobString, (*resp3.Reader).ReadBlobString)
}

func testReadEnd(t *testing.T) {
	runEmptyReadTest(t, resp3.TypeEnd, (*resp3.Reader).ReadEnd)
}

func testReadMap(t *testing.T) {
	runAggregateReadTest(t, resp3.TypeMap, (*resp3.Reader).ReadMapHeader)
}

func testReadNull(t *testing.T) {
	runEmptyReadTest(t, resp3.TypeNull, (*resp3.Reader).ReadNull)

	// RESP2 backward compatibility
	for _, in := range []string{
		string(resp3.TypeArray) + "-1\r\n",
		string(resp3.TypeBlobString) + "-1\r\n",
	} {
		rr, _ := newTestReader(in)
		assertError(t, nil, rr.ReadNull())
	}
}

func testReadNumber(t *testing.T) {
	p := newTypePrefixFunc(resp3.TypeInteger)
	for _, c := range []struct {
		in  string
		n   int64
		err error
	}{
		{err: resp3.ErrUnexpectedEOL},

		{in: "A", err: resp3.ErrInvalidType},
		{in: string(resp3.TypeArray), err: resp3.ErrUnexpectedType},
		{in: string(resp3.TypeInvalid), err: resp3.ErrInvalidType},

		{in: p(""), err: resp3.ErrUnexpectedEOL},
		{in: p("\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("-10\r\n"), n: -10},
		{in: p("-1\r\n"), n: -1},
		{in: p("0\r\n")},
		{in: p("1\r\n"), n: 1},
		{in: p("10\r\n"), n: 10},

		{in: p("A\r\n"), err: resp3.ErrInvalidNumber},
		{in: p("1a\r\n"), err: resp3.ErrInvalidNumber},
		{in: p("1.\r\n"), err: resp3.ErrInvalidNumber},
		{in: p("1.0\r\n"), err: resp3.ErrInvalidNumber},
		{in: p("1.01\r\n"), err: resp3.ErrInvalidNumber},
		{in: p("#\r\n"), err: resp3.ErrInvalidNumber},
		{in: p("-\r\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("+\r\n"), err: resp3.ErrInvalidNumber},
		{in: p("+1\r\n"), err: resp3.ErrInvalidNumber},

		// Numbers are parsed as int64
		{in: p("-184467440737095516151\r\n"), err: resp3.ErrOverflow},
		{in: p("184467440737095516151\r\n"), err: resp3.ErrOverflow},
	} {
		rr, _ := newTestReader(c.in)
		n, err := rr.ReadInteger()
		assertError(t, c.err, err)
		if n != c.n {
			t.Errorf("got %d, expected %d", n, c.n)
		}
	}
}

func testReadPush(t *testing.T) {
	runAggregateReadTest(t, resp3.TypePush, (*resp3.Reader).ReadPushHeader)
}

func testReadSet(t *testing.T) {
	runAggregateReadTest(t, resp3.TypeSet, (*resp3.Reader).ReadSetHeader)
}

func testReadSimpleError(t *testing.T) {
	runSimpleReadTest(t, resp3.TypeSimpleError, (*resp3.Reader).ReadSimpleError)
}

func testReadSimpleString(t *testing.T) {
	runSimpleReadTest(t, resp3.TypeSimpleString, (*resp3.Reader).ReadSimpleString)
}

func testReadVerbatimString(t *testing.T) {
	p := newTypePrefixFunc(resp3.TypeVerbatimString)
	for _, c := range []struct {
		in    string
		limit int
		p     string
		s     string
		err   error
	}{
		{err: resp3.ErrUnexpectedEOL},

		{in: "A", err: resp3.ErrInvalidType},
		{in: string(resp3.TypeArray), err: resp3.ErrUnexpectedType},
		{in: string(resp3.TypeInvalid), err: resp3.ErrInvalidType},

		{in: p(""), err: resp3.ErrUnexpectedEOL},
		{in: p("\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("\r\nfoo:\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("3\r\nbar\r\n"), err: resp3.ErrInvalidVerbatimString},
		{in: p("4\r\n:bar\r\n"), err: resp3.ErrInvalidVerbatimString},
		{in: p("5\r\nf:bar\r\n"), err: resp3.ErrInvalidVerbatimString},
		{in: p("6\r\nfo:bar\r\n"), err: resp3.ErrInvalidVerbatimString},
		{in: p("4\r\nfoo:\r\n"), p: "foo"},
		{in: p("5\r\nfoo:b\r\n"), p: "foo", s: "b"},
		{in: p("6\r\nfoo:ba\r\n"), p: "foo", s: "ba"},
		{in: p("7\r\nfoo:bar\r\n"), p: "foo", s: "bar"},

		{in: p("5\r\nfoo:hello world\r\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("10\r\nfoo:hello\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("7\r\nfoo:bar"), err: resp3.ErrUnexpectedEOL},
		{in: p("7\r\nfoo:bar\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("7\r\nfoo:bar\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("7\r\nfoo:bar\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("7\r\nfoo:bar\r\r"), err: resp3.ErrUnexpectedEOL},

		{
			in: p("11004\r\nfoo:" + strings.Repeat("hello world", 1000) + "\r\n"),
			p:  "foo",
			s:  strings.Repeat("hello world", 1000),
		},

		{
			in: p(strconv.Itoa(resp3.DefaultSingleReadSizeLimit) + "\r\nfoo:" +
				strings.Repeat("a", resp3.DefaultSingleReadSizeLimit-len("foo:")) + "\r\n"),
			p: "foo",
			s: strings.Repeat("a", resp3.DefaultSingleReadSizeLimit-len("foo:")),
		},

		{
			in: p(strconv.Itoa(resp3.DefaultSingleReadSizeLimit+1) + "\r\nfoo:" +
				strings.Repeat("a", resp3.DefaultSingleReadSizeLimit-len("foo:")+1) + "\r\n"),
			err: resp3.ErrSingleReadSizeLimitExceeded,
		},

		{
			in: p(strconv.Itoa(resp3.DefaultSingleReadSizeLimit+1) + "\r\nfoo:" +
				strings.Repeat("a", resp3.DefaultSingleReadSizeLimit-len("foo:")+1) + "\r\n"),
			limit: -1,
			p:     "foo",
			s:     strings.Repeat("a", resp3.DefaultSingleReadSizeLimit-len("foo:")+1),
		},

		{
			in:    p("7\r\nfoo:bar\r\n"),
			limit: 7,
			p:     "foo",
			s:     "bar",
		},

		{
			in:    p("7\r\nfoo:bar\r\n"),
			limit: 6,
			err:   resp3.ErrSingleReadSizeLimitExceeded,
		},
	} {
		withBuf := func(base []byte) {
			rr, _ := newTestReader(c.in)
			rr.SingleReadSizeLimit = c.limit
			p, s, err := rr.ReadVerbatimString(base)
			assertReadResultEqual(t, append(base, c.s...), s, c.err, err)
			if err != nil {
				return
			}
			assertBytes(t, c.p, p[:])
		}
		withBuf(nil)
		withBuf([]byte("existing data"))
	}
}

func BenchmarkReaderRead(b *testing.B) {
	b.Run("Array", makeReadAggregationBenchmark(resp3.TypeArray, (*resp3.Reader).ReadArrayHeader))
	b.Run("Attribute", makeReadAggregationBenchmark(resp3.TypeAttribute, (*resp3.Reader).ReadAttributeHeader))
	b.Run("BigNumber", benchmarkReadBigNumber)
	b.Run("Boolean", benchmarkReadBoolean)
	b.Run("Double", benchmarkReadDouble)
	b.Run("BlobError", makeReadBlobBenchmark(resp3.TypeBlobError, (*resp3.Reader).ReadBlobError))
	b.Run("BlobString", makeReadBlobBenchmark(resp3.TypeBlobString, (*resp3.Reader).ReadBlobString))
	b.Run("BlobChunk", benchmarkReadBlobChunk)
	b.Run("BlobChunks", benchmarkReadBlobChunks)
	b.Run("End", benchmarkReadEnd)
	b.Run("Integer", benchmarkReadInteger)
	b.Run("Map", makeReadAggregationBenchmark(resp3.TypeMap, (*resp3.Reader).ReadMapHeader))
	b.Run("Null", benchmarkReadNull)
	b.Run("Push", makeReadAggregationBenchmark(resp3.TypePush, (*resp3.Reader).ReadPushHeader))
	b.Run("Set", makeReadAggregationBenchmark(resp3.TypeSet, (*resp3.Reader).ReadSetHeader))
	b.Run("SimpleError", makeReadSimpleBenchmark(resp3.TypeSimpleError, (*resp3.Reader).ReadSimpleError))
	b.Run("SimpleString", makeReadSimpleBenchmark(resp3.TypeSimpleString, (*resp3.Reader).ReadSimpleString))
	b.Run("VerbatimString", benchmarkReadVerbatimString)
}

func makeReadAggregationBenchmark(ty resp3.Type, readHeader func(*resp3.Reader) (int64, bool, error)) func(*testing.B) {
	return func(b *testing.B) {
		b.Run("Fixed", func(b *testing.B) {
			in := string(ty) + "16\r\n"
			rr, reset := newTestReader(in)
			for b.Loop() {
				reset(in)
				_, _, _ = readHeader(rr)
			}
		})

		b.Run("Streamed", func(b *testing.B) {
			in := string(ty) + "?\r\n"
			rr, reset := newTestReader(in)
			for b.Loop() {
				reset(in)
				_, _, _ = readHeader(rr)
			}
		})
	}
}

func makeReadBlobBenchmark(ty resp3.Type, readBlob func(*resp3.Reader, []byte) ([]byte, bool, error)) func(*testing.B) {
	return func(b *testing.B) {
		b.Run("Fixed", func(b *testing.B) {
			var buf [32]byte
			in := string(ty) + "32\r\nhello world! what's up? kthxbye!\r\n"
			rr, reset := newTestReader(in)
			for b.Loop() {
				reset(in)
				_, _, _ = readBlob(rr, buf[:0])
			}
		})

		b.Run("Streamed", func(b *testing.B) {
			in := string(ty) + "?\r\n"
			rr, reset := newTestReader(in)
			for b.Loop() {
				reset(in)
				_, _, _ = readBlob(rr, nil)
			}
		})
	}
}

func makeReadSimpleBenchmark(ty resp3.Type, readSimple func(*resp3.Reader, []byte) ([]byte, error)) func(*testing.B) {
	return func(b *testing.B) {
		var buf [32 + len("\r\n")]byte
		in := string(ty) + "hello world! what's up? kthxbye!\r\n"
		rr, reset := newTestReader(in)
		for b.Loop() {
			reset(in)
			_, _ = readSimple(rr, buf[:0])
		}
	}
}

func benchmarkReadBigNumber(b *testing.B) {
	in := string(resp3.TypeBigNumber) + "123456789123456789123456789123456789\r\n"
	rr, reset := newTestReader(in)
	n := new(big.Int)
	for b.Loop() {
		reset(in)
		_ = rr.ReadBigNumber(n)
	}
}

func benchmarkReadBoolean(b *testing.B) {
	in := string(resp3.TypeBoolean) + "t\r\n"
	rr, reset := newTestReader(in)
	for b.Loop() {
		reset(in)
		_, _ = rr.ReadBoolean()
	}
}

func benchmarkReadDouble(b *testing.B) {
	in := string(resp3.TypeDouble) + "1234.5678\r\n"
	rr, reset := newTestReader(in)
	for b.Loop() {
		reset(in)
		_, _ = rr.ReadDouble()
	}
}

func benchmarkReadBlobChunk(b *testing.B) {
	b.Run("Chunk", func(b *testing.B) {
		var buf [32]byte
		in := string(resp3.TypeBlobChunk) + "32\r\nhello world! what's up? kthxbye!\r\n"
		rr, reset := newTestReader(in)
		for b.Loop() {
			reset(in)
			_, _, _ = rr.ReadBlobChunk(buf[:0])
		}
	})

	b.Run("End", func(b *testing.B) {
		in := string(resp3.TypeBlobChunk) + "0\r\n"
		rr, reset := newTestReader(in)
		for b.Loop() {
			reset(in)
			_, _, _ = rr.ReadBlobChunk(nil)
		}
	})
}

func benchmarkReadBlobChunks(b *testing.B) {
	var buf [32]byte
	in := string(resp3.TypeBlobChunk) + "5\r\nhello\r\n" +
		string(resp3.TypeBlobChunk) + "1\r\n \r\n" +
		string(resp3.TypeBlobChunk) + "5\r\nworld\r\n" +
		string(resp3.TypeBlobChunk) + "10\r\nwhat's up?\r\n" +
		string(resp3.TypeBlobChunk) + "1\r\n \r\n" +
		string(resp3.TypeBlobChunk) + "8\r\nkthxbye!\r\n" +
		string(resp3.TypeBlobChunk) + "0\r\n"
	rr, reset := newTestReader(in)
	for b.Loop() {
		reset(in)
		_, _ = rr.ReadBlobChunks(buf[:0])
	}
}

func benchmarkReadEnd(b *testing.B) {
	in := string(resp3.TypeEnd) + "\r\n"
	rr, reset := newTestReader(in)
	for b.Loop() {
		reset(in)
		_ = rr.ReadEnd()
	}
}

func benchmarkReadInteger(b *testing.B) {
	in := string(resp3.TypeInteger) + "12345678\r\n"
	rr, reset := newTestReader(in)
	for b.Loop() {
		reset(in)
		_, _ = rr.ReadInteger()
	}
}

func benchmarkReadNull(b *testing.B) {
	makeBench := func(in string) func(*testing.B) {
		return func(b *testing.B) {
			rr, reset := newTestReader(in)
			for b.Loop() {
				reset(in)
				_ = rr.ReadNull()
			}
		}
	}
	b.Run("Native", makeBench(string(resp3.TypeNull)+"\r\n"))
	b.Run("NilArray", makeBench(string(resp3.TypeArray)+"-1\r\n"))
	b.Run("NilBLobString", makeBench(string(resp3.TypeBlobString)+"-1\r\n"))
}

func benchmarkReadVerbatimString(b *testing.B) {
	var buf [36]byte
	in := string(resp3.TypeVerbatimString) + "36\r\ntxt:hello world! what's up? kthxbye!\r\n"
	rr, reset := newTestReader(in)
	for b.Loop() {
		reset(in)
		_, _, _ = rr.ReadVerbatimString(buf[:0])
	}
}

type discardTest struct {
	in string

	limit  int
	nested bool

	ty   resp3.Type
	err  error
	rest string
}

var discardTests = map[string][]discardTest{
	"All": {
		{
			err: io.EOF,
		},

		{
			in:   "A",
			err:  resp3.ErrInvalidType,
			rest: "A",
		},
	},
	"Array": {
		{
			in:  "*0",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: "*-1\r\n",
			ty: resp3.TypeNull,
		},
		{
			in: "*0\r\n",
			ty: resp3.TypeArray,
		},
		{
			in: "*1\r\n",
			ty: resp3.TypeArray,
		},
		{
			in:   "*1\r\n+OK\r\n",
			ty:   resp3.TypeArray,
			rest: "+OK\r\n",
		},
		{
			in:     "*1\r\n+OK\r\n",
			nested: true,
			ty:     resp3.TypeArray,
		},
		{
			in:     "*1\r\n+OK\r\n-ERR\r\n",
			nested: true,
			ty:     resp3.TypeArray,
			rest:   "-ERR\r\n",
		},
		{
			in:     "*2\r\n+OK\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:     "*1\r\n*2\r\n+OK\r\n-ERR\r\n",
			nested: true,
			ty:     resp3.TypeArray,
		},
	},
	"Attribute": {
		{
			in:  "|0",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "|-1\r\n",
			err: resp3.ErrInvalidAggregateTypeLength,
		},
		{
			in: "|0\r\n",
			ty: resp3.TypeAttribute,
		},
		{
			in: "|1\r\n",
			ty: resp3.TypeAttribute,
		},
		{
			in:   "|1\r\n+OK\r\n",
			ty:   resp3.TypeAttribute,
			rest: "+OK\r\n",
		},
		{
			in:     "|1\r\n+OK\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:     "|1\r\n+OK\r\n-ERR\r\n",
			nested: true,
			ty:     resp3.TypeAttribute,
		},
		{
			in:     "|1\r\n+OK\r\n-ERR\r\n:1234\r\n",
			nested: true,
			ty:     resp3.TypeAttribute,
			rest:   ":1234\r\n",
		},
		{
			in:     "|2\r\n+OK\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:     "|1\r\n+KEY\r\n*2\r\n+OK\r\n-ERR\r\n",
			nested: true,
			ty:     resp3.TypeAttribute,
		},
	},
	"BigNumber": {
		{
			in:  "(",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "(\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "(12345678901234567890123456789012345678901234567890",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: "(12345678901234567890123456789012345678901234567890\r\n",
			ty: resp3.TypeBigNumber,
		},
		{
			in:  "(12345678901234567890123456789012345678901234567890.1234\r\n",
			err: resp3.ErrInvalidBigNumber,
		},
		{
			in:   "(12345678901234567890123456789012345678901234567890\r\n+OK\r\n",
			ty:   resp3.TypeBigNumber,
			rest: "+OK\r\n",
		},
		{
			in:     "(12345678901234567890123456789012345678901234567890\r\n+OK\r\n",
			nested: true,
			ty:     resp3.TypeBigNumber,
			rest:   "+OK\r\n",
		},
	},
	"Boolean": {
		{
			in:  "#",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "#\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "#f",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: "#f\r\n",
			ty: resp3.TypeBoolean,
		},
		{
			in:  "#t",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: "#t\r\n",
			ty: resp3.TypeBoolean,
		},
		{
			in:   "#f\r\n+OK\r\n",
			ty:   resp3.TypeBoolean,
			rest: "+OK\r\n",
		},
		{
			in:     "#f\r\n+OK\r\n",
			ty:     resp3.TypeBoolean,
			nested: true,
			rest:   "+OK\r\n",
		},
	},
	"BlobChunk": {
		{
			in:  ";",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  ";\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: ";0\r\n",
			ty: resp3.TypeBlobChunk,
		},
		{
			in: ";5\r\nhello\r\n",
			ty: resp3.TypeBlobChunk,
		},
		{
			in:  ";5\r\nhell\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  ";5\r\nhello!\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  ";-1\r\n",
			err: resp3.ErrInvalidBlobLength,
		},
		{
			in:   ";5\r\nhello\r\n+OK\r\n",
			ty:   resp3.TypeBlobChunk,
			rest: "+OK\r\n",
		},
		{
			in:     ";5\r\nhello\r\n+OK\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedType,
			rest:   "+OK\r\n",
		},
		{
			in:  ";?\r\n",
			err: resp3.ErrInvalidNumber,
		},
		{
			in:  ";1234567890\r\n",
			err: resp3.ErrSingleReadSizeLimitExceeded,
		},
		{
			in:    ";6\r\n",
			limit: 5,
			err:   resp3.ErrSingleReadSizeLimitExceeded,
		},
	},
	"BlobError": {
		{
			in:  "!",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "!\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "!0\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: "!0\r\n\r\n",
			ty: resp3.TypeBlobError,
		},
		{
			in: "!5\r\nhello\r\n",
			ty: resp3.TypeBlobError,
		},
		{
			in:  "!5\r\nhell\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "!5\r\nhello!\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "!-1\r\n",
			err: resp3.ErrInvalidBlobLength,
		},
		{
			in:   "!5\r\nhello\r\n+OK\r\n",
			ty:   resp3.TypeBlobError,
			rest: "+OK\r\n",
		},
		{
			in:     "!5\r\nhello\r\n+OK\r\n",
			nested: true,
			ty:     resp3.TypeBlobError,
			rest:   "+OK\r\n",
		},
		{
			in: "!?\r\n",
			ty: resp3.TypeBlobError,
		},
		{
			in:     "!?\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:     "!?\r\n;5\r\nhello\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:     "!?\r\n;5\r\nhello\r\n;0\r\n",
			ty:     resp3.TypeBlobError,
			nested: true,
		},
		{
			in:     "!?\r\n;5\r\nhello\r\n;6\r\nworld\r\n;0\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:   "!?\r\n;5\r\nhello\r\n;0\r\n",
			ty:   resp3.TypeBlobError,
			rest: ";5\r\nhello\r\n;0\r\n",
		},
		{
			in:     "!?\r\n!5\r\nhello\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedType,
		},
		{
			in:  "!1234567890\r\n",
			err: resp3.ErrSingleReadSizeLimitExceeded,
		},
		{
			in:    "!6\r\n",
			limit: 5,
			err:   resp3.ErrSingleReadSizeLimitExceeded,
		},
	},
	"BlobString": {
		{
			in:  "$",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "$\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "$0\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: "$0\r\n\r\n",
			ty: resp3.TypeBlobString,
		},
		{
			in: "$5\r\nhello\r\n",
			ty: resp3.TypeBlobString,
		},
		{
			in:  "$5\r\nhell\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "$5\r\nhello$\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: "$-1\r\n",
			ty: resp3.TypeNull,
		},
		{
			in:  "$-2\r\n",
			err: resp3.ErrInvalidBlobLength,
		},
		{
			in:   "$5\r\nhello\r\n+OK\r\n",
			ty:   resp3.TypeBlobString,
			rest: "+OK\r\n",
		},
		{
			in:     "$5\r\nhello\r\n+OK\r\n",
			nested: true,
			ty:     resp3.TypeBlobString,
			rest:   "+OK\r\n",
		},
		{
			in: "$?\r\n",
			ty: resp3.TypeBlobString,
		},
		{
			in:     "$?\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:     "$?\r\n;5\r\nhello\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:     "$?\r\n;5\r\nhello\r\n;0\r\n",
			ty:     resp3.TypeBlobString,
			nested: true,
		},
		{
			in:     "$?\r\n;5\r\nhello\r\n;6\r\nworld\r\n;0\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:   "$?\r\n;5\r\nhello\r\n;0\r\n",
			ty:   resp3.TypeBlobString,
			rest: ";5\r\nhello\r\n;0\r\n",
		},
		{
			in:     "$?\r\n$5\r\nhello\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedType,
		},
		{
			in:  "$1234567890\r\n",
			err: resp3.ErrSingleReadSizeLimitExceeded,
		},
		{
			in:    "$6\r\n",
			limit: 5,
			err:   resp3.ErrSingleReadSizeLimitExceeded,
		},
	},
	"Double": {
		{
			in:  ",",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  ",\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  ",1234",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: ",1234\r\n",
			ty: resp3.TypeDouble,
		},
		{
			in: ",1234.5678\r\n",
			ty: resp3.TypeDouble,
		},
		{
			in:   ",1234.5678\r\n+OK\r\n",
			ty:   resp3.TypeDouble,
			rest: "+OK\r\n",
		},
		{
			in:     ",1234.5678\r\n+OK\r\n",
			nested: true,
			ty:     resp3.TypeDouble,
			rest:   "+OK\r\n",
		},
	},
	"End": {
		{
			in:  ".",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: ".\r\n",
			ty: resp3.TypeEnd,
		},
		{
			in:   ".\r\n+OK\r\n",
			ty:   resp3.TypeEnd,
			rest: "+OK\r\n",
		},
		{
			in:     ".\r\n+OK\r\n",
			ty:     resp3.TypeEnd,
			nested: true,
			rest:   "+OK\r\n",
		},
	},
	"Integer": {
		{
			in:  ":",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  ":\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  ":1234",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: ":1234\r\n",
			ty: resp3.TypeInteger,
		},
		{
			in:  ":1234.5678\r\n",
			err: resp3.ErrInvalidNumber,
		},
		{
			in:   ":1234\r\n+OK\r\n",
			ty:   resp3.TypeInteger,
			rest: "+OK\r\n",
		},
		{
			in:     ":1234\r\n+OK\r\n",
			nested: true,
			ty:     resp3.TypeInteger,
			rest:   "+OK\r\n",
		},
	},
	"Map": {
		{
			in:  "%0",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "%-1\r\n",
			err: resp3.ErrInvalidAggregateTypeLength,
		},
		{
			in: "%0\r\n",
			ty: resp3.TypeMap,
		},
		{
			in: "%1\r\n",
			ty: resp3.TypeMap,
		},
		{
			in:   "%1\r\n+OK\r\n",
			ty:   resp3.TypeMap,
			rest: "+OK\r\n",
		},
		{
			in:     "%1\r\n+OK\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:     "%1\r\n+OK\r\n-ERR\r\n",
			nested: true,
			ty:     resp3.TypeMap,
		},
		{
			in:     "%1\r\n+OK\r\n-ERR\r\n:1234\r\n",
			nested: true,
			ty:     resp3.TypeMap,
			rest:   ":1234\r\n",
		},
		{
			in:     "%2\r\n+OK\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:     "%1\r\n+KEY\r\n*2\r\n+OK\r\n-ERR\r\n",
			nested: true,
			ty:     resp3.TypeMap,
		},
	},
	"Null": {
		{
			in:  "_",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: "_\r\n",
			ty: resp3.TypeNull,
		},
		{
			in:   "_\r\n+OK\r\n",
			ty:   resp3.TypeNull,
			rest: "+OK\r\n",
		},
		{
			in:     "_\r\n+OK\r\n",
			ty:     resp3.TypeNull,
			nested: true,
			rest:   "+OK\r\n",
		},
	},
	"Push": {
		{
			in:  ">0",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  ">-1\r\n",
			err: resp3.ErrInvalidAggregateTypeLength,
		},
		{
			in: ">0\r\n",
			ty: resp3.TypePush,
		},
		{
			in: ">1\r\n",
			ty: resp3.TypePush,
		},
		{
			in:   ">1\r\n+OK\r\n",
			ty:   resp3.TypePush,
			rest: "+OK\r\n",
		},
		{
			in:     ">1\r\n+OK\r\n",
			nested: true,
			ty:     resp3.TypePush,
		},
		{
			in:     ">1\r\n+OK\r\n-ERR\r\n",
			nested: true,
			ty:     resp3.TypePush,
			rest:   "-ERR\r\n",
		},
		{
			in:     ">2\r\n+OK\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:     ">1\r\n>2\r\n+OK\r\n-ERR\r\n",
			nested: true,
			ty:     resp3.TypePush,
		},
	},
	"Set": {
		{
			in:  "~0",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "~-1\r\n",
			err: resp3.ErrInvalidAggregateTypeLength,
		},
		{
			in: "~0\r\n",
			ty: resp3.TypeSet,
		},
		{
			in: "~1\r\n",
			ty: resp3.TypeSet,
		},
		{
			in:   "~1\r\n+OK\r\n",
			ty:   resp3.TypeSet,
			rest: "+OK\r\n",
		},
		{
			in:     "~1\r\n+OK\r\n",
			nested: true,
			ty:     resp3.TypeSet,
		},
		{
			in:     "~1\r\n+OK\r\n-ERR\r\n",
			nested: true,
			ty:     resp3.TypeSet,
			rest:   "-ERR\r\n",
		},
		{
			in:     "~2\r\n+OK\r\n",
			nested: true,
			err:    resp3.ErrUnexpectedEOL,
		},
		{
			in:     "~1\r\n~2\r\n+OK\r\n-ERR\r\n",
			nested: true,
			ty:     resp3.TypeSet,
		},
	},
	"SimpleError": {
		{
			in:  "-",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: "-\r\n",
			ty: resp3.TypeSimpleError,
		},
		{
			in: "-ERR something went wrong\r\n",
			ty: resp3.TypeSimpleError,
		},
		{
			in:   "-ERR something went wrong\r\n+OK\r\n",
			ty:   resp3.TypeSimpleError,
			rest: "+OK\r\n",
		},
		{
			in:     "-ERR something went wrong\r\n+OK\r\n",
			nested: true,
			ty:     resp3.TypeSimpleError,
			rest:   "+OK\r\n",
		},
		{
			in:  "-ERR" + strings.Repeat("a", resp3.DefaultSingleReadSizeLimit) + "\r\n",
			err: resp3.ErrSingleReadSizeLimitExceeded,
		},
		{
			in:    "-ERR hello world\r\n",
			limit: 5,
			err:   resp3.ErrSingleReadSizeLimitExceeded,
		},
	},
	"SimpleString": {
		{
			in:  "+",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in: "+\r\n",
			ty: resp3.TypeSimpleString,
		},
		{
			in: "+OK something went right\r\n",
			ty: resp3.TypeSimpleString,
		},
		{
			in:   "+OK something went right\r\n+OK\r\n",
			ty:   resp3.TypeSimpleString,
			rest: "+OK\r\n",
		},
		{
			in:     "+OK something went right\r\n+OK\r\n",
			nested: true,
			ty:     resp3.TypeSimpleString,
			rest:   "+OK\r\n",
		},
		{
			in:  "+OK" + strings.Repeat("a", resp3.DefaultSingleReadSizeLimit) + "\r\n",
			err: resp3.ErrSingleReadSizeLimitExceeded,
		},
		{
			in:    "+OK hello world\r\n",
			limit: 5,
			err:   resp3.ErrSingleReadSizeLimitExceeded,
		},
	},
	"VerbatimString": {
		{
			in:  "=",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "=\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "=0\r\n",
			err: resp3.ErrUnexpectedEOL,
		},
		{
			in:  "=0\r\n\r\n",
			err: resp3.ErrInvalidVerbatimString,
		},
		{
			in:  "=-1\r\n",
			err: resp3.ErrInvalidBlobLength,
		},
		{
			in:  "=2\r\nhe\r\n",
			err: resp3.ErrInvalidVerbatimString,
		},
		{
			in:  "=5\r\nhello\r\n",
			err: resp3.ErrInvalidVerbatimString,
		},
		{
			in: "=5\r\nhel:o\r\n",
			ty: resp3.TypeVerbatimString,
		},
		{
			in:   "=5\r\nhel:o\r\n+OK\r\n",
			ty:   resp3.TypeVerbatimString,
			rest: "+OK\r\n",
		},
		{
			in:     "=5\r\nhel:o\r\n+OK\r\n",
			nested: true,
			ty:     resp3.TypeVerbatimString,
			rest:   "+OK\r\n",
		},
		{
			in:  "=?\r\n",
			err: resp3.ErrInvalidNumber,
		},
		{
			in:  "=1234567890\r\n",
			err: resp3.ErrSingleReadSizeLimitExceeded,
		},
		{
			in:    "=6\r\n",
			limit: 5,
			err:   resp3.ErrSingleReadSizeLimitExceeded,
		},
	},
}

func TestReaderDiscard(t *testing.T) {
	names := slices.Sorted(maps.Keys(discardTests))

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			for _, test := range discardTests[name] {
				br := bufio.NewReader(strings.NewReader(test.in))

				rr := resp3.NewReader(br)
				rr.SingleReadSizeLimit = test.limit

				ty, err := rr.Discard(test.nested)

				if ty != test.ty {
					t.Errorf("got type %q, expected %q", ty, test.ty)
				}

				assertError(t, test.err, err)

				if test.err == nil {
					rest, _ := io.ReadAll(br)
					if string(rest) != test.rest {
						t.Errorf("got %q left in input, expected %q", string(rest), test.rest)
					}
				}
			}
		})
	}
}

func fuzzAdd(f *testing.F, name string) {
	tests := discardTests[name]
	if len(tests) == 0 {
		panic("no tests found for name " + name)
	}
	for _, test := range tests {
		f.Add([]byte(test.in))
	}
}

func FuzzReader_Array(f *testing.F) {
	fuzzAdd(f, "Array")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _, _ = rr.ReadArrayHeader()
	})
}

func FuzzReader_Attribute(f *testing.F) {
	fuzzAdd(f, "Attribute")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _, _ = rr.ReadAttributeHeader()
	})
}

func FuzzReader_BigNumber(f *testing.F) {
	fuzzAdd(f, "BigNumber")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_ = rr.ReadBigNumber(new(big.Int))
	})
}

func FuzzReader_Boolean(f *testing.F) {
	fuzzAdd(f, "Boolean")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _ = rr.ReadBoolean()
	})
}

func FuzzReader_Double(f *testing.F) {
	fuzzAdd(f, "Double")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _ = rr.ReadDouble()
	})
}

func FuzzReader_BlobError(f *testing.F) {
	fuzzAdd(f, "BlobError")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _, _ = rr.ReadBlobError(nil)
	})
}

func FuzzReader_BlobString(f *testing.F) {
	fuzzAdd(f, "BlobString")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _, _ = rr.ReadBlobString(nil)
	})
}

func FuzzReader_BlobChunk(f *testing.F) {
	fuzzAdd(f, "BlobChunk")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _, _ = rr.ReadBlobChunk(nil)
	})
}

func FuzzReader_BlobChunks(f *testing.F) {
	fuzzAdd(f, "BlobChunk")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _ = rr.ReadBlobChunks(nil)
	})
}

func FuzzReader_End(f *testing.F) {
	fuzzAdd(f, "End")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_ = rr.ReadEnd()
	})
}

func FuzzReader_Integer(f *testing.F) {
	fuzzAdd(f, "Integer")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _ = rr.ReadInteger()
	})
}

func FuzzReader_Map(f *testing.F) {
	fuzzAdd(f, "Map")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _, _ = rr.ReadMapHeader()
	})
}

func FuzzReader_Null(f *testing.F) {
	fuzzAdd(f, "Null")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_ = rr.ReadNull()
	})
}

func FuzzReader_Push(f *testing.F) {
	fuzzAdd(f, "Push")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _, _ = rr.ReadPushHeader()
	})
}

func FuzzReader_Set(f *testing.F) {
	fuzzAdd(f, "Set")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _, _ = rr.ReadSetHeader()
	})
}

func FuzzReader_SimpleError(f *testing.F) {
	fuzzAdd(f, "SimpleError")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _ = rr.ReadSimpleError(nil)
	})
}

func FuzzReader_SimpleString(f *testing.F) {
	fuzzAdd(f, "SimpleString")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _ = rr.ReadSimpleString(nil)
	})
}

func FuzzReader_VerbatimString(f *testing.F) {
	fuzzAdd(f, "VerbatimString")

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _, _ = rr.ReadVerbatimString(nil)
	})
}

func FuzzReader_Discard(f *testing.F) {
	for _, tests := range discardTests {
		for _, test := range tests {
			f.Add([]byte(test.in))
		}
	}

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _ = rr.Discard(false)
	})
}

func FuzzReader_DiscardNested(f *testing.F) {
	for _, tests := range discardTests {
		for _, test := range tests {
			f.Add([]byte(test.in))
		}
	}

	f.Fuzz(func(_ *testing.T, data []byte) {
		rr := resp3.NewReader(bytes.NewReader(data))
		_, _ = rr.Discard(false)
	})
}
