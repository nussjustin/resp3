package resp3_test

import (
	"bufio"
	"bytes"
	"math"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/nussjustin/resp3"
)

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
		resp3.TypeMap:            true,
		resp3.TypeNumber:         true,
		resp3.TypeNull:           true,
		resp3.TypePush:           true,
		resp3.TypeSet:            true,
		resp3.TypeSimpleError:    true,
		resp3.TypeSimpleString:   true,
		resp3.TypeVerbatimString: true,
	}

	for i := byte(0); i < ^byte(0); i++ {
		rr := resp3.NewReader(bytes.NewReader([]byte{i}))

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

func newTestReader() (rr *resp3.Reader, reset func(string)) {
	r := strings.NewReader("")
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
	rr, reset := newTestReader()
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
	} {
		reset(c.in)
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
	rr, reset := newTestReader()
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
	} {
		rr.SingleReadSizeLimit = c.limit
		reset(c.in)
		buf, chunked, err := readBlob(rr, nil)
		assertError(t, c.err, err)
		if got := string(buf); got != c.s {
			t.Errorf("got %q, expected %q", got, c.s)
		}
		if chunked {
			t.Errorf("got chunked=%v, expected chunked=%v", chunked, false)
		}
	}
}

func runStreamableBlobReadTest(t *testing.T, ty resp3.Type, readBlob func(*resp3.Reader, []byte) ([]byte, bool, error)) {
	runBlobReadTest(t, ty, readBlob)

	p := newTypePrefixFunc(ty)
	rr, reset := newTestReader()

	{
		reset(p("0\r\n"))
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
		reset(p("?\r\n"))
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
	rr, reset := newTestReader()
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
		reset(c.in)
		assertError(t, c.err, readEmpty(rr))
	}
}

func runSimpleReadTest(t *testing.T, ty resp3.Type, readSimple func(*resp3.Reader, []byte) ([]byte, error)) {
	p := newTypePrefixFunc(ty)
	rr, reset := newTestReader()
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
		rr.SingleReadSizeLimit = c.limit
		reset(c.in)
		buf, err := readSimple(rr, nil)
		assertError(t, c.err, err)
		if got := string(buf); got != c.s {
			t.Errorf("got %q, expected %q", got, c.s)
		}
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
	rr, reset := newTestReader()
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
		{in: p("-123456789123456789123456789123456789\r\n"),
			n: newBigInt("-123456789123456789123456789123456789")},
		{in: p("123456789123456789123456789123456789\r\n"),
			n: newBigInt("123456789123456789123456789123456789")},
		{in: p("+123456789123456789123456789123456789\r\n"),
			n: newBigInt("123456789123456789123456789123456789")},
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
		reset(c.in)
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
	rr, reset := newTestReader()
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
		reset(c.in)
		b, err := rr.ReadBoolean()
		assertError(t, c.err, err)
		if b != c.b {
			t.Errorf("got %v, expected %v", b, c.b)
		}
	}
}

func testReadDouble(t *testing.T) {
	p := newTypePrefixFunc(resp3.TypeDouble)
	rr, reset := newTestReader()
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
		reset(c.in)
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
	rr, reset := newTestReader()

	{
		reset(p("0\r\n"))
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
	rr, reset := newTestReader()
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

		{in: p("11000\r\n"+strings.Repeat("hello world", 1000)+"\r\n") + p("0\r\n"),
			s: strings.Repeat("hello world", 1000)},
	} {
		reset(c.in)
		buf, err := rr.ReadBlobChunks(nil)
		assertError(t, c.err, err)
		if got := string(buf); got != c.s {
			t.Errorf("got %q, expected %q", got, c.s)
		}
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
}

func testReadNumber(t *testing.T) {
	p := newTypePrefixFunc(resp3.TypeNumber)
	rr, reset := newTestReader()
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
	} {
		reset(c.in)
		n, err := rr.ReadNumber()
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
	rr, reset := newTestReader()
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

		{in: p("\r\nfoo:\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("3\r\nbar\r\n"), err: resp3.ErrInvalidVerbatimStringPrefix},
		{in: p("4\r\n:bar\r\n"), err: resp3.ErrInvalidVerbatimStringPrefix},
		{in: p("5\r\nf:bar\r\n"), err: resp3.ErrInvalidVerbatimStringPrefix},
		{in: p("6\r\nfo:bar\r\n"), err: resp3.ErrInvalidVerbatimStringPrefix},
		{in: p("4\r\nfoo:\r\n"), s: "foo:"},
		{in: p("5\r\nfoo:b\r\n"), s: "foo:b"},
		{in: p("6\r\nfoo:ba\r\n"), s: "foo:ba"},
		{in: p("7\r\nfoo:bar\r\n"), s: "foo:bar"},

		{in: p("5\r\nfoo:hello world\r\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("10\r\nfoo:hello\r\n"), err: resp3.ErrUnexpectedEOL},

		{in: p("7\r\nfoo:bar"), err: resp3.ErrUnexpectedEOL},
		{in: p("7\r\nfoo:bar\n"), err: resp3.ErrUnexpectedEOL},
		{in: p("7\r\nfoo:bar\n\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("7\r\nfoo:bar\r"), err: resp3.ErrUnexpectedEOL},
		{in: p("7\r\nfoo:bar\r\r"), err: resp3.ErrUnexpectedEOL},

		{
			in: p("11004\r\nfoo:" + strings.Repeat("hello world", 1000) + "\r\n"),
			s:  "foo:" + strings.Repeat("hello world", 1000),
		},

		{
			in: p(strconv.Itoa(resp3.DefaultSingleReadSizeLimit) + "\r\nfoo:" +
				strings.Repeat("a", resp3.DefaultSingleReadSizeLimit-len("foo:")) + "\r\n"),
			s: "foo:" + strings.Repeat("a", resp3.DefaultSingleReadSizeLimit-len("foo:")),
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
			s:     "foo:" + strings.Repeat("a", resp3.DefaultSingleReadSizeLimit-len("foo:")+1),
		},

		{
			in:    p("7\r\nfoo:bar\r\n"),
			limit: 7,
			s:     "foo:bar",
		},

		{
			in:    p("7\r\nfoo:bar\r\n"),
			limit: 6,
			err:   resp3.ErrSingleReadSizeLimitExceeded,
		},
	} {
		rr.SingleReadSizeLimit = c.limit
		reset(c.in)
		buf, err := rr.ReadVerbatimString(nil)
		assertError(t, c.err, err)
		if got := string(buf); got != c.s {
			t.Errorf("got %q, expected %q", got, c.s)
		}
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
	b.Run("End", makeReadEmptyBenchmark(resp3.TypeEnd, (*resp3.Reader).ReadEnd))
	b.Run("Map", makeReadAggregationBenchmark(resp3.TypeMap, (*resp3.Reader).ReadMapHeader))
	b.Run("Null", makeReadEmptyBenchmark(resp3.TypeNumber, (*resp3.Reader).ReadNull))
	b.Run("Number", benchmarkReadNumber)
	b.Run("Push", makeReadAggregationBenchmark(resp3.TypePush, (*resp3.Reader).ReadPushHeader))
	b.Run("Set", makeReadAggregationBenchmark(resp3.TypeSimpleError, (*resp3.Reader).ReadSetHeader))
	b.Run("SimpleError", makeReadSimpleBenchmark(resp3.TypeSimpleError, (*resp3.Reader).ReadSimpleError))
	b.Run("SimpleString", makeReadSimpleBenchmark(resp3.TypeSimpleString, (*resp3.Reader).ReadSimpleString))
	b.Run("VerbatimString", benchmarkReadVerbatimString)
}

func makeReadAggregationBenchmark(ty resp3.Type, readHeader func(*resp3.Reader) (int64, bool, error)) func(*testing.B) {
	return func(b *testing.B) {
		b.Run("Fixed", func(b *testing.B) {
			in := string(ty) + "16\r\n"
			rr, reset := newTestReader()
			for i := 0; i < b.N; i++ {
				reset(in)
				_, _, _ = readHeader(rr)
			}
		})

		b.Run("Streamed", func(b *testing.B) {
			in := string(ty) + "?\r\n"
			rr, reset := newTestReader()
			for i := 0; i < b.N; i++ {
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
			rr, reset := newTestReader()
			for i := 0; i < b.N; i++ {
				reset(in)
				_, _, _ = readBlob(rr, buf[:0])
			}
		})

		b.Run("Streamed", func(b *testing.B) {
			in := string(ty) + "?\r\n"
			rr, reset := newTestReader()
			for i := 0; i < b.N; i++ {
				reset(in)
				_, _, _ = readBlob(rr, nil)
			}
		})
	}
}

func makeReadEmptyBenchmark(ty resp3.Type, readEmpty func(*resp3.Reader) error) func(*testing.B) {
	return func(b *testing.B) {
		in := string(ty) + "\r\n"
		rr, reset := newTestReader()
		for i := 0; i < b.N; i++ {
			reset(in)
			_ = readEmpty(rr)
		}
	}
}

func makeReadSimpleBenchmark(ty resp3.Type, readSimple func(*resp3.Reader, []byte) ([]byte, error)) func(*testing.B) {
	return func(b *testing.B) {
		var buf [32 + len("\r\n")]byte
		in := string(ty) + "hello world! what's up? kthxbye!\r\n"
		rr, reset := newTestReader()
		for i := 0; i < b.N; i++ {
			reset(in)
			_, _ = readSimple(rr, buf[:0])
		}
	}
}

var benchVarBigNumber = new(big.Int)

func benchmarkReadBigNumber(b *testing.B) {
	in := string(resp3.TypeBigNumber) + "123456789123456789123456789123456789\r\n"
	rr, reset := newTestReader()
	for i := 0; i < b.N; i++ {
		reset(in)
		_ = rr.ReadBigNumber(benchVarBigNumber)
	}
}

var benchVarBoolean bool

func benchmarkReadBoolean(b *testing.B) {
	in := string(resp3.TypeBoolean) + "t\r\n"
	rr, reset := newTestReader()
	for i := 0; i < b.N; i++ {
		reset(in)
		benchVarBoolean, _ = rr.ReadBoolean()
	}
}

var benchVarDouble float64

func benchmarkReadDouble(b *testing.B) {
	in := string(resp3.TypeDouble) + "1234.5678\r\n"
	rr, reset := newTestReader()
	for i := 0; i < b.N; i++ {
		reset(in)
		benchVarDouble, _ = rr.ReadDouble()
	}
}

var benchVarBlobChunk []byte

func benchmarkReadBlobChunk(b *testing.B) {
	b.Run("Chunk", func(b *testing.B) {
		var buf [32]byte
		in := string(resp3.TypeBlobChunk) + "32\r\nhello world! what's up? kthxbye!\r\n"
		rr, reset := newTestReader()
		for i := 0; i < b.N; i++ {
			reset(in)
			benchVarBlobChunk, _, _ = rr.ReadBlobChunk(buf[:0])
		}
	})

	b.Run("End", func(b *testing.B) {
		in := string(resp3.TypeBlobChunk) + "0\r\n"
		rr, reset := newTestReader()
		for i := 0; i < b.N; i++ {
			reset(in)
			benchVarBlobChunk, _, _ = rr.ReadBlobChunk(nil)
		}
	})
}

var benchVarBlobChunks []byte

func benchmarkReadBlobChunks(b *testing.B) {
	var buf [32]byte
	in := string(resp3.TypeBlobChunk) + "5\r\nhello\r\n" +
		string(resp3.TypeBlobChunk) + "1\r\n \r\n" +
		string(resp3.TypeBlobChunk) + "5\r\nworld\r\n" +
		string(resp3.TypeBlobChunk) + "10\r\nwhat's up?\r\n" +
		string(resp3.TypeBlobChunk) + "1\r\n \r\n" +
		string(resp3.TypeBlobChunk) + "8\r\nkthxbye!\r\n" +
		string(resp3.TypeBlobChunk) + "0\r\n"
	rr, reset := newTestReader()
	for i := 0; i < b.N; i++ {
		reset(in)
		benchVarBlobChunks, _ = rr.ReadBlobChunks(buf[:0])
	}
}

var benchVarNumber int64

func benchmarkReadNumber(b *testing.B) {
	in := string(resp3.TypeNumber) + "12345678\r\n"
	rr, reset := newTestReader()
	for i := 0; i < b.N; i++ {
		reset(in)
		benchVarNumber, _ = rr.ReadNumber()
	}
}

var benchVarVerbatimString []byte

func benchmarkReadVerbatimString(b *testing.B) {
	var buf [36]byte
	in := string(resp3.TypeVerbatimString) + "36\r\ntxt:hello world! what's up? kthxbye!\r\n"
	rr, reset := newTestReader()
	for i := 0; i < b.N; i++ {
		reset(in)
		benchVarVerbatimString, _ = rr.ReadVerbatimString(buf[:0])
	}
}
