package resp3_test

import (
	"bufio"
	"bytes"
	"math"
	"math/big"
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
		assertError(t, nil, err)
		if types[resp3.Type(i)] && ty != resp3.Type(i) {
			t.Errorf("got %v, expected %v", ty, resp3.Type(i))
		} else if !types[resp3.Type(i)] && ty != resp3.TypeInvalid {
			t.Errorf("got %v, expected %v", ty, resp3.TypeInvalid)
		}
	}
}

func TestReaderRead(t *testing.T) {
	t.Run("Array", makeReadAggregationTest('*', (*resp3.Reader).ReadArrayHeader))
	t.Run("Attribute", makeReadAggregationTest('|', (*resp3.Reader).ReadAttributeHeader))
	t.Run("BigNumber", testReadBigNumber)
	t.Run("Boolean", testReadBoolean)
	t.Run("Double", testReadDouble)
	t.Run("BlobError", makeReadBlobTest('!', (*resp3.Reader).ReadBlobError))
	t.Run("BlobString", makeReadBlobTest('$', (*resp3.Reader).ReadBlobString))
	t.Run("BlobChunk", testReadBlobChunk)
	t.Run("End", makeReadEmptyTest('.', (*resp3.Reader).ReadEnd))
	t.Run("Map", makeReadAggregationTest('%', (*resp3.Reader).ReadMapHeader))
	t.Run("Null", makeReadEmptyTest('_', (*resp3.Reader).ReadNull))
	t.Run("Number", testReadNumber)
	t.Run("Push", makeReadAggregationTest('>', (*resp3.Reader).ReadPushHeader))
	t.Run("Set", makeReadAggregationTest('~', (*resp3.Reader).ReadSetHeader))
	t.Run("SimpleError", makeReadSimpleTest('-', (*resp3.Reader).ReadSimpleError))
	t.Run("SimpleString", makeReadSimpleTest('+', (*resp3.Reader).ReadSimpleString))
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

func makeReadAggregationTest(ty resp3.Type, readHeader func(*resp3.Reader) (int64, bool, error)) func(*testing.T) {
	return func(t *testing.T) {
		rr, reset := newTestReader()
		for _, c := range []struct {
			in      string
			n       int64
			chunked bool
			err     error
		}{
			{"", 0, false, resp3.ErrUnexpectedEOL},
			{"A", 0, false, resp3.ErrUnexpectedType},

			{string(resp3.TypeBlobString), 0, false, resp3.ErrUnexpectedType},
			{string(resp3.TypeInvalid), 0, false, resp3.ErrUnexpectedType},

			{string(ty) + "", 0, false, resp3.ErrUnexpectedEOL},
			{string(ty) + "\n", 0, false, resp3.ErrUnexpectedEOL},
			{string(ty) + "\n\r", 0, false, resp3.ErrUnexpectedEOL},
			{string(ty) + "\r", 0, false, resp3.ErrUnexpectedEOL},
			{string(ty) + "\r\n", 0, false, resp3.ErrUnexpectedEOL},

			{string(ty) + "a\r\n", 0, false, resp3.ErrInvalidAggregateTypeLength},
			{string(ty) + "-2\r\n", 0, false, resp3.ErrInvalidAggregateTypeLength},
			{string(ty) + "-1\r\n", 0, false, resp3.ErrInvalidAggregateTypeLength},

			{string(ty) + "0\r\n", 0, false, nil},
			{string(ty) + "1\r\n", 1, false, nil},
			{string(ty) + "2\r\n", 2, false, nil},

			{string(ty) + "?\r\n", -1, true, nil},
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
}

func makeReadBlobTest(ty resp3.Type, readBlob func(*resp3.Reader, []byte) ([]byte, bool, error)) func(*testing.T) {
	return func(t *testing.T) {
		rr, reset := newTestReader()
		for _, c := range []struct {
			in      string
			s       string
			chunked bool
			err     error
		}{
			{"", "", false, resp3.ErrUnexpectedEOL},

			{"A", "", false, resp3.ErrUnexpectedType},
			{string(resp3.TypeArray), "", false, resp3.ErrUnexpectedType},
			{string(resp3.TypeInvalid), "", false, resp3.ErrUnexpectedType},

			{string(ty), "", false, resp3.ErrUnexpectedEOL},
			{string(ty) + "\n", "", false, resp3.ErrUnexpectedEOL},
			{string(ty) + "\n\r", "", false, resp3.ErrUnexpectedEOL},
			{string(ty) + "\r", "", false, resp3.ErrUnexpectedEOL},
			{string(ty) + "\r\n", "", false, resp3.ErrUnexpectedEOL},

			{string(ty) + "-2\r\n", "", false, resp3.ErrInvalidBlobLength},
			{string(ty) + "-1\r\n", "", false, resp3.ErrInvalidBlobLength},

			{string(ty) + "\r\nhello\r\n", "", false, resp3.ErrUnexpectedEOL},

			{string(ty) + "0\r\n", "", false, resp3.ErrUnexpectedEOL},

			{string(ty) + "5\r\nhello\r\n", "hello", false, nil},

			{string(ty) + "5\r\nhello world\r\n", "", false, resp3.ErrUnexpectedEOL},
			{string(ty) + "10\r\nhello\r\n", "", false, resp3.ErrUnexpectedEOL},

			{string(ty) + "5\r\nhello", "", false, resp3.ErrUnexpectedEOL},
			{string(ty) + "5\r\nhello\n", "", false, resp3.ErrUnexpectedEOL},
			{string(ty) + "5\r\nhello\n\r", "", false, resp3.ErrUnexpectedEOL},
			{string(ty) + "5\r\nhello\r", "", false, resp3.ErrUnexpectedEOL},
			{string(ty) + "5\r\nhello\r\r", "", false, resp3.ErrUnexpectedEOL},

			{string(ty) + "11000\r\n" + strings.Repeat("hello world", 1000) + "\r\n",
				strings.Repeat("hello world", 1000), false, nil},

			{string(ty) + "?\r\n", "", true, nil},
		} {
			reset(c.in)
			buf, chunked, err := readBlob(rr, nil)
			assertError(t, c.err, err)
			if got := string(buf); got != c.s {
				t.Errorf("got %q, expected %q", got, c.s)
			}
			if chunked != c.chunked {
				t.Errorf("got chunked=%v, expected chunked=%v", chunked, c.chunked)
			}
		}
	}
}

func makeReadEmptyTest(ty resp3.Type, readEmpty func(*resp3.Reader) error) func(*testing.T) {
	return func(t *testing.T) {
		rr, reset := newTestReader()
		for _, c := range []struct {
			in  string
			err error
		}{
			{"", resp3.ErrUnexpectedEOL},

			{"A", resp3.ErrUnexpectedType},
			{string(resp3.TypeArray), resp3.ErrUnexpectedType},
			{string(resp3.TypeInvalid), resp3.ErrUnexpectedType},

			{string(ty), resp3.ErrUnexpectedEOL},
			{string(ty) + "\n", resp3.ErrUnexpectedEOL},
			{string(ty) + "\n\r", resp3.ErrUnexpectedEOL},
			{string(ty) + "\r", resp3.ErrUnexpectedEOL},
			{string(ty) + "\r\r", resp3.ErrUnexpectedEOL},

			{string(ty) + "\r\n", nil},

			{string(ty) + ".\r\n", resp3.ErrUnexpectedEOL},
			{string(ty) + "#\r\n", resp3.ErrUnexpectedEOL},
			{string(ty) + "A\r\n", resp3.ErrUnexpectedEOL},
		} {
			reset(c.in)
			assertError(t, c.err, readEmpty(rr))
		}
	}
}

func makeReadSimpleTest(ty resp3.Type, readSimple func(*resp3.Reader, []byte) ([]byte, error)) func(*testing.T) {
	return func(t *testing.T) {
		rr, reset := newTestReader()
		for _, c := range []struct {
			in  string
			s   string
			err error
		}{
			{"", "", resp3.ErrUnexpectedEOL},

			{"A", "", resp3.ErrUnexpectedType},
			{string(resp3.TypeArray), "", resp3.ErrUnexpectedType},
			{string(resp3.TypeInvalid), "", resp3.ErrUnexpectedType},

			{string(ty), "", resp3.ErrUnexpectedEOL},
			{string(ty) + "\n", "", resp3.ErrUnexpectedEOL},
			{string(ty) + "\n\r", "", resp3.ErrUnexpectedEOL},
			{string(ty) + "\r", "", resp3.ErrUnexpectedEOL},
			{string(ty) + "\r\r", "", resp3.ErrUnexpectedEOL},

			{string(ty) + "\r\n", "", nil},
			{string(ty) + "OK\r\n", "OK", nil},
			{string(ty) + strings.Repeat("hello world", 1000) + "\r\n",
				strings.Repeat("hello world", 1000), nil},
		} {
			reset(c.in)
			buf, err := readSimple(rr, nil)
			assertError(t, c.err, err)
			if got := string(buf); got != c.s {
				t.Errorf("got %q, expected %q", got, c.s)
			}
		}
	}
}

func testReadBigNumber(t *testing.T) {
	newBigInt := func(s string) *big.Int {
		n, _ := new(big.Int).SetString(s, 10)
		return n
	}
	rr, reset := newTestReader()
	for _, c := range []struct {
		in  string
		n   *big.Int
		err error
	}{
		{"", nil, resp3.ErrUnexpectedEOL},

		{"A", nil, resp3.ErrUnexpectedType},
		{string(resp3.TypeArray), nil, resp3.ErrUnexpectedType},
		{string(resp3.TypeInvalid), nil, resp3.ErrUnexpectedType},

		{string(resp3.TypeBigNumber), nil, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBigNumber) + "\n", nil, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBigNumber) + "\n\r", nil, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBigNumber) + "\r", nil, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBigNumber) + "\r\n", nil, resp3.ErrUnexpectedEOL},

		{string(resp3.TypeBigNumber) + "-10\r\n", big.NewInt(-10), nil},
		{string(resp3.TypeBigNumber) + "-1\r\n", big.NewInt(-1), nil},
		{string(resp3.TypeBigNumber) + "0\r\n", big.NewInt(0), nil},
		{string(resp3.TypeBigNumber) + "1\r\n", big.NewInt(1), nil},
		{string(resp3.TypeBigNumber) + "10\r\n", big.NewInt(10), nil},
		{string(resp3.TypeBigNumber) + "-123456789123456789123456789123456789\r\n",
			newBigInt("-123456789123456789123456789123456789"), nil},
		{string(resp3.TypeBigNumber) + "123456789123456789123456789123456789\r\n",
			newBigInt("123456789123456789123456789123456789"), nil},
		{string(resp3.TypeBigNumber) + "+123456789123456789123456789123456789\r\n",
			newBigInt("123456789123456789123456789123456789"), nil},
		{string(resp3.TypeBigNumber) + "+1\r\n", big.NewInt(1), nil},

		{string(resp3.TypeBigNumber) + "A\r\n", nil, resp3.ErrInvalidBigNumber},
		{string(resp3.TypeBigNumber) + "1a\r\n", nil, resp3.ErrInvalidBigNumber},
		{string(resp3.TypeBigNumber) + "1.\r\n", nil, resp3.ErrInvalidBigNumber},
		{string(resp3.TypeBigNumber) + "1.0\r\n", nil, resp3.ErrInvalidBigNumber},
		{string(resp3.TypeBigNumber) + "1.01\r\n", nil, resp3.ErrInvalidBigNumber},
		{string(resp3.TypeBigNumber) + "#\r\n", nil, resp3.ErrInvalidBigNumber},
		{string(resp3.TypeBigNumber) + "-\r\n", nil, resp3.ErrInvalidBigNumber},
		{string(resp3.TypeBigNumber) + "+\r\n", nil, resp3.ErrInvalidBigNumber},
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
	rr, reset := newTestReader()
	for _, c := range []struct {
		in  string
		b   bool
		err error
	}{
		{"", false, resp3.ErrUnexpectedEOL},

		{"A", false, resp3.ErrUnexpectedType},
		{string(resp3.TypeArray), false, resp3.ErrUnexpectedType},
		{string(resp3.TypeInvalid), false, resp3.ErrUnexpectedType},

		{string(resp3.TypeBoolean), false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBoolean) + "\n", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBoolean) + "\n\r", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBoolean) + "\r", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBoolean) + "\r\r", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBoolean) + "f\n", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBoolean) + "f\n\r", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBoolean) + "f\r", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBoolean) + "f\r\r", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBoolean) + "t\n", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBoolean) + "t\n\r", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBoolean) + "t\r", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBoolean) + "t\r\r", false, resp3.ErrUnexpectedEOL},

		{string(resp3.TypeBoolean) + "f\r\n", false, nil},
		{string(resp3.TypeBoolean) + "t\r\n", true, nil},

		{string(resp3.TypeBoolean) + "#\r\n", false, resp3.ErrInvalidBoolean},
		{string(resp3.TypeBoolean) + "A\r\n", false, resp3.ErrInvalidBoolean},
		{string(resp3.TypeBoolean) + "F\r\n", false, resp3.ErrInvalidBoolean},
		{string(resp3.TypeBoolean) + "T\r\n", false, resp3.ErrInvalidBoolean},
		{string(resp3.TypeBoolean) + "Z\r\n", false, resp3.ErrInvalidBoolean},
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
	rr, reset := newTestReader()
	for _, c := range []struct {
		in  string
		f   float64
		err error
	}{
		{"", 0, resp3.ErrUnexpectedEOL},

		{"A", 0, resp3.ErrUnexpectedType},
		{string(resp3.TypeArray), 0, resp3.ErrUnexpectedType},
		{string(resp3.TypeInvalid), 0, resp3.ErrUnexpectedType},

		{string(resp3.TypeDouble), 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeDouble) + "\n", 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeDouble) + "\n\r", 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeDouble) + "\r", 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeDouble) + "\r\n", 0, resp3.ErrUnexpectedEOL},

		{string(resp3.TypeDouble) + "-1", 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeDouble) + "0", 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeDouble) + "1", 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeDouble) + "inf", 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeDouble) + "-inf", 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeDouble) + "+inf", 0, resp3.ErrUnexpectedEOL},

		{string(resp3.TypeDouble) + "-1.2\r\n", -1.2, nil},
		{string(resp3.TypeDouble) + "-1.0\r\n", -1, nil},
		{string(resp3.TypeDouble) + "-1\r\n", -1, nil},
		{string(resp3.TypeDouble) + "-0.01\r\n", -0.01, nil},
		{string(resp3.TypeDouble) + "-0.1\r\n", -0.1, nil},
		{string(resp3.TypeDouble) + "-0.0\r\n", 0, nil},
		{string(resp3.TypeDouble) + "0\r\n", 0, nil},
		{string(resp3.TypeDouble) + "0.0\r\n", 0, nil},
		{string(resp3.TypeDouble) + "0.01\r\n", 0.01, nil},
		{string(resp3.TypeDouble) + "0.1\r\n", 0.1, nil},
		{string(resp3.TypeDouble) + "1\r\n", 1, nil},
		{string(resp3.TypeDouble) + "1.0\r\n", 1, nil},
		{string(resp3.TypeDouble) + "1.2\r\n", 1.2, nil},

		{string(resp3.TypeDouble) + "1.\r\n", 1, nil},
		{string(resp3.TypeDouble) + "1.01\r\n", 1.01, nil},
		{string(resp3.TypeDouble) + "+1\r\n", 1, nil},

		{string(resp3.TypeDouble) + "inf\r\n", math.Inf(1), nil},
		{string(resp3.TypeDouble) + "+inf\r\n", math.Inf(1), nil}, // not specified, but handled by ParseFloat
		{string(resp3.TypeDouble) + "-inf\r\n", math.Inf(-1), nil},

		{string(resp3.TypeDouble) + "A\r\n", 0, resp3.ErrInvalidDouble},
		{string(resp3.TypeDouble) + "1a\r\n", 0, resp3.ErrInvalidDouble},
		{string(resp3.TypeDouble) + "#\r\n", 0, resp3.ErrInvalidDouble},
		{string(resp3.TypeDouble) + "-\r\n", 0, resp3.ErrInvalidDouble},
		{string(resp3.TypeDouble) + "+\r\n", 0, resp3.ErrInvalidDouble},
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
	rr, reset := newTestReader()
	for _, c := range []struct {
		in   string
		s    string
		last bool
		err  error
	}{
		{"", "", false, resp3.ErrUnexpectedEOL},

		{"A", "", false, resp3.ErrUnexpectedType},
		{string(resp3.TypeArray), "", false, resp3.ErrUnexpectedType},
		{string(resp3.TypeInvalid), "", false, resp3.ErrUnexpectedType},

		{string(resp3.TypeBlobChunk), "", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBlobChunk) + "\n", "", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBlobChunk) + "\n\r", "", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBlobChunk) + "\r", "", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBlobChunk) + "\r\n", "", false, resp3.ErrUnexpectedEOL},

		{string(resp3.TypeBlobChunk) + "-2\r\n", "", false, resp3.ErrInvalidBlobLength},
		{string(resp3.TypeBlobChunk) + "-1\r\n", "", false, resp3.ErrInvalidBlobLength},

		{string(resp3.TypeBlobChunk) + "\r\nhello\r\n", "", false, resp3.ErrUnexpectedEOL},

		{string(resp3.TypeBlobChunk) + "0\r\n", "", true, nil},

		{string(resp3.TypeBlobChunk) + "5\r\nhello\r\n", "hello", false, nil},

		{string(resp3.TypeBlobChunk) + "5\r\nhello world\r\n", "", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBlobChunk) + "10\r\nhello\r\n", "", false, resp3.ErrUnexpectedEOL},

		{string(resp3.TypeBlobChunk) + "5\r\nhello", "", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBlobChunk) + "5\r\nhello\n", "", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBlobChunk) + "5\r\nhello\n\r", "", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBlobChunk) + "5\r\nhello\r", "", false, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeBlobChunk) + "5\r\nhello\r\r", "", false, resp3.ErrUnexpectedEOL},

		{string(resp3.TypeBlobChunk) + "11000\r\n" + strings.Repeat("hello world", 1000) + "\r\n",
			strings.Repeat("hello world", 1000), false, nil},
	} {
		reset(c.in)
		buf, last, err := rr.ReadBlobChunk(nil)
		assertError(t, c.err, err)
		if got := string(buf); got != c.s {
			t.Errorf("got %q, expected %q", got, c.s)
		}
		if last != c.last {
			t.Errorf("got last=%v, expected last=%v", last, c.last)
		}
	}
}

func testReadNumber(t *testing.T) {
	rr, reset := newTestReader()
	for _, c := range []struct {
		in  string
		n   int64
		err error
	}{
		{"", 0, resp3.ErrUnexpectedEOL},

		{"A", 0, resp3.ErrUnexpectedType},
		{string(resp3.TypeArray), 0, resp3.ErrUnexpectedType},
		{string(resp3.TypeInvalid), 0, resp3.ErrUnexpectedType},

		{string(resp3.TypeNumber), 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeNumber) + "\n", 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeNumber) + "\n\r", 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeNumber) + "\r", 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeNumber) + "\r\n", 0, resp3.ErrUnexpectedEOL},

		{string(resp3.TypeNumber) + "-10\r\n", -10, nil},
		{string(resp3.TypeNumber) + "-1\r\n", -1, nil},
		{string(resp3.TypeNumber) + "0\r\n", 0, nil},
		{string(resp3.TypeNumber) + "1\r\n", 1, nil},
		{string(resp3.TypeNumber) + "10\r\n", 10, nil},

		{string(resp3.TypeNumber) + "A\r\n", 0, resp3.ErrInvalidNumber},
		{string(resp3.TypeNumber) + "1a\r\n", 0, resp3.ErrInvalidNumber},
		{string(resp3.TypeNumber) + "1.\r\n", 0, resp3.ErrInvalidNumber},
		{string(resp3.TypeNumber) + "1.0\r\n", 0, resp3.ErrInvalidNumber},
		{string(resp3.TypeNumber) + "1.01\r\n", 0, resp3.ErrInvalidNumber},
		{string(resp3.TypeNumber) + "#\r\n", 0, resp3.ErrInvalidNumber},
		{string(resp3.TypeNumber) + "-\r\n", 0, resp3.ErrUnexpectedEOL},
		{string(resp3.TypeNumber) + "+\r\n", 0, resp3.ErrInvalidNumber},
		{string(resp3.TypeNumber) + "+1\r\n", 0, resp3.ErrInvalidNumber},
	} {
		reset(c.in)
		n, err := rr.ReadNumber()
		assertError(t, c.err, err)
		if n != c.n {
			t.Errorf("got %d, expected %d", n, c.n)
		}
	}
}

func testReadVerbatimString(t *testing.T) {
	rr, reset := newTestReader()
	for _, c := range []struct {
		in  string
		s   string
		err error
	}{
		{"", "", resp3.ErrUnexpectedEOL},

		{"A", "", resp3.ErrUnexpectedType},
		{string(resp3.TypeArray), "", resp3.ErrUnexpectedType},
		{string(resp3.TypeInvalid), "", resp3.ErrUnexpectedType},

		{string(resp3.TypeVerbatimString), "", resp3.ErrUnexpectedEOL},
		{string(resp3.TypeVerbatimString) + "\n", "", resp3.ErrUnexpectedEOL},
		{string(resp3.TypeVerbatimString) + "\n\r", "", resp3.ErrUnexpectedEOL},
		{string(resp3.TypeVerbatimString) + "\r", "", resp3.ErrUnexpectedEOL},
		{string(resp3.TypeVerbatimString) + "\r\n", "", resp3.ErrUnexpectedEOL},

		{string(resp3.TypeVerbatimString) + "\r\nfoo:\r\n", "", resp3.ErrUnexpectedEOL},

		{string(resp3.TypeVerbatimString) + "3\r\nbar\r\n", "", resp3.ErrInvalidVerbatimStringPrefix},
		{string(resp3.TypeVerbatimString) + "4\r\n:bar\r\n", "", resp3.ErrInvalidVerbatimStringPrefix},
		{string(resp3.TypeVerbatimString) + "5\r\nf:bar\r\n", "", resp3.ErrInvalidVerbatimStringPrefix},
		{string(resp3.TypeVerbatimString) + "6\r\nfo:bar\r\n", "", resp3.ErrInvalidVerbatimStringPrefix},
		{string(resp3.TypeVerbatimString) + "4\r\nfoo:\r\n", "foo:", nil},
		{string(resp3.TypeVerbatimString) + "5\r\nfoo:b\r\n", "foo:b", nil},
		{string(resp3.TypeVerbatimString) + "6\r\nfoo:ba\r\n", "foo:ba", nil},
		{string(resp3.TypeVerbatimString) + "7\r\nfoo:bar\r\n", "foo:bar", nil},

		{string(resp3.TypeVerbatimString) + "5\r\nfoo:hello world\r\n", "", resp3.ErrUnexpectedEOL},
		{string(resp3.TypeVerbatimString) + "10\r\nfoo:hello\r\n", "", resp3.ErrUnexpectedEOL},

		{string(resp3.TypeVerbatimString) + "7\r\nfoo:bar", "", resp3.ErrUnexpectedEOL},
		{string(resp3.TypeVerbatimString) + "7\r\nfoo:bar\n", "", resp3.ErrUnexpectedEOL},
		{string(resp3.TypeVerbatimString) + "7\r\nfoo:bar\n\r", "", resp3.ErrUnexpectedEOL},
		{string(resp3.TypeVerbatimString) + "7\r\nfoo:bar\r", "", resp3.ErrUnexpectedEOL},
		{string(resp3.TypeVerbatimString) + "7\r\nfoo:bar\r\r", "", resp3.ErrUnexpectedEOL},

		{string(resp3.TypeVerbatimString) + "11004\r\nfoo:" + strings.Repeat("hello world", 1000) + "\r\n",
			"foo:" + strings.Repeat("hello world", 1000), nil},
	} {
		reset(c.in)
		buf, err := rr.ReadVerbatimString(nil)
		assertError(t, c.err, err)
		if got := string(buf); got != c.s {
			t.Errorf("got %q, expected %q", got, c.s)
		}
	}
}

func BenchmarkReaderRead(b *testing.B) {
	b.Run("Array", makeReadAggregationBenchmark('*', (*resp3.Reader).ReadArrayHeader))
	b.Run("Attribute", makeReadAggregationBenchmark('|', (*resp3.Reader).ReadAttributeHeader))
	b.Run("BigNumber", benchmarkReadBigNumber)
	b.Run("Boolean", benchmarkReadBoolean)
	b.Run("Double", benchmarkReadDouble)
	b.Run("BlobError", makeReadBlobBenchmark('!', (*resp3.Reader).ReadBlobError))
	b.Run("BlobString", makeReadBlobBenchmark('$', (*resp3.Reader).ReadBlobString))
	b.Run("BlobChunk", benchmarkReadBlobChunk)
	b.Run("End", makeReadEmptyBenchmark('.', (*resp3.Reader).ReadEnd))
	b.Run("Map", makeReadAggregationBenchmark('%', (*resp3.Reader).ReadMapHeader))
	b.Run("Null", makeReadEmptyBenchmark('_', (*resp3.Reader).ReadNull))
	b.Run("Number", benchmarkReadNumber)
	b.Run("Push", makeReadAggregationBenchmark('>', (*resp3.Reader).ReadPushHeader))
	b.Run("Set", makeReadAggregationBenchmark('~', (*resp3.Reader).ReadSetHeader))
	b.Run("SimpleError", makeReadSimpleBenchmark('-', (*resp3.Reader).ReadSimpleError))
	b.Run("SimpleString", makeReadSimpleBenchmark('+', (*resp3.Reader).ReadSimpleString))
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
