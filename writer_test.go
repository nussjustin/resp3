package resp3_test

import (
	"bufio"
	"bytes"
	"io"
	"math/big"
	"testing"

	"github.com/nussjustin/resp3"
)

func assertBytes(tb testing.TB, expected string, actual []byte) {
	tb.Helper()

	if actualStr := string(actual); actualStr != expected {
		tb.Errorf("read failed. got %q, expected %q", actualStr, expected)
	}
}

func mustWrite(tb testing.TB, w io.Writer, b []byte) {
	tb.Helper()

	if n, err := w.Write(b); err != nil {
		tb.Fatalf("write failed: %s", err)
	} else if n < len(b) {
		tb.Fatalf("failed to write all bytes. wrote %d, expected %d", n, len(b))
	}
}

func TestWriterReset(t *testing.T) {
	var b1 bytes.Buffer
	bw1 := bufio.NewWriter(&b1)
	w := resp3.NewWriter(bw1)

	mustWrite(t, bw1, []byte("hello"))
	_ = bw1.Flush()
	assertBytes(t, "hello", b1.Bytes())

	var b2 bytes.Buffer
	bw2 := bufio.NewWriter(&b2)
	w.Reset(bw2)

	mustWrite(t, bw2, []byte("world"))
	_ = bw1.Flush()
	_ = bw2.Flush()
	assertBytes(t, "hello", b1.Bytes())
	assertBytes(t, "world", b2.Bytes())

	var b3 bytes.Buffer
	w.Reset(&b3)
	mustWrite(t, &b3, []byte("!"))
	_ = bw1.Flush()
	_ = bw2.Flush()
	assertBytes(t, "hello", b1.Bytes())
	assertBytes(t, "world", b2.Bytes())
	assertBytes(t, "!", b3.Bytes())
}

func TestWriterWrite(t *testing.T) {
	t.Run("Array", makeWriteAggregationTest('*',
		(*resp3.Writer).WriteArrayHeader,
		(*resp3.Writer).WriteArrayStreamHeader))
	t.Run("Attribute", makeWriteAggregationTest('|',
		(*resp3.Writer).WriteAttributeHeader,
		(*resp3.Writer).WriteAttributeStreamHeader))
	t.Run("BigNumber", testWriteBigNumber)
	t.Run("Boolean", testWriteBoolean)
	t.Run("Double", testWriteDouble)
	t.Run("BlobError", makeWriteBlobTest('!', (*resp3.Writer).WriteBlobError))
	t.Run("BlobErrorStreamHeader", makeWriteBlobStreamHeader('!', (*resp3.Writer).WriteBlobErrorStreamHeader))
	t.Run("BlobString", makeWriteBlobTest('$', (*resp3.Writer).WriteBlobString))
	t.Run("BlobStringStreamHeader", makeWriteBlobStreamHeader('$', (*resp3.Writer).WriteBlobStringStreamHeader))
	t.Run("BlobChunk", testWriteBlobChunk)
	t.Run("End", testWriteEnd)
	t.Run("Map", makeWriteAggregationTest('%',
		(*resp3.Writer).WriteMapHeader,
		(*resp3.Writer).WriteMapStreamHeader))
	t.Run("Null", testWriteNull)
	t.Run("Number", testWriteNumber)
	t.Run("Push", makeWriteAggregationTest('>',
		(*resp3.Writer).WritePushHeader,
		(*resp3.Writer).WritePushStreamHeader))
	t.Run("Set", makeWriteAggregationTest('~',
		(*resp3.Writer).WriteSetHeader,
		(*resp3.Writer).WriteSetStreamHeader))
	t.Run("SimpleError", makeWriteSimpleTest('-', (*resp3.Writer).WriteSimpleError))
	t.Run("SimpleString", makeWriteSimpleTest('+', (*resp3.Writer).WriteSimpleString))
	t.Run("VerbatimString", testWriteVerbatimString)
}

func newTestWriter(t *testing.T) (rw *resp3.Writer, assert func(expected string, expectedError error, err error)) {
	var b bytes.Buffer
	return resp3.NewWriter(&b), func(expected string, expectedError error, err error) {
		t.Helper()
		if got := b.String(); got != expected {
			t.Errorf("got %q, expected %q", got, expected)
		}
		assertError(t, expectedError, err)
		b.Reset()
	}
}

func makeWriteAggregationTest(ty resp3.Type,
	writeHeader func(*resp3.Writer, int64) error,
	writeStreamHeader func(*resp3.Writer) error) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("Fixed", func(t *testing.T) {
			rw, assert := newTestWriter(t)
			for _, c := range []struct {
				i   int64
				s   string
				err error
			}{
				{-10, "", resp3.ErrInvalidAggregateTypeLength},
				{-1, "", resp3.ErrInvalidAggregateTypeLength},
				{0, string(ty) + "0\r\n", nil},
				{1, string(ty) + "1\r\n", nil},
				{10, string(ty) + "10\r\n", nil},
			} {
				assert(c.s, c.err, writeHeader(rw, c.i))
			}
		})

		t.Run("Streamed", func(t *testing.T) {
			rw, assert := newTestWriter(t)
			assert(string(ty)+"?\r\n", nil, writeStreamHeader(rw))
		})
	}
}

func makeWriteBlobTest(ty resp3.Type, write func(*resp3.Writer, []byte) error) func(t *testing.T) {
	return func(t *testing.T) {
		rw, assert := newTestWriter(t)
		for _, c := range []struct {
			b string
			s string
		}{
			{"", string(ty) + "0\r\n\r\n"},
			{"hello", string(ty) + "5\r\nhello\r\n"},
			{"hello world", string(ty) + "11\r\nhello world\r\n"},
			{"hello\nworld", string(ty) + "11\r\nhello\nworld\r\n"},
			{"hello\rworld", string(ty) + "11\r\nhello\rworld\r\n"},
			{"hello\r\nworld", string(ty) + "12\r\nhello\r\nworld\r\n"},
		} {
			assert(c.s, nil, write(rw, []byte(c.b)))
		}
	}
}

func makeWriteBlobStreamHeader(ty resp3.Type, writeHeader func(*resp3.Writer) error) func(t *testing.T) {
	return func(t *testing.T) {
		rw, assert := newTestWriter(t)
		assert(string(ty)+"?\r\n", nil, writeHeader(rw))
	}
}

func makeWriteSimpleTest(ty resp3.Type, write func(*resp3.Writer, []byte) error) func(t *testing.T) {
	return func(t *testing.T) {
		rw, assert := newTestWriter(t)
		for _, c := range []struct {
			ss  string
			s   string
			err error
		}{
			{"", string(ty) + "\r\n", nil},
			{"hello", string(ty) + "hello\r\n", nil},
			{"hello world", string(ty) + "hello world\r\n", nil},
			{"\r", "", resp3.ErrInvalidSimpleValue},
			{"\n", "", resp3.ErrInvalidSimpleValue},
			{"\r\n", "", resp3.ErrInvalidSimpleValue},
			{"hello\nworld", "", resp3.ErrInvalidSimpleValue},
			{"hello\rworld", "", resp3.ErrInvalidSimpleValue},
			{"hello\r\nworld", "", resp3.ErrInvalidSimpleValue},
		} {
			assert(c.s, c.err, write(rw, []byte(c.ss)))
		}
	}
}

func testWriteBigNumber(t *testing.T) {
	rw, assert := newTestWriter(t)
	for _, c := range []struct {
		n string
		s string
	}{
		{"-100000000000000000000", "(-100000000000000000000\r\n"},
		{"-10000000000000000", "(-10000000000000000\r\n"},
		{"-1000000000000", "(-1000000000000\r\n"},
		{"-100000000", "(-100000000\r\n"},
		{"-10000", "(-10000\r\n"},
		{"-1", "(-1\r\n"},
		{"0", "(0\r\n"},
		{"1", "(1\r\n"},
		{"10000", "(10000\r\n"},
		{"100000000", "(100000000\r\n"},
		{"1000000000000", "(1000000000000\r\n"},
		{"10000000000000000", "(10000000000000000\r\n"},
		{"100000000000000000000", "(100000000000000000000\r\n"},
	} {
		bn, ok := big.NewInt(0).SetString(c.n, 10)
		if !ok {
			t.Fatalf("failed to set number %q", c.n)
		}
		assert(c.s, nil, rw.WriteBigNumber(bn))
	}
}

func testWriteBlobChunk(t *testing.T) {
	rw, assert := newTestWriter(t)
	for _, c := range []struct {
		c string
		s string
	}{
		{"", ";0\r\n"},
		{"hello", ";5\r\nhello\r\n"},
		{"hello world", ";11\r\nhello world\r\n"},
		{"hello\nworld", ";11\r\nhello\nworld\r\n"},
		{"hello\rworld", ";11\r\nhello\rworld\r\n"},
		{"hello\r\nworld", ";12\r\nhello\r\nworld\r\n"},
	} {
		assert(c.s, nil, rw.WriteBlobChunk([]byte(c.c)))
	}
}

func testWriteBoolean(t *testing.T) {
	rw, assert := newTestWriter(t)
	{
		assert("#t\r\n", nil, rw.WriteBoolean(true))
	}
	{
		assert("#f\r\n", nil, rw.WriteBoolean(false))
	}
}

func testWriteDouble(t *testing.T) {
	rw, assert := newTestWriter(t)
	for _, c := range []struct {
		f float64
		s string
	}{
		{-1000.1234, ",-1000.1234\r\n"},
		{-1000, ",-1000\r\n"},
		{-100.123, ",-100.123\r\n"},
		{-100, ",-100\r\n"},
		{-10.12, ",-10.12\r\n"},
		{-10, ",-10\r\n"},
		{-1.1, ",-1.1\r\n"},
		{-1, ",-1\r\n"},
		{0, ",0\r\n"},
		{0.1, ",0.1\r\n"},
		{0.01, ",0.01\r\n"},
		{1, ",1\r\n"},
		{1.1, ",1.1\r\n"},
		{10, ",10\r\n"},
		{10.12, ",10.12\r\n"},
		{100, ",100\r\n"},
		{100.123, ",100.123\r\n"},
		{1000, ",1000\r\n"},
		{1000.1234, ",1000.1234\r\n"},
	} {
		assert(c.s, nil, rw.WriteDouble(c.f))
	}
}

func testWriteEnd(t *testing.T) {
	rw, assert := newTestWriter(t)
	assert(".\r\n", nil, rw.WriteEnd())
}

func testWriteNull(t *testing.T) {
	rw, assert := newTestWriter(t)
	assert("_\r\n", nil, rw.WriteNull())
}

func testWriteNumber(t *testing.T) {
	rw, assert := newTestWriter(t)
	for _, c := range []struct {
		i int64
		s string
	}{
		{-1000, ":-1000\r\n"},
		{-100, ":-100\r\n"},
		{-10, ":-10\r\n"},
		{-1, ":-1\r\n"},
		{0, ":0\r\n"},
		{1, ":1\r\n"},
		{10, ":10\r\n"},
		{100, ":100\r\n"},
		{1000, ":1000\r\n"},
	} {
		assert(c.s, nil, rw.WriteNumber(c.i))
	}
}

func testWriteVerbatimString(t *testing.T) {
	rw, assert := newTestWriter(t)
	for _, c := range []struct {
		p   string
		v   string
		s   string
		err error
	}{
		{"", "hello", "", resp3.ErrInvalidVerbatimString},
		{"t", "hello", "", resp3.ErrInvalidVerbatimString},
		{"tx", "hello", "", resp3.ErrInvalidVerbatimString},
		{"txtx", "hello", "", resp3.ErrInvalidVerbatimString},

		{"foo", "", "=4\r\nfoo:\r\n", nil},
		{"txt", "hello", "=9\r\ntxt:hello\r\n", nil},
		{"mkd", "hello world", "=15\r\nmkd:hello world\r\n", nil},
		{"bar", "hello\r\nworld", "=16\r\nbar:hello\r\nworld\r\n", nil},
	} {
		assert(c.s, c.err, rw.WriteVerbatimString(c.p, c.v))
	}
}
