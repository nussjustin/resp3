// +build integration

package resp3_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/nussjustin/resp3"
)

func mustWriteLines(tb testing.TB, w io.Writer, lines ...string) {
	tb.Helper()

	for _, line := range lines {
		if _, err := w.Write([]byte(line + "\r\n")); err != nil {
			tb.Fatalf("failed to write line %q: %s", line, err)
		}
	}
}

func assertReadBytesFunc(tb testing.TB, typeName string, f func([]byte) ([]byte, error), expected []byte) {
	tb.Helper()

	got, err := f(nil)
	switch {
	case err != nil:
		tb.Fatalf("failed to read %s: %s", typeName, err)
	case !bytes.Equal(got, expected):
		tb.Fatalf("got %q, expected %q", got, expected)
	case (got == nil && expected != nil) || (got != nil && expected == nil):
		tb.Fatalf("got %#v, expected %#v", got, expected)
	}
}

func assertReadNumberFunc(tb testing.TB, typeName string, f func() (int64, error), expected int64) {
	tb.Helper()

	if got, err := f(); err != nil {
		tb.Fatalf("failed to read %s: %s", typeName, err)
	} else if got != expected {
		tb.Fatalf("got %d, expected %d", got, expected)
	}
}

func assertReadSetHeader(tb testing.TB, r *resp3.Reader, n int64) {
	tb.Helper()
	assertReadNumberFunc(tb, "set header", r.ReadSetHeader, n)
}

func assertReadBlobString(tb testing.TB, r *resp3.Reader, s []byte) {
	tb.Helper()
	assertReadBytesFunc(tb, "blob string", r.ReadBlobString, s)
}

func assertReadError(tb testing.TB, r *resp3.Reader, s []byte) {
	tb.Helper()
	assertReadBytesFunc(tb, "error", r.ReadSimpleError, s)
}

func assertReadNull(tb testing.TB, r *resp3.Reader) {
	tb.Helper()
	if err := r.ReadNull(); err != nil {
		tb.Fatalf("failed to read null: %s", err)
	}
}

func assertReadNumber(tb testing.TB, r *resp3.Reader, n int64) {
	tb.Helper()
	assertReadNumberFunc(tb, "integer", r.ReadNumber, n)
}

func assertReadSimpleString(tb testing.TB, r *resp3.Reader, s []byte) {
	tb.Helper()
	assertReadBytesFunc(tb, "simple string", r.ReadSimpleString, s)
}

func TestReaderIntegration(t *testing.T) {
	withRedisConn(t, func(conn io.ReadWriteCloser, rrw *resp3.ReadWriter) {
		r := &rrw.Reader

		mustWriteLines(t, conn, "*2", "$3", "GET", "$6", "string")
		assertReadNull(t, r)

		mustWriteLines(t, conn, "*3", "$3", "SET", "$6", "string", "$6", "value1")
		assertReadSimpleString(t, r, []byte("OK"))
		mustWriteLines(t, conn, "*4", "$3", "SET", "$6", "string", "$6", "value2", "$2", "NX")
		assertReadNull(t, r)
		mustWriteLines(t, conn, "*2", "$3", "GET", "$6", "string")
		assertReadBlobString(t, r, []byte("value1"))

		mustWriteLines(t, conn, "*2", "$8", "SMEMBERS", "$3", "set")
		assertReadSetHeader(t, r, 0)
		mustWriteLines(t, conn, "*3", "$4", "SADD", "$3", "set", "$6", "value3")
		assertReadNumber(t, r, 1)
		mustWriteLines(t, conn, "*3", "$4", "SADD", "$3", "set", "$6", "value3")
		assertReadNumber(t, r, 0)
		mustWriteLines(t, conn, "*2", "$8", "SMEMBERS", "$3", "set")
		assertReadSetHeader(t, r, 1)
		assertReadBlobString(t, r, []byte("value3"))

		mustWriteLines(t, conn, "*4", "$4", "ZADD", "$3", "set", "$3", "100", "$6", "value4")
		assertReadError(t, r, []byte("WRONGTYPE Operation against a key holding the wrong kind of value"))
	})
}
