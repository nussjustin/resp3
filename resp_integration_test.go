// +build integration

package resp3_test

import (
	"bytes"
	"flag"
	"io"
	"math/big"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/nussjustin/resp3"
)

const (
	defaultRedisHost = "127.0.0.1:6379"
)

func dialRedis(tb testing.TB) io.ReadWriteCloser {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = defaultRedisHost
	}

	proto := "tcp"
	if strings.HasPrefix(host, "/") {
		proto = "unix"
	}

	conn, err := net.Dial(proto, host)
	if err != nil {
		tb.Fatalf("failed to dial redis: %s", err)
	}
	tb.Cleanup(func() {
		if err := conn.Close(); err != nil {
			tb.Errorf("failed to close connection to redis: %s", err)
		}
	})

	return conn
}

var (
	discardHeaderFuncs = map[resp3.Type]func(*resp3.Reader) (int64, error){
		resp3.TypeArray: (*resp3.Reader).ReadArrayHeader,
		resp3.TypePush:  (*resp3.Reader).ReadPushHeader,
		resp3.TypeSet:   (*resp3.Reader).ReadSetHeader,
	}

	discardMapHeaderFuncs = map[resp3.Type]func(*resp3.Reader) (int64, error){
		resp3.TypeAttribute: (*resp3.Reader).ReadAttributeHeader,
		resp3.TypeMap:       (*resp3.Reader).ReadMapHeader,
	}

	discardStringFuncs = map[resp3.Type]func(*resp3.Reader, []byte) ([]byte, error){
		resp3.TypeBlobError:    (*resp3.Reader).ReadBlobError,
		resp3.TypeBlobString:   (*resp3.Reader).ReadBlobString,
		resp3.TypeSimpleError:  (*resp3.Reader).ReadSimpleError,
		resp3.TypeSimpleString: (*resp3.Reader).ReadSimpleString,
	}

	discardEmptyFuncs = map[resp3.Type]func(*resp3.Reader) error{
		resp3.TypeEnd:  (*resp3.Reader).ReadEnd,
		resp3.TypeNull: (*resp3.Reader).ReadNull,
	}
)

func discard(tb testing.TB, rr *resp3.Reader) {
	tb.Helper()

	assertNoError := func(err error) {
		if err != nil {
			tb.Fatal(err)
		}
	}

	ty, err := rr.Peek()
	assertNoError(err)

	switch {
	case discardHeaderFuncs[ty] != nil:
		n, err := discardHeaderFuncs[ty](rr)
		assertNoError(err)
		for i := int64(0); i < n; i++ {
			discard(tb, rr)
		}
	case discardMapHeaderFuncs[ty] != nil:
		n, err := discardMapHeaderFuncs[ty](rr)
		assertNoError(err)
		for i := int64(0); i < n*2; i++ {
			discard(tb, rr)
		}
	case discardStringFuncs[ty] != nil:
		_, err := discardStringFuncs[ty](rr, nil)
		assertNoError(err)
	case discardEmptyFuncs[ty] != nil:
		assertNoError(discardEmptyFuncs[ty](rr))
	default:
		switch ty {
		case resp3.TypeBigNumber:
			assertNoError(rr.ReadBigNumber(new(big.Int)))
		case resp3.TypeBoolean:
			_, err := rr.ReadBoolean()
			assertNoError(err)
		case resp3.TypeDouble:
			_, err := rr.ReadDouble()
			assertNoError(err)
		case resp3.TypeBlobChunk:
			_, _, err := rr.ReadBlobChunk(nil)
			assertNoError(err)
		case resp3.TypeNumber:
			_, err := rr.ReadNumber()
			assertNoError(err)
		}
	}
}

type debugReadWriter struct {
	io.ReadWriter
	tb testing.TB
}

var flagDebug = flag.Bool("debug", false, "enable debug logging")

func (d *debugReadWriter) format(b []byte) []byte {
	b = bytes.Replace(b, []byte("\r\n"), []byte("\\r\\n"), -1)
	b = bytes.Replace(b, []byte("\n"), []byte("\n< "), -1)
	return b
}

func (d *debugReadWriter) Read(p []byte) (n int, err error) {
	n, err = d.ReadWriter.Read(p)
	if err != nil {
		d.tb.Logf("<< ERROR: %s", err)
	} else if n > 0 {
		d.tb.Logf("< %s", d.format(p[:n]))
	}
	return n, err
}

func (d *debugReadWriter) Write(p []byte) (n int, err error) {
	n, err = d.ReadWriter.Write(p)
	if err != nil {
		d.tb.Logf(">> ERROR: %s", err)
	} else if n > 0 {
		d.tb.Logf("> %s", d.format(p[:n]))
	}
	return n, err
}

func withRedisConn(tb testing.TB, f func(io.ReadWriteCloser, *resp3.ReadWriter)) {
	conn := dialRedis(tb)
	var rw io.ReadWriter = conn
	if *flagDebug {
		rw = &debugReadWriter{ReadWriter: rw, tb: tb}
	}
	rrw := resp3.NewReadWriter(rw)

	assertError(tb, rrw.WriteArrayHeader(2), nil)
	assertError(tb, rrw.WriteBlobString([]byte("HELLO")), nil)
	assertError(tb, rrw.WriteBlobString([]byte("3")), nil)

	discard(tb, &rrw.Reader)

	assertError(tb, rrw.WriteArrayHeader(2), nil)
	assertError(tb, rrw.WriteBlobString([]byte("FLUSHDB")), nil)
	assertError(tb, rrw.WriteBlobString([]byte("ASYNC")), nil)

	res, err := rrw.ReadSimpleString(nil)
	assertError(tb, err, nil)
	assertBytes(tb, res, "OK")

	f(conn, rrw)
}
