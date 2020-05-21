// +build integration

package resp3_test

import (
	"bytes"
	"flag"
	"io"
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

	assertError(tb, nil, rrw.WriteArrayHeader(2))
	assertError(tb, nil, rrw.WriteBlobString([]byte("HELLO")))
	assertError(tb, nil, rrw.WriteBlobString([]byte("3")))

	_, err := rrw.Discard(true)
	assertError(tb, nil, err)

	assertError(tb, nil, rrw.WriteArrayHeader(2))
	assertError(tb, nil, rrw.WriteBlobString([]byte("FLUSHDB")))
	assertError(tb, nil, rrw.WriteBlobString([]byte("ASYNC")))

	res, err := rrw.ReadSimpleString(nil)
	assertError(tb, nil, err)
	assertBytes(tb, "OK", res)

	f(conn, rrw)
}
