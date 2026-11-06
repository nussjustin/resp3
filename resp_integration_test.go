//go:build integration

package resp3_test

import (
	"bytes"
	"flag"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/nussjustin/resp3"
)

const (
	defaultRedisHost = "127.0.0.1:6379"
)

func parseRedisHost(tb testing.TB) *url.URL {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = defaultRedisHost
	}

	if !strings.Contains(host, "://") {
		return &url.URL{Scheme: "redis", Host: host}
	}

	dsn, err := url.Parse(host)
	if err != nil {
		tb.Fatalf("failed to parse REDIS_HOST: %s", err)
	}

	if dsn.Scheme != "redis" && dsn.Scheme != "valkey" {
		tb.Fatalf("unsupported scheme %q for REDIS_HOST", dsn.Scheme)
	}

	if dsn.Path != "" && dsn.Path != "/" {
		if db, err := strconv.Atoi(dsn.Path[1:]); err != nil {
			tb.Fatalf("invalid database in REDIS_HOST: %s", err)
		} else if db < 0 {
			tb.Fatal("invalid database in REDIS_HOST: database must be >= 0")
		}
	}

	return dsn
}

func dialRedis(tb testing.TB, dsn *url.URL) io.ReadWriteCloser {
	proto := "tcp"
	if strings.HasPrefix(dsn.Host, "/") {
		proto = "unix"
	}

	conn, err := net.Dial(proto, dsn.Host)
	if err != nil {
		tb.Fatalf("failed to dial server: %s", err)
	}
	tb.Cleanup(func() {
		if err := conn.Close(); err != nil {
			tb.Errorf("failed to close connection to server: %s", err)
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
	dsn := parseRedisHost(tb)

	conn := dialRedis(tb, dsn)
	var rw io.ReadWriter = conn
	if *flagDebug {
		rw = &debugReadWriter{ReadWriter: rw, tb: tb}
	}
	rrw := resp3.NewReadWriter(rw)

	username := dsn.User.Username()
	if username == "" {
		username = "default"
	}

	password, passwordSet := dsn.User.Password()

	var db int
	if dsn.Path != "" && dsn.Path != "/" {
		db, _ = strconv.Atoi(dsn.Path)
	}

	if username != "default" || passwordSet {
		assertError(tb, nil, rrw.WriteArrayHeader(5))
		assertError(tb, nil, rrw.WriteBlobString([]byte("HELLO")))
		assertError(tb, nil, rrw.WriteBlobString([]byte("3")))
		assertError(tb, nil, rrw.WriteBlobString([]byte("AUTH")))
		assertError(tb, nil, rrw.WriteBlobString([]byte(username)))
		assertError(tb, nil, rrw.WriteBlobString([]byte(password)))
	} else {
		assertError(tb, nil, rrw.WriteArrayHeader(2))
		assertError(tb, nil, rrw.WriteBlobString([]byte("HELLO")))
		assertError(tb, nil, rrw.WriteBlobString([]byte("3")))
	}

	for t, _ := rrw.Peek(); t == resp3.TypeAttribute; t, _ = rrw.Peek() {
		_, err := rrw.Discard(true)
		assertError(tb, nil, err)
	}

	t, err := rrw.Peek()
	assertError(tb, nil, err)

	if t != resp3.TypeMap {
		tb.Fatalf("unexpected response after HELLO: %s", t)
	}

	_, err = rrw.Discard(true)
	assertError(tb, nil, err)

	if db != 0 {
		assertError(tb, nil, rrw.WriteArrayHeader(2))
		assertError(tb, nil, rrw.WriteBlobString([]byte("SELECT")))
		assertError(tb, nil, rrw.WriteBlobString([]byte(strconv.Itoa(db))))

		res, err := rrw.ReadSimpleString(nil)
		assertError(tb, nil, err)
		assertBytes(tb, "OK", res)
	}

	f(conn, rrw)
}
