# resp3
> Fast RESP 3 protocol reader and writer for Go.

[![GoDoc](https://godoc.org/github.com/nussjustin/resp3?status.svg)](https://godoc.org/github.com/nussjustin/resp3)
[![Lint](https://github.com/nussjustin/resp3/workflows/Lint/badge.svg)](https://github.com/nussjustin/resp3/actions?query=workflow%3ALint)
[![Test](https://github.com/nussjustin/resp3/workflows/Test/badge.svg)](https://github.com/nussjustin/resp3/actions?query=workflow%3ATest)
[![Go Report Card](https://goreportcard.com/badge/github.com/nussjustin/resp3)](https://goreportcard.com/report/github.com/nussjustin/resp3)

This is a small package that provides fast reader and writer types for version 3 of the
[REdis Serialization Protocol](https://redis.io/topics/protocol) (short RESP).

## Installation

```sh
go get -u github.com/nussjustin/resp3
```

## Testing

To run all unit tests, just call `go test`:

```sh
go test
```

If you want to run integration tests you need to pass the `integration` tag to `go test`:

```sh
go test -tags integration
```

By default integration tests will try to connect to a Redis instance on `127.0.0.1:6379`.

If your instance has a non-default config, you can use the `REDIS_HOST` environment variable, to override the address:

```sh
REDIS_HOST=127.0.0.1:6380   go test -tags integration # different port
REDIS_HOST=192.168.0.1:6380 go test -tags integration # different host
REDIS_HOST=/tmp/redis.sock  go test -tags integration # unix socket
```

Note: If you want to test using a unix socket, make sure that the path to the socket starts with a slash,
for example `/tmp/redis.sock`.

Debug logging for integration tests can be enabled by passing the `-debug` flag to `go test`.

## Meta

Justin Nuß – [@nussjustin](https://twitter.com/nussjustin)

Distributed under the MIT license. See ``LICENSE`` for more information.

## Contributing

1. Fork it (<https://github.com/nussjustin/resp3/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request
