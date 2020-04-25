// Package resp3 implements fast Reader and Writer types for parsing the Redis RESP3 protocol.
//
// This package is low level and only deals with parsing of the different types of the RESP protocol.
//
// All structs can be reused via the corresponding Reset method and duplex connections are supported using a ReadWriter
// type that wraps a Reader and a Writer in a single allocation.
//
// Methods that take []byte to write (e.g. WriteSimpleString) are optimized to allow the compiler to avoid allocations
// when passing a string converted to a []byte as parameter (e.g. WriteSimpleString([]byte("OK")) should not allocate).
package resp3
