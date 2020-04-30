// +build gofuzz

package test

import "github.com/nussjustin/resp3/internal/fuzz"

func Fuzz(data []byte) int {
	return fuzz.Reader(data)
}
