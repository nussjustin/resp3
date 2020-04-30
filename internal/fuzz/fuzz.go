package fuzz

import (
	"bytes"
	"math/big"

	"github.com/nussjustin/resp3"
)

var ReaderFuncs = []struct {
	Name string
	Func func(*resp3.Reader) error
}{
	{Name: "Array", Func: func(rr *resp3.Reader) error { _, _, err := rr.ReadArrayHeader(); return err }},
	{Name: "Attribute", Func: func(rr *resp3.Reader) error { _, _, err := rr.ReadAttributeHeader(); return err }},
	{Name: "BigNumber", Func: func(rr *resp3.Reader) error { return rr.ReadBigNumber(new(big.Int)) }},
	{Name: "Boolean", Func: func(rr *resp3.Reader) error { _, err := rr.ReadBoolean(); return err }},
	{Name: "Double", Func: func(rr *resp3.Reader) error { _, err := rr.ReadDouble(); return err }},
	{Name: "BlobError", Func: func(rr *resp3.Reader) error { _, _, err := rr.ReadBlobError(nil); return err }},
	{Name: "BlobString", Func: func(rr *resp3.Reader) error { _, _, err := rr.ReadBlobString(nil); return err }},
	{Name: "BlobChunk", Func: func(rr *resp3.Reader) error { _, _, err := rr.ReadBlobChunk(nil); return err }},
	{Name: "BlobChunks", Func: func(rr *resp3.Reader) error { _, err := rr.ReadBlobChunks(nil); return err }},
	{Name: "End", Func: func(rr *resp3.Reader) error { return rr.ReadEnd() }},
	{Name: "Map", Func: func(rr *resp3.Reader) error { _, _, err := rr.ReadMapHeader(); return err }},
	{Name: "Number", Func: func(rr *resp3.Reader) error { _, err := rr.ReadNumber(); return err }},
	{Name: "Null", Func: func(rr *resp3.Reader) error { return rr.ReadNull() }},
	{Name: "Push", Func: func(rr *resp3.Reader) error { _, _, err := rr.ReadPushHeader(); return err }},
	{Name: "Set", Func: func(rr *resp3.Reader) error { _, _, err := rr.ReadSetHeader(); return err }},
	{Name: "SimpleError", Func: func(rr *resp3.Reader) error { _, err := rr.ReadSimpleError(nil); return err }},
	{Name: "SimpleString", Func: func(rr *resp3.Reader) error { _, err := rr.ReadSimpleString(nil); return err }},
	{Name: "VerbatimString", Func: func(rr *resp3.Reader) error { _, err := rr.ReadVerbatimString(nil); return err }},
}

func Reader(data []byte) int {
	var ret int
	for _, f := range ReaderFuncs {
		if err := f.Func(resp3.NewReader(bytes.NewReader(data))); err == nil {
			ret = 1
		}
	}
	return ret
}
