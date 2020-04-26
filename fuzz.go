// +build gofuzz

package resp3

import (
	"bytes"
	"math/big"
)

var fuzzFuncs = []func(*Reader) error{
	/* Array: */ func(rr *Reader) error { _, _, err := rr.ReadArrayHeader(); return err },
	/* Attribute: */ func(rr *Reader) error { _, _, err := rr.ReadAttributeHeader(); return err },
	/* BigNumber: */ func(rr *Reader) error { return rr.ReadBigNumber(new(big.Int)) },
	/* Boolean: */ func(rr *Reader) error { _, err := rr.ReadBoolean(); return err },
	/* Double: */ func(rr *Reader) error { _, err := rr.ReadDouble(); return err },
	/* BlobError: */ func(rr *Reader) error { _, _, err := rr.ReadBlobError(nil); return err },
	/* BlobString: */ func(rr *Reader) error { _, _, err := rr.ReadBlobString(nil); return err },
	/* BlobChunk: */ func(rr *Reader) error { _, _, err := rr.ReadBlobChunk(nil); return err },
	/* BlobChunks: */ func(rr *Reader) error { _, err := rr.ReadBlobChunks(nil); return err },
	/* End: */ func(rr *Reader) error { return rr.ReadEnd() },
	/* Map: */ func(rr *Reader) error { _, _, err := rr.ReadMapHeader(); return err },
	/* Number: */ func(rr *Reader) error { _, err := rr.ReadNumber(); return err },
	/* Null: */ func(rr *Reader) error { return rr.ReadNull() },
	/* Push: */ func(rr *Reader) error { _, _, err := rr.ReadPushHeader(); return err },
	/* Set: */ func(rr *Reader) error { _, _, err := rr.ReadSetHeader(); return err },
	/* SimpleError: */ func(rr *Reader) error { _, err := rr.ReadSimpleError(nil); return err },
	/* SimpleString: */ func(rr *Reader) error { _, err := rr.ReadSimpleString(nil); return err },
	/* VerbatimString: */ func(rr *Reader) error { _, err := rr.ReadVerbatimString(nil); return err },
}

func Fuzz(data []byte) int {
	var ret int
	for _, f := range fuzzFuncs {
		if err := f(NewReader(bytes.NewReader(data))); err == nil {
			ret = 1
		}
	}
	return ret
}
