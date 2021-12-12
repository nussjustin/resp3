//go:build gofuzzbeta || go1.18
// +build gofuzzbeta go1.18

package resp3

import (
	"fmt"
	"math/big"
	"strings"
	"testing"
)

var inputs = []string{
	fmt.Sprint(TypeArray, "\r\n"),
	fmt.Sprint(TypeArray, "-0\r\n"),
	fmt.Sprint(TypeArray, "-1\r\n"),
	fmt.Sprint(TypeArray, "0\r\n"),
	fmt.Sprint(TypeArray, "1\r\n"),
	fmt.Sprint(TypeArray, "?\r\n"),

	fmt.Sprint(TypeAttribute, "\r\n"),
	fmt.Sprint(TypeAttribute, "-0\r\n"),
	fmt.Sprint(TypeAttribute, "-1\r\n"),
	fmt.Sprint(TypeAttribute, "0\r\n"),
	fmt.Sprint(TypeAttribute, "1\r\n"),
	fmt.Sprint(TypeAttribute, "?\r\n"),

	fmt.Sprint(TypeBigNumber, "\r\n"),
	fmt.Sprint(TypeBigNumber, "-0\r\n"),
	fmt.Sprint(TypeBigNumber, "-100\r\n"),
	fmt.Sprint(TypeBigNumber, "-1844674407370955161518446744073709551615\r\n"),
	fmt.Sprint(TypeBigNumber, "0\r\n"),
	fmt.Sprint(TypeBigNumber, "100\r\n"),
	fmt.Sprint(TypeBigNumber, "1844674407370955161518446744073709551615\r\n"),

	fmt.Sprint(TypeBlobChunk, "\r\n"),
	fmt.Sprint(TypeBlobChunk, "-0\r\n"),
	fmt.Sprint(TypeBlobChunk, "-1\r\n"),
	fmt.Sprint(TypeBlobChunk, "0\r\n"),
	fmt.Sprint(TypeBlobChunk, "1\r\n"),
	fmt.Sprint(TypeBlobChunk, "5\r\nhello\r\n"),
	fmt.Sprint(TypeBlobChunk, "?\r\n"),

	fmt.Sprint(TypeBlobError, "\r\n"),
	fmt.Sprint(TypeBlobError, "-0\r\n"),
	fmt.Sprint(TypeBlobError, "-1\r\n"),
	fmt.Sprint(TypeBlobError, "0\r\n"),
	fmt.Sprint(TypeBlobError, "1\r\n"),
	fmt.Sprint(TypeBlobError, "5\r\nhello\r\n"),
	fmt.Sprint(TypeBlobError, "?\r\n"),

	fmt.Sprint(TypeBlobString, "\r\n"),
	fmt.Sprint(TypeBlobString, "-0\r\n"),
	fmt.Sprint(TypeBlobString, "-1\r\n"),
	fmt.Sprint(TypeBlobString, "0\r\n"),
	fmt.Sprint(TypeBlobString, "1\r\n"),
	fmt.Sprint(TypeBlobString, "5\r\nhello\r\n"),
	fmt.Sprint(TypeBlobString, "?\r\n"),

	fmt.Sprint(TypeBoolean, "\r\n"),
	fmt.Sprint(TypeBoolean, "f\r\n"),
	fmt.Sprint(TypeBoolean, "t\r\n"),
	fmt.Sprint(TypeBoolean, "x\r\n"),

	fmt.Sprint(TypeDouble, "\r\n"),
	fmt.Sprint(TypeDouble, "+inf\r\n"),
	fmt.Sprint(TypeDouble, "-0\r\n"),
	fmt.Sprint(TypeDouble, "-100\r\n"),
	fmt.Sprint(TypeDouble, "-inf\r\n"),
	fmt.Sprint(TypeDouble, ".\r\n"),
	fmt.Sprint(TypeDouble, "..\r\n"),
	fmt.Sprint(TypeDouble, ".0\r\n"),
	fmt.Sprint(TypeDouble, ".100\r\n"),
	fmt.Sprint(TypeDouble, "0\r\n"),
	fmt.Sprint(TypeDouble, "0.\r\n"),
	fmt.Sprint(TypeDouble, "0.0\r\n"),
	fmt.Sprint(TypeDouble, "100\r\n"),
	fmt.Sprint(TypeDouble, "100.\r\n"),
	fmt.Sprint(TypeDouble, "100.100\r\n"),

	fmt.Sprint(TypeEnd, "\r\n"),

	fmt.Sprint(TypeMap, "\r\n"),
	fmt.Sprint(TypeMap, "-0\r\n"),
	fmt.Sprint(TypeMap, "-1\r\n"),
	fmt.Sprint(TypeMap, "0\r\n"),
	fmt.Sprint(TypeMap, "1\r\n"),
	fmt.Sprint(TypeMap, "?\r\n"),

	fmt.Sprint(TypeNull, "\r\n"),

	fmt.Sprint(TypeNumber, "\r\n"),
	fmt.Sprint(TypeNumber, "+184467440737095516150\r\n"),
	fmt.Sprint(TypeNumber, "-0\r\n"),
	fmt.Sprint(TypeNumber, "-100\r\n"),
	fmt.Sprint(TypeNumber, "-184467440737095516150\r\n"),
	fmt.Sprint(TypeNumber, "0\r\n"),
	fmt.Sprint(TypeNumber, "100\r\n"),

	fmt.Sprint(TypePush, "\r\n"),
	fmt.Sprint(TypePush, "-0\r\n"),
	fmt.Sprint(TypePush, "-1\r\n"),
	fmt.Sprint(TypePush, "0\r\n"),
	fmt.Sprint(TypePush, "1\r\n"),
	fmt.Sprint(TypePush, "?\r\n"),

	fmt.Sprint(TypeSet, "\r\n"),
	fmt.Sprint(TypeSet, "-0\r\n"),
	fmt.Sprint(TypeSet, "-1\r\n"),
	fmt.Sprint(TypeSet, "0\r\n"),
	fmt.Sprint(TypeSet, "1\r\n"),
	fmt.Sprint(TypeSet, "?\r\n"),

	fmt.Sprint(TypeSimpleError, "\r\n"),
	fmt.Sprint(TypeSimpleError, "hello\r\n"),
	fmt.Sprint(TypeSimpleError, "hello\nworld\r\n"),

	fmt.Sprint(TypeSimpleString, "\r\n"),
	fmt.Sprint(TypeSimpleString, "hello\r\n"),
	fmt.Sprint(TypeSimpleString, "hello\nworld\r\n"),

	fmt.Sprint(TypeVerbatimString, "\r\n"),
	fmt.Sprint(TypeVerbatimString, "-0\r\n"),
	fmt.Sprint(TypeVerbatimString, "-1\r\n"),
	fmt.Sprint(TypeVerbatimString, "0\r\n"),
	fmt.Sprint(TypeVerbatimString, "1\r\n"),
	fmt.Sprint(TypeVerbatimString, "5\r\n:foo!\r\n"),
	fmt.Sprint(TypeVerbatimString, "5\r\nf:oo!\r\n"),
	fmt.Sprint(TypeVerbatimString, "5\r\nfo:o!\r\n"),
	fmt.Sprint(TypeVerbatimString, "5\r\nfoo!:\r\n"),
	fmt.Sprint(TypeVerbatimString, "5\r\nfoo:\r\n"),
	fmt.Sprint(TypeVerbatimString, "5\r\nfoo:!\r\n"),
	fmt.Sprint(TypeVerbatimString, "5\r\nhello\r\n"),
	fmt.Sprint(TypeVerbatimString, "?\r\n"),
}

func FuzzReader(f *testing.F) {
	for _, input := range inputs {
		f.Add(input)
	}

	f.Fuzz(func(t *testing.T, data string) {
		rr := NewReader(strings.NewReader(data))

		var n big.Int
		var b []byte

		for {
			ty, err := rr.Peek()
			if err != nil {
				return
			}

			switch ty {
			case TypeArray:
				_, _, err = rr.ReadArrayHeader()
			case TypeAttribute:
				_, _, err = rr.ReadAttributeHeader()
			case TypeBlobError:
				b, _, err = rr.ReadBlobError(b[:0])
			case TypeBlobString:
				b, _, err = rr.ReadBlobString(b[:0])
			case TypeBlobChunk:
				b, _, err = rr.ReadBlobChunk(b[:0])
			case TypeBigNumber:
				err = rr.ReadBigNumber(&n)
			case TypeBoolean:
				_, err = rr.ReadBoolean()
			case TypeDouble:
				_, err = rr.ReadDouble()
			case TypeEnd:
				err = rr.ReadEnd()
			case TypeMap:
				_, _, err = rr.ReadMapHeader()
			case TypeNumber:
				_, err = rr.ReadNumber()
			case TypeNull:
				err = rr.ReadNull()
			case TypePush:
				_, _, err = rr.ReadPushHeader()
			case TypeSet:
				_, _, err = rr.ReadSetHeader()
			case TypeSimpleError:
				b, err = rr.ReadSimpleError(b[:0])
			case TypeSimpleString:
				b, err = rr.ReadSimpleString(b[:0])
			case TypeVerbatimString:
				b, err = rr.ReadVerbatimString(b[:0])
			default:
				t.Fatalf("unknown type %q", ty.String())
			}

			if err != nil {
				return
			}
		}
	})
}

func FuzzReader_Discard(f *testing.F) {
	for _, input := range inputs {
		f.Add(input)
	}

	f.Fuzz(func(t *testing.T, data string) {
		_, _ = NewReader(strings.NewReader(data)).Discard(true)
	})
}
