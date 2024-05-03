package xrg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pierrec/lz4"
	"unsafe"
)

const XRG_HEADER_SIZE = 48

var XRG_MAGIC = []byte("XRG1")

const XRG_ARRAY_HEADER_SIZE = 16

func Align(alignment int32, value int32) uintptr {
	return uintptr((value + alignment - 1) & ^(alignment - 1))
}

const XRG_SHIFT_NULL = 0
const XRG_SHIFT_INVAL = 1
const XRG_SHIFT_EXCEPT = 2

const XRG_FLAG_NULL = (1 << XRG_SHIFT_NULL)     // is nul
const XRG_FLAG_INVAL = (1 << XRG_SHIFT_INVAL)   // invalid
const XRG_FLAG_EXCEPT = (1 << XRG_SHIFT_EXCEPT) // exception

type PhysicalType int16

const (
	XRG_PTYP_UNKNOWN PhysicalType = 0
	XRG_PTYP_INT8    PhysicalType = 1
	XRG_PTYP_INT16   PhysicalType = 2
	XRG_PTYP_INT32   PhysicalType = 3
	XRG_PTYP_INT64   PhysicalType = 4
	XRG_PTYP_INT128  PhysicalType = 5
	XRG_PTYP_FP32    PhysicalType = 6
	XRG_PTYP_FP64    PhysicalType = 7
	XRG_PTYP_BYTEA   PhysicalType = 8
	XRG_PTYP_MAX     PhysicalType = 8
)

type LogicalType int16

const (
	XRG_LTYP_UNKNOWN   LogicalType = 0
	XRG_LTYP_NONE      LogicalType = 1
	XRG_LTYP_STRING    LogicalType = 2
	XRG_LTYP_DECIMAL   LogicalType = 3
	XRG_LTYP_INTERVAL  LogicalType = 4
	XRG_LTYP_TIME      LogicalType = 5
	XRG_LTYP_DATE      LogicalType = 6
	XRG_LTYP_TIMESTAMP LogicalType = 7
	XRG_LTYP_ARRAY     LogicalType = 8
	XRG_LTYP_MAX       LogicalType = 8
)

var XRG_TYPES = []string{"int8", "int16", "int32", "int64", "float", "double", "decimal",
	"string", "interval", "time", "date", "timestamp",
	"int8[]", "int16[]", "int32[]", "int64[]", "float[]", "double[]", "decimal[]",
	"string[]", "interval[]", "time[]", "date[]", "timestamp[]"}

func ValidateType(typ string) bool {
	for _, s := range XRG_TYPES {
		if typ == s {
			return true
		}
	}
	return false
}

func XRG_LTYP_PTYP(ltyp LogicalType, ptyp PhysicalType) int32 {
	return (int32(ltyp) << 16) | int32(ptyp)
}

func ByteArrayPtr(ptr uintptr) uintptr {
	return ptr + 4
}

func ByteArrayLen(ptr uintptr) uintptr {
	return uintptr(*(*int32)(unsafe.Pointer(ptr)))
}

type Interval struct {
	Usec int64
	Day  int32
	Mon  int32
}

type VectorHeader struct {
	Magic     [4]byte
	Ptyp      PhysicalType
	Ltyp      LogicalType
	Fieldidx  int16
	Itemsz    int16
	Scale     int16
	Precision int16
	Nbyte     int32
	Zbyte     int32
	Nnull     int32
	Nitem     int32
	Unused1   int32
	Unused2   int64
}

func (hdr *VectorHeader) Read(b []byte) error {
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.LittleEndian, hdr)
	if err != nil {
		fmt.Println("binary.Read failed:", err)
		return err
	}
	return err
}

type ArrayHeader struct {
	Len        int32        /* length of the buffer */
	Ndim       int32        /* # of dimensions same as postgres. value is always 1 */
	Dataoffset int32        /* offset of data, or 0 if no bitmap */
	Ptyp       PhysicalType /* physical type of the array data */
	Ltyp       LogicalType  /* logical type of the array data */
}

func (hdr *ArrayHeader) Read(b []byte) error {
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.LittleEndian, hdr)
	if err != nil {
		fmt.Println("binary.Read failed:", err)
		return err
	}
	return err
}

func (hdr *ArrayHeader) Print() {
	fmt.Println("Array Len:", hdr.Len)
	fmt.Println("Array Ndim:", hdr.Ndim)
	fmt.Println("Array Offset:", hdr.Dataoffset)
	fmt.Println("Array Ptyp:", hdr.Ptyp)
	fmt.Println("Array Ltyp:", hdr.Ltyp)

}

type ArrayType struct {
	Header    ArrayHeader
	Precision int16
	Scale     int16
	Dims      []int32
	Lbs       []int32
	Bitmap    []byte
	Values    []any
}

func NewArrayType(dataptr uintptr, precision int16, scale int16) (ArrayType, error) {
	var a ArrayType
	err := a.Read(dataptr, precision, scale)
	if err != nil {
		return a, err
	}
	return a, nil
}

func (arr *ArrayType) Read(dataptr uintptr, precision int16, scale int16) error {

	arr.Precision = precision
	arr.Scale = scale

	hdr := unsafe.Slice((*byte)(unsafe.Pointer(dataptr)), XRG_ARRAY_HEADER_SIZE)
	err := arr.Header.Read(hdr)
	if err != nil {
		return err
	}

	arr.Dims = nil
	arr.Lbs = nil
	arr.Values = nil
	arr.Bitmap = nil

	ndim := arr.Header.Ndim
	if ndim == 0 {
		arr.Values = make([]any, 0)
		return nil
	}

	if ndim != 1 {
		err := fmt.Errorf("array is not 1-D")
		return err
	}

	ptr := dataptr
	ptr += XRG_ARRAY_HEADER_SIZE

	arr.Dims = unsafe.Slice((*int32)(unsafe.Pointer(ptr)), ndim)
	ptr += uintptr(ndim * 4)
	arr.Lbs = unsafe.Slice((*int32)(unsafe.Pointer(ptr)), ndim)
	ptr += uintptr(ndim * 4)

	nitems := arr.GetNitem(ndim, arr.Dims)
	dataoffset := arr.Header.Dataoffset

	hdrsz := uintptr(0)
	if dataoffset == 0 {
		hdrsz = arr.GetOverHeadNoNulls(ndim)
	} else {
		bitmapsz := uintptr((nitems + 7) / 8)

		arr.Bitmap = unsafe.Slice((*byte)(unsafe.Pointer(ptr)), bitmapsz)
		hdrsz = arr.GetOverHeadWithNulls(ndim, nitems)
	}

	ptr = dataptr + hdrsz
	arr.Values, err = arr.PointerGetArray(ptr, nitems)
	if err != nil {
		return err
	}

	return nil
}

func (arr *ArrayType) GetNitem(ndim int32, dims []int32) int32 {
	if ndim == 0 {
		return 0
	}
	return dims[0]
}

func (arr *ArrayType) GetOverHeadNoNulls(ndim int32) uintptr {
	return Align(8, XRG_ARRAY_HEADER_SIZE+2*4*ndim)
}

func (arr *ArrayType) GetOverHeadWithNulls(ndim int32, nitems int32) uintptr {
	return Align(8, XRG_ARRAY_HEADER_SIZE+2*4*ndim+((nitems+7)/8))
}

func (arr *ArrayType) IsNull(offset int32) bool {
	if arr.Bitmap == nil {
		return false
	}

	if arr.Bitmap[offset/8]&(1<<(offset%8)) != 0 {
		return false
	}
	return true
}

func (arr *ArrayType) PointerGetArray(ptr uintptr, nitems int32) ([]any, error) {
	var err error = nil
	values := make([]any, 0)
	for i := int32(0); i < nitems; i++ {
		if arr.IsNull(i) {
			values = append(values, nil)
		} else {
			var v any
			switch arr.Header.Ptyp {
			case XRG_PTYP_INT8:
				v, err = PointerGetValue(ptr, arr.Header.Ptyp, arr.Header.Ltyp, int16(1), arr.Precision, arr.Scale)
				ptr += 1
				break
			case XRG_PTYP_INT16:
				v, err = PointerGetValue(ptr, arr.Header.Ptyp, arr.Header.Ltyp, int16(2), arr.Precision, arr.Scale)
				ptr += 2
				break
			case XRG_PTYP_INT32:
				v, err = PointerGetValue(ptr, arr.Header.Ptyp, arr.Header.Ltyp, int16(4), arr.Precision, arr.Scale)
				ptr += 4
				break
			case XRG_PTYP_INT64:
				v, err = PointerGetValue(ptr, arr.Header.Ptyp, arr.Header.Ltyp, int16(8), arr.Precision, arr.Scale)
				ptr += 8
				break
			case XRG_PTYP_INT128:
				v, err = PointerGetValue(ptr, arr.Header.Ptyp, arr.Header.Ltyp, int16(16), arr.Precision, arr.Scale)
				ptr += 16
				break
			case XRG_PTYP_FP32:
				v, err = PointerGetValue(ptr, arr.Header.Ptyp, arr.Header.Ltyp, int16(4), arr.Precision, arr.Scale)
				ptr += 4
				break
			case XRG_PTYP_FP64:
				v, err = PointerGetValue(ptr, arr.Header.Ptyp, arr.Header.Ltyp, int16(8), arr.Precision, arr.Scale)
				ptr += 8
				break
			case XRG_PTYP_BYTEA:
				v, err = PointerGetValue(ptr, arr.Header.Ptyp, arr.Header.Ltyp, int16(-1), arr.Precision, arr.Scale)
				ptr += 4 + ByteArrayLen(ptr)
				break
			default:
				err = fmt.Errorf("array element type not supported. ", arr.Header.Ptyp)
				return nil, err
			}

			if err != nil {
				return nil, err
			}

			values = append(values, v)
		}
	}

	return values, err
}

type VectorFooter struct {
	Nvec  int32
	Magic [4]byte
}

type Vector struct {
	Header VectorHeader
	Data   []byte
	Flag   []byte
}

func NewVector(b []byte) (Vector, error) {
	var v Vector
	err := v.Read(b)
	if err != nil {
		return v, err
	}
	return v, nil
}

func (v *Vector) Read(b []byte) error {
	err := v.Header.Read(b[0:XRG_HEADER_SIZE])
	if err != nil {
		return err
	}
	if v.Header.Nbyte != v.Header.Zbyte {
		v.Data = make([]byte, v.Header.Nbyte)
		retsz, err := lz4.UncompressBlock(b[XRG_HEADER_SIZE:XRG_HEADER_SIZE+v.Header.Zbyte], v.Data)
		if err != nil {
			fmt.Println(err)
			return err
		}

		if retsz != int(v.Header.Nbyte) {
			return fmt.Errorf("lz4 uncompress return size != Nbyte ", v.Header.Nbyte)
		}
	} else {
		v.Data = b[XRG_HEADER_SIZE : XRG_HEADER_SIZE+v.Header.Nbyte]
	}
	v.Flag = b[XRG_HEADER_SIZE+v.Header.Zbyte : XRG_HEADER_SIZE+v.Header.Zbyte+v.Header.Nitem]
	return nil
}

type Iterator struct {
	Nvec         int
	Vec          []Vector
	ValuePtr     []uintptr
	NextValuePtr []uintptr
	Value        []any
	Flag         []byte
	Header       []VectorHeader
	Valuesz      []int16
	Nitem        int32
	curr         int64
}

func NewIterator(vec []Vector) Iterator {
	var iter Iterator
	iter.Nvec = len(vec)
	iter.Vec = vec
	iter.curr = -1
	iter.Header = make([]VectorHeader, iter.Nvec)
	iter.ValuePtr = make([]uintptr, iter.Nvec)
	iter.NextValuePtr = make([]uintptr, iter.Nvec)
	iter.Value = make([]any, iter.Nvec)
	iter.Flag = make([]byte, iter.Nvec)
	iter.Valuesz = make([]int16, iter.Nvec)

	for i := 0; i < iter.Nvec; i++ {
		iter.Header[i] = vec[i].Header
		iter.ValuePtr[i] = uintptr(unsafe.Pointer(&vec[i].Data[0]))
		iter.Flag[i] = 0
		iter.NextValuePtr[i] = 0
		iter.Valuesz[i] = vec[i].Header.Itemsz
	}
	iter.Nitem = iter.Header[0].Nitem

	return iter
}

func PointerGetValue(ptr uintptr, ptyp PhysicalType, ltyp LogicalType, itemsz int16, precision int16, scale int16) (any, error) {
	var err error = nil
	if itemsz > 0 {
		switch ptyp {
		case XRG_PTYP_INT8:
			i8 := *(*byte)(unsafe.Pointer(ptr))
			return i8, err
		case XRG_PTYP_INT16:
			i16 := *(*int16)(unsafe.Pointer(ptr))
			return i16, err
		case XRG_PTYP_INT32:
			i32 := *(*int32)(unsafe.Pointer(ptr))
			return i32, err
		case XRG_PTYP_INT64:
			i64 := *(*int64)(unsafe.Pointer(ptr))
			return i64, err
		case XRG_PTYP_INT128:
			if ltyp == XRG_LTYP_INTERVAL {
				interval := *(*Interval)(unsafe.Pointer(ptr))
				return interval, err
			} else {
				p := (*int64)(unsafe.Pointer(ptr))
				i128 := unsafe.Slice(p, 2)
				return i128, err
			}
		case XRG_PTYP_FP32:
			fp32 := *(*float32)(unsafe.Pointer(ptr))
			return fp32, err
		case XRG_PTYP_FP64:
			fp64 := *(*float64)(unsafe.Pointer(ptr))
			return fp64, err
		default:
			err = fmt.Errorf("unknown type", ptyp)
			break
		}
	} else {
		dataptr := ByteArrayPtr(ptr)
		sz := ByteArrayLen(ptr)
		if ltyp == XRG_LTYP_STRING {
			s := string(unsafe.Slice((*byte)(unsafe.Pointer(dataptr)), sz))
			return s, err
		} else {
			arr, err := NewArrayType(dataptr, precision, scale)
			if err != nil {
				return nil, err
			}
			return arr, err
		}
	}

	return nil, err
}

func (iter *Iterator) Next() bool {
	var err error = nil
	inval := byte(1)
	for inval != 0 {
		curr := iter.curr + 1
		if curr >= int64(iter.Nitem) {
			return false
		}

		iter.curr = curr
		inval = 0

		if 0 == curr {
			for i := 0; i < iter.Nvec; i++ {
				iter.ValuePtr[i] = uintptr(unsafe.Pointer(&iter.Vec[i].Data[0]))
				iter.NextValuePtr[i] = uintptr(unsafe.Pointer(&iter.Vec[i].Data[0]))
				iter.Value[i], err = PointerGetValue(iter.ValuePtr[i], iter.Header[i].Ptyp, iter.Header[i].Ltyp, iter.Header[i].Itemsz, iter.Header[i].Precision, iter.Header[i].Scale)
				if err != nil {
					fmt.Println(err)
					return false
				}
				iter.Flag[i] = iter.Vec[i].Flag[curr]
				inval |= iter.Flag[i] & XRG_FLAG_INVAL

				itemsz := iter.Header[i].Itemsz
				if itemsz > 0 {
					iter.NextValuePtr[i] = iter.NextValuePtr[i] + uintptr(itemsz)
				} else {
					iter.NextValuePtr[i] = ByteArrayPtr(iter.NextValuePtr[i]) + ByteArrayLen(iter.NextValuePtr[i])
				}
			}
		} else {
			for i := 0; i < iter.Nvec; i++ {
				// advance the flag
				iter.Flag[i] = iter.Vec[i].Flag[curr]
				inval |= iter.Flag[i] & XRG_FLAG_INVAL

				iter.ValuePtr[i] = iter.NextValuePtr[i]
				iter.Value[i], err = PointerGetValue(iter.ValuePtr[i], iter.Header[i].Ptyp, iter.Header[i].Ltyp, iter.Header[i].Itemsz, iter.Header[i].Precision, iter.Header[i].Scale)
				if err != nil {
					fmt.Println(err)
					return false
				}

				// advance the value
				itemsz := iter.Header[i].Itemsz
				if itemsz > 0 {
					iter.NextValuePtr[i] = iter.NextValuePtr[i] + uintptr(itemsz)
				} else {
					iter.NextValuePtr[i] = ByteArrayPtr(iter.NextValuePtr[i]) + ByteArrayLen(iter.NextValuePtr[i])
				}
			}
		}

	}
	return true
}
