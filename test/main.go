package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
    "encoding/json"
	"fmt"
	"github.com/vderic/kite-client-go"
	"github.com/vderic/kite-client-go/xrg"
	"github.com/vderic/kite-client-go/client"
	"io"
	"os"
	"reflect"
	"unsafe"
)

func main() {

    spec := kite.ParquetFileSpec{kite.FileSpec{"parquet"}}

    js, err := json.Marshal(spec)

    fmt.Println(string(js))

	ptyp := xrg.XRG_PTYP_INT8
	ltyp := xrg.XRG_LTYP_ARRAY

	typ := xrg.XRG_LTYP_PTYP(ltyp, ptyp)

	fmt.Println("typ = ", typ)

    fmt.Println(client.KITE_MESSAGE_KIT1)

	file, err := os.Open("data/gpdb0_0.xrg")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return
	}

	bs := make([]byte, stat.Size())
	_, err = bufio.NewReader(file).Read(bs)
	if err != nil && err != io.EOF {
		fmt.Println(err)
		return
	}

	var footer xrg.VectorFooter
	offset := len(bs) - 8
	off := bytes.NewReader(bs[offset:])
	err = binary.Read(off, binary.LittleEndian, &footer)
	if err != nil {
		fmt.Println("binary.Read failed:", err)
	}

	//fmt.Println("Footer Nvec", footer.Nvec)

	off2 := len(bs) - 8 - int(footer.Nvec*8)
	pint64off := uintptr(unsafe.Pointer(&bs[off2]))

	nvec := int(footer.Nvec)
	vec := make([]xrg.Vector, nvec)

	for i := 0; i < nvec && i < int(footer.Nvec); i++ {
		int64off := *(*int64)(unsafe.Pointer(pint64off))
		offset = int(int64off)
		//fmt.Println(int64off)

		err = vec[i].Read(bs[offset:])
		if err != nil {
			fmt.Println("binary.Read failed:", err)
			return
		}
		/*
			fmt.Println(string(v.Header.Magic[:]))
			fmt.Println("Itemsz: ", v.Header.Itemsz)
			fmt.Println("Nbyte: ", v.Header.Nbyte)
			fmt.Println("Zbyte: ", v.Header.Zbyte)
			fmt.Println("Ptyp: ", v.Header.Ptyp)
			fmt.Println("Ltyp: ", v.Header.Ltyp)
		*/

		pint64off += 8
	}

	var it xrg.Iterator

	err = it.Create(vec)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("iterator start")
	for it.Next() {
		fmt.Println("next")
		for i := 0; i < it.Nvec; i++ {
			fmt.Println("col ", i, ", v=", it.Value[i], reflect.TypeOf(it.Value[i]).String())
		}
	}

}
