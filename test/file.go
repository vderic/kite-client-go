package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/vderic/kite-client-go"
	"github.com/vderic/kite-client-go/xrg"
	"io"
	"io/ioutil"
	"os"
	//"reflect"
	"unsafe"
)

func main() {

	// read schema in JSON file
	jfile, err := os.Open("data/gpdb0.schema")
	if err != nil {
		fmt.Println(err)
		return
	}

	bv, err := ioutil.ReadAll(jfile)
	if err != nil {
		fmt.Println(err)
		return
	}

	var schema []kite.Coldef
	json.Unmarshal(bv, &schema)

	// read xrg file
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
		pint64off += 8
	}

	it := xrg.NewIterator(vec)
	for it.Next() {
		for i := 0; i < it.Nvec; i++ {
			//fmt.Println("col ", i, ", v=", it.Value[i], reflect.TypeOf(it.Value[i]).String())
			if i > 0 {
				fmt.Print(",")
			}
			if it.Flag[i] != 0 {
				fmt.Print("NULL")
			} else {
				fmt.Print(it.Value[i])
			}
		}
		fmt.Print("\n")
	}

}
