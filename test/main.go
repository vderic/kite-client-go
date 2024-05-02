package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/vderic/kite-client-go"
	"github.com/vderic/kite-client-go/client"
	"github.com/vderic/kite-client-go/xrg"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"unsafe"
)

func main() {

	spec := kite.ParquetFileSpec{"parquet"}
	//spec := kite.CsvFileSpec{"csv", ",", "\"", "\"", "", false}

	//schema := []kite.Coldef{{Name: "col1", Type: "int8"}, {Name: "col2", Type: "fp32", Precision: 1, Scale: 2}}

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

	sql := "select * from table"

	cli := new(kite.KiteClient)
	cli.Schema(schema).Sql(sql).Fragment(0, 3).FileSpec(spec)

	fragment := [2]int{0, 4}
	request := kite.Request{schema, sql, fragment, spec}

	js, err := json.Marshal(request)

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

	it := xrg.NewIterator(vec)
	fmt.Println("iterator start")
	for it.Next() {
		fmt.Println("next")
		for i := 0; i < it.Nvec; i++ {
			fmt.Println("col ", i, ", v=", it.Value[i], reflect.TypeOf(it.Value[i]).String())
		}
	}

}
