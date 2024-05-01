package kite

import (
	"fmt"
	//    "encoding/json"
	"github.com/vderic/kite-client-go/client"
	"github.com/vderic/kite-client-go/xrg"
)

type FileSpec interface{}

type CsvFileSpec struct {
	Fmt        string `json:"fmt"`
	Delim      string `json:"delim"`
	Quote      string `json:"quote"`
	Escape     string `json:"escape"`
	Nullstr    string `json:"nullstr"`
	HeaderLine bool   `json:"header_line"`
}

type ParquetFileSpec struct {
	Fmt string `json:"fmt"`
}

type Coldef struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Precision int    `json:"precision"`
	Scale     int    `json:"scale"`
}

type Schema struct {
	columns []Coldef `json:"schema"`
}

type Request struct {
	Schema   []Coldef `json:"schema"`
	Sql      string   `json:"sql"`
	Fragment [2]int   `json:"fragment"`
	Spec     FileSpec `json:"filespec"`
}

func test() {
	fmt.Println(xrg.XRG_PTYP_INT8)
	fmt.Println(client.KITE_MESSAGE_KIT1)
}
