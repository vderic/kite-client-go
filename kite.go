package kite

import (
    "fmt"
    //"encoding/json"
    "github.com/vderic/kite-client-go/xrg"
)

type FileSpec struct {
    Fmt string `json:"fmt"`
}

type CsvFileSpec struct {
    FileSpec 
    Delim string `json:"delim"`
    Quote string `json:"quote"`
    Escape string `json:"escape"`
    Nullstr string `json:"nullstr"`
    HeaderLine bool `json:"header_line"`
}

type ParquetFileSpec struct {
    FileSpec
}

func test() {
    fmt.Println(xrg.XRG_PTYP_INT8)
}
