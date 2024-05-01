package kite

import (
    "fmt"
    "encoding/json"
)

type FileSpec struct {
    Fmt string `json:"fmt"`
}

type CsvFileSpec struct {
    FileSpec 
    Delim byte `json:"delim"`
    Quote byte `json:"quote"`
    Escape byte `json:"escape"`
    Nullstr byte `json:"nullstr"`
    HeaderLine bool `json:"header"`
}

type ParquetFileSpec struct {
    FileSpec
}

