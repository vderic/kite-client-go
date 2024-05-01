package kite

import (
	"fmt"
	//    "encoding/json"
	"github.com/vderic/kite-client-go/client"
	"github.com/vderic/kite-client-go/xrg"
)

type FileSpec interface {
	x()
}

type CsvFileSpec struct {
	Fmt        string `json:"fmt"`
	Delim      string `json:"delim"`
	Quote      string `json:"quote"`
	Escape     string `json:"escape"`
	Nullstr    string `json:"nullstr"`
	HeaderLine bool   `json:"header_line"`
}

func (s CsvFileSpec) x() {}

type ParquetFileSpec struct {
	Fmt string `json:"fmt"`
}

func (s ParquetFileSpec) x() {}

type Coldef struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Precision int    `json:"precision,omitempty"`
	Scale     int    `json:"scale,omitempty"`
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

func recv(sss []client.SockStream) <-chan any {
	res := make(chan any)
	for _, ss := range sss {
		go func(ss client.SockStream) {

			for {
				page := make([]xrg.Vector, 0)
				for {
					msg, err := ss.Recv()
					if err != nil {
						res <- err
						return
					}

					if msg.Msgty == client.KITE_MESSAGE_BYE {
						res <- nil
						return
					} else if msg.Msgty == client.KITE_MESSAGE_ERROR {
						res <- fmt.Errorf(string(msg.Buffer[0:msg.Msglen]))
						return
					} else if msg.Msgty == client.KITE_MESSAGE_VECTOR {
						if msg.Msglen == 0 {
							break
						} else {
							var vec xrg.Vector
							vec.Read(msg.Buffer)
							page = append(page, vec)
						}
					} else {
						res <- fmt.Errorf("Invalid kite message type")
						return
					}
				}

				res <- page
			}
		}(ss)
	}

	return res
}

func submit() {

}

func next() {

	sss := make([]client.SockStream, 3)

	read := recv(sss)

	for msg := range read {
		if msg == nil {
			return
		}

		switch msg.(type) {
		case xrg.Vector:
			break
		case error:
			break
		}
	}
}
