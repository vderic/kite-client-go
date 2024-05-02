package kite

import (
	"fmt"
	"net"
	//    "encoding/json"
	"github.com/smallnest/epoller"
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

type KiteClient struct {
	request Request
	sss     []client.SockStream
	poller  *epoller.Epoll
}

func (c *KiteClient) getPage(sock client.SockStream) (p []xrg.Vector, err error) {
	page := make([]xrg.Vector, 0)
	for {
		msg, err := sock.Recv()
		if err != nil {
			return nil, err
		}

		if msg.Msgty == client.KITE_MESSAGE_BYE {
			return nil, nil
		} else if msg.Msgty == client.KITE_MESSAGE_ERROR {
			err = fmt.Errorf(string(msg.Buffer[0:msg.Msglen]))
			return nil, err
		} else if msg.Msgty == client.KITE_MESSAGE_VECTOR {
			if msg.Msglen == 0 {
				break
			} else {
				var vec xrg.Vector
				vec.Read(msg.Buffer)
				page = append(page, vec)
			}
		} else {
			err = fmt.Errorf("Invalid kite message type")
			return nil, err
		}
	}

	p = page
	return p, nil
}

func (c *KiteClient) submit() error {

	var err error = nil
	c.poller, err = epoller.NewPoller(1000000)
	if err != nil {
		return err
	}

	sss := make([]client.SockStream, 3)

	for _, ss := range sss {
		c.poller.Add(ss.Conn)
	}

	return nil
}

func (c *KiteClient) searchSockStream(conn net.Conn) *client.SockStream {
	for _, ss := range c.sss {
		if ss.Conn == conn {
			return &ss
		}
	}
	return nil
}

func (c *KiteClient) Next() error {

	conns, err := c.poller.Wait(1)
	if err != nil {
		return err
	}

	for _, connection := range conns {
		pss := c.searchSockStream(connection)
		if pss == nil {
			err = fmt.Errorf("sockstream not found.")
			return err
		}

		page, err := c.getPage(*pss)
		if err != nil {
			return err
		}

		if page == nil {
			c.poller.Remove(connection)
		}
	}

	return nil
}

func (c *KiteClient) Close() {
	c.poller.Close(false)
	for _, ss := range c.sss {
		ss.Close()
	}
}
