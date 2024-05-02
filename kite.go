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
	sss     map[uintptr]client.SockStream
	poller  *epoller.Epoll
	pages   []xrg.Iterator
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

func getFd(conn net.Conn) (uintptr, error) {
	file, err := conn.(*net.TCPConn).File()
	if err != nil {
		return 0, err
	}
	fd := file.Fd()
	return fd, nil
}

func (c *KiteClient) submit() error {

	var err error = nil
	c.poller, err = epoller.NewPoller(1000000)
	if err != nil {
		return err
	}

	var conn net.Conn
	fd, err := getFd(conn)
	if err != nil {
		return err
	}

	c.sss[fd] = client.SockStream{conn}

	for _, ss := range c.sss {
		c.poller.Add(ss.Conn)
	}

	return nil
}

func (c *KiteClient) searchSockStream(conn net.Conn) *client.SockStream {
	fd, err := getFd(conn)
	if err != nil {
		return nil
	}

	if s, ok := c.sss[fd]; ok {
		return &s
	}
	return nil
}

func (c *KiteClient) nextPage() (it *xrg.Iterator, err error) {

	var x xrg.Iterator
	if len(c.pages) != 0 {
		x, c.pages = c.pages[0], c.pages[1:]
		return &x, nil
	}

	for {
		if len(c.sss) == 0 {
			break
		}

		conns, err := c.poller.Wait(1)
		if err != nil {
			return it, err
		}

		for _, connection := range conns {
			pss := c.searchSockStream(connection)
			if pss == nil {
				err = fmt.Errorf("sockstream not found.")
				return it, err
			}

			page, err := c.getPage(*pss)
			if err != nil {
				/*
					            if err == io.EOF || errors.Is(err, net.ErrClosed) {
					                c.poller.Remove(sock.Conn)
					            } else {
								    return err
					            }
				*/
				return it, err
			}

			if page == nil {
				c.poller.Remove(connection)
				(*pss).Close()
				fd, _ := getFd(connection)
				delete(c.sss, fd)
			}

			// TODO: push to the list
			var iter xrg.Iterator
			err = iter.Create(page)
			if err != nil {
				return it, err
			}
			c.pages = append(c.pages, iter)
		}
	}

	// pop
	if len(c.pages) == 0 {
		return nil, nil
	}

	x, c.pages = c.pages[0], c.pages[1:]
	return &x, nil
}

func (c *KiteClient) Close() {
	c.poller.Close(false)
	for _, ss := range c.sss {
		ss.Close()
	}
}
