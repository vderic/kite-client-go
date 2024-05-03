package kite

import (
	"encoding/json"
	"fmt"
	"github.com/smallnest/epoller"
	"github.com/vderic/kite-client-go/client"
	"github.com/vderic/kite-client-go/xrg"
	"net"
	"syscall"
)

type FileSpec interface {
	Validate() bool
}

type CsvFileSpec struct {
	Fmt        string `json:"fmt"`
	Delim      string `json:"delim"`
	Quote      string `json:"quote"`
	Escape     string `json:"escape"`
	Nullstr    string `json:"nullstr"`
	HeaderLine bool   `json:"header_line"`
}

func (s CsvFileSpec) Validate() bool {
	return s.Fmt == "csv"
}

type ParquetFileSpec struct {
	Fmt string `json:"fmt"`
}

func (s ParquetFileSpec) Validate() bool {
	return s.Fmt == "parquet"
}

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
	sss     map[int]client.SockStream
	poller  *epoller.Epoll
	pages   []xrg.Iterator
	curr    *xrg.Iterator
	hosts   []string
	fragid  int
	fragcnt int
}

func NewKiteClient() *KiteClient {
	c := new(KiteClient)
	c.sss = make(map[int]client.SockStream)
	c.curr = nil
	return c
}

func (c *KiteClient) Schema(schema []Coldef) *KiteClient {
	c.request.Schema = schema
	return c
}

func (c *KiteClient) Sql(sql string) *KiteClient {
	c.request.Sql = sql
	return c
}

func (c *KiteClient) Fragment(fragid, fragcnt int) *KiteClient {
	c.fragid = fragid
	c.fragcnt = fragcnt
	return c
}

func (c *KiteClient) FileSpec(fspec FileSpec) *KiteClient {
	c.request.Spec = fspec
	return c
}

func (c *KiteClient) Host(hosts []string) *KiteClient {
	c.hosts = hosts
	return c
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
				vec, err := xrg.NewVector(msg.Buffer)
				if err != nil {
					return nil, err
				}
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

func getFd(conn net.Conn) int {
	if con, ok := conn.(syscall.Conn); ok {
		raw, err := con.SyscallConn()
		if err != nil {
			return 0
		}
		sfd := 0
		raw.Control(func(fd uintptr) {
			sfd = int(fd)
		})
		return sfd
	} else if con, ok := conn.(epoller.ConnImpl); ok {
		return con.GetFD()
	}
	return 0
}

func (c *KiteClient) Submit() error {
	var err error = nil
	var requests []Request

	c.curr = nil

	if len(c.request.Sql) == 0 {
		return fmt.Errorf("invalid SQL statement")
	}

	if c.request.Spec.Validate() == false {
		return fmt.Errorf("invalid file spec")
	}

	if len(c.request.Schema) == 0 {
		return fmt.Errorf("no schema found")
	}

	for _, col := range c.request.Schema {
		if xrg.ValidateType(col.Type) == false {
			return fmt.Errorf("invalid type in schema ", c)
		}
	}

	if c.fragcnt <= 0 {
		err := fmt.Errorf("error: fragcnt <= 0")
		return err
	}

	if c.fragid == -1 {
		for i := 0; i < c.fragcnt; i++ {
			req := c.request
			req.Fragment = [2]int{i, c.fragcnt}
			requests = append(requests, req)
		}
	} else {
		c.request.Fragment = [2]int{c.fragid, c.fragcnt}
		requests = append(requests, c.request)

	}

	c.poller, err = epoller.NewPoller(1000000)
	if err != nil {
		return err
	}

	for i := 0; i < len(requests); i++ {

		// JSON request
		js, err := json.Marshal(requests[i])
		if err != nil {
			return err
		}

		n := i % len(c.hosts)
		conn, err := net.Dial("tcp", c.hosts[n])
		if err != nil {
			return err
		}
		fd := getFd(conn)

		// add to epoll
		ss := client.SockStream{conn}
		c.sss[fd] = ss
		c.poller.Add(conn)

		// send message
		ss.Send(client.KITE_MESSAGE_KIT1, nil)
		ss.Send(client.KITE_MESSAGE_JSON, js)
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
			fd := getFd(connection)

			ss, ok := c.sss[fd]
			if !ok {
				err = fmt.Errorf("sockstream not found.")
				return it, err
			}

			page, err := c.getPage(ss)
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
				ss.Close()
				delete(c.sss, fd)
			} else {
				// push to the list
				iter := xrg.NewIterator(page)
				c.pages = append(c.pages, iter)
			}
		}
	}

	// pop
	if len(c.pages) == 0 {
		return nil, nil
	}

	x, c.pages = c.pages[0], c.pages[1:]
	return &x, nil
}

func (c *KiteClient) NextRow() (*xrg.Iterator, error) {
	var err error = nil

	if c.curr != nil && c.curr.Next() {
		return c.curr, err
	}

	c.curr, err = c.nextPage()
	if err != nil {
		return nil, err
	}
	if c.curr != nil && c.curr.Next() {
		return c.curr, err
	}
	return nil, err
}

func (c *KiteClient) Close() {
	c.poller.Close(false)
	for _, ss := range c.sss {
		ss.Close()
	}
}
