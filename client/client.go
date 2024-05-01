package client

import (
	"fmt"
	"net"
	"strconv"
)

var KITE_MESSAGE_KIT1 = [4]byte{'K', 'I', 'T', '1'}
var KITE_MESSAGE_JSON = [4]byte{'J', 'S', 'O', 'N'}
var KITE_MESSAGE_BYE = [4]byte{'B', 'Y', 'E', '_'}
var KITE_MESSAGE_ERROR = [4]byte{'E', 'R', 'R', '_'}
var KITE_MESSAGE_VECTOR = [4]byte{'V', 'E', 'C', '_'}

type KiteMessage struct {
	Msgty  [4]byte
	Msglen int32
	Buffer []byte
}

type SockStream struct {
	socket net.Conn
}

func (sock *SockStream) Close() {
	sock.socket.Close()
}

func (sock *SockStream) readfully(msg []byte, msgsz int) error {
	var err error = nil
	p := 0
	msglen := msgsz
	for p < msgsz {
		n, err := sock.socket.Read(msg[p:])
		if err != nil {
			return err
		}
		p += n
		msglen -= n
	}

	return err
}

func (sock *SockStream) writefully(msg []byte, msgsz int) error {
	var err error = nil
	p := 0
	msglen := msgsz
	for p < msgsz {
		n, err := sock.socket.Write(msg[p:])
		if err != nil {
			return err
		}
		p += n
		msglen -= n
	}

	return err
}

func (sock *SockStream) Send(msgty []byte, msg []byte) error {
	msgsz := 0
	if msg != nil {
		msgsz = len(msg)
	}

	hex := []byte(fmt.Sprintf("%08X", msgsz))
	meta := append(msgty, hex...)

	err := sock.writefully(meta, len(meta))
	if err != nil {
		return err
	}

	if msg != nil {
		err = sock.writefully(msg, msgsz)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sock *SockStream) Recv() (msg KiteMessage, err error) {
	meta := make([]byte, 12)
	err = sock.readfully(meta, len(meta))
	if err != nil {
		return
	}

	var msgty [4]byte
	copy(msgty[:], meta[0:4])
	msglen, err := strconv.ParseInt(string(meta[4:]), 16, 32)
	if err != nil {
		return
	}

	msg = KiteMessage{msgty, int32(msglen), make([]byte, msglen)}
	if msglen > 0 {
		sock.readfully(msg.Buffer, int(msglen))
	}
	return
}
