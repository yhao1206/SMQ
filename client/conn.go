package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/yhao1206/SMQ/message"
	"io"
	"net"
	"strconv"
)

type Client struct {
	conn io.ReadWriteCloser
}

type Command struct {
	name   []byte
	params [][]byte
}

type Response struct {
	FrameType int32
	Data      interface{}
}

func NewClient(conn net.Conn) *Client {
	return &Client{conn}
}

func (c *Client) Connect(address string, port int) error {
	fqAddress := address + ":" + strconv.Itoa(port)
	conn, err := net.Dial("tcp", fqAddress)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) Version(version string) error {
	_, err := c.conn.Write([]byte(version))
	return err
}

func (c *Client) Subscribe(topic string, channel string) *Command {
	params := make([][]byte, 2)
	params[0] = []byte(topic)
	params[1] = []byte(channel)
	return &Command{[]byte("SUB"), params}
}

func (c *Client) Ready(count int) *Command {
	params := make([][]byte, 1)
	params[0] = []byte(strconv.Itoa(count))
	return &Command{[]byte("RDY"), params}
}

func (c *Client) Finish(uuid string) *Command {
	params := make([][]byte, 1)
	params[0] = []byte(uuid)
	return &Command{[]byte("FIN"), params}
}

func (c *Client) Requeue(uuid string) *Command {
	params := make([][]byte, 1)
	params[0] = []byte(uuid)
	return &Command{[]byte("REQ"), params}
}

func (c *Client) Get() *Command {
	return &Command{[]byte("GET"), nil}
}

func (c *Client) WriteCommand(cmd *Command) error {
	if len(cmd.params) > 0 {
		_, err := fmt.Fprintf(c.conn, "%s %s\n", cmd.name, string(bytes.Join(cmd.params, []byte(" "))))
		if err != nil {
			return err
		}
	} else {
		_, err := fmt.Fprintf(c.conn, "%s\n", cmd.name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) ReadResponse() (*message.Message, error) {
	err := c.WriteCommand(c.Get())
	if err != nil {
		return nil, err
	}

	var msgSize int32
	err = binary.Read(c.conn, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, msgSize)
	_, err = c.conn.Read(buf)
	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(buf)

	return msg, nil
}
