package server

import (
	"context"
	"encoding/binary"
	"github.com/yhao1206/SMQ/protocol"
	"io"
	"log"
)

type Client struct {
	conn  io.ReadWriteCloser
	name  string
	state int
}

func NewClient(conn io.ReadWriteCloser, name string) *Client {
	return &Client{conn, name, -1}
}

func (c *Client) String() string {
	return c.name
}

func (c *Client) GetState() int {
	return c.state
}

func (c *Client) SetState(state int) {
	c.state = state
}

func (c *Client) Read(data []byte) (int, error) {
	return c.conn.Read(data)
}

func (c *Client) Write(data []byte) (int, error) {
	var err error

	err = binary.Write(c.conn, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := c.conn.Write(data)
	if err != nil {
		return 0, err
	}

	return n + 4, nil
}

func (c *Client) Close() {
	log.Printf("CLIENT(%s): closing", c.String())
	c.conn.Close()
}

// Handle reads data from the client, keeps state, and responds.
func (c *Client) Handle(ctx context.Context) {
	defer c.Close()
	proto := &protocol.Protocol{}
	err := proto.IOLoop(ctx, c)
	if err != nil {
		log.Printf("ERROR: client(%s) - %s", c.String(), err.Error())
		return
	}
}
