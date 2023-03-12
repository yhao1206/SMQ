package message

import (
	"bytes"
	"encoding/gob"
)

type Message struct {
	data []byte
}

func NewMessage(data []byte) *Message {
	return &Message{
		data: data,
	}
}

func NewMessageDecoded(data []byte) (msg *Message, err error) {
	var (
		b bytes.Buffer
		d *gob.Decoder
	)
	if _, err = b.Write(data); err != nil {
		return
	}
	d = gob.NewDecoder(&b)
	err = d.Decode(msg)
	return
}

func (m *Message) Uuid() []byte {
	return m.data[:16]
}

func (m *Message) Body() []byte {
	return m.data[16:]
}

func (m *Message) Data() []byte {
	return m.data
}

func (m *Message) Encode() ([]byte, error) {
	var (
		b   bytes.Buffer
		e   = gob.NewEncoder(&b)
		err error
	)
	if err = e.Encode(*m); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
