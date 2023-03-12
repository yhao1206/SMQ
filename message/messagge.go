package message

type Message struct {
	data []byte
}

func NewMessage(data []byte) *Message {
	return &Message{
		data: data,
	}
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
