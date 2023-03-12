package message

import (
	"github.com/yhao1206/SMQ/util"
	"log"
)

type Consumer interface {
	Close()
}

type Channel struct {
	name                string
	addClientChan       chan util.ChanReq
	removeClientChan    chan util.ChanReq
	clients             []Consumer
	incomingMessageChan chan *Message
	msgChan             chan *Message
	ClientMessageChan   chan *Message
	exitChan            chan util.ChanReq
}

func NewChannel(name string, inMemSize int) *Channel {
	channel := &Channel{
		name:                name,
		addClientChan:       make(chan util.ChanReq),
		removeClientChan:    make(chan util.ChanReq),
		clients:             make([]Consumer, 0, 5),
		incomingMessageChan: make(chan *Message, 5),
		msgChan:             make(chan *Message, inMemSize),
		ClientMessageChan:   make(chan *Message),
		exitChan:            make(chan util.ChanReq),
	}
	go channel.Router()
	return channel
}

func (c *Channel) AddClient(client Consumer) {
	log.Printf("Channel(%s): adding client...", c.name)
	doneChan := make(chan interface{})
	c.addClientChan <- util.ChanReq{
		Variable: client,
		RetChan:  doneChan,
	}
	<-doneChan
}

func (c *Channel) RemoveClient(client Consumer) {
	log.Printf("Channel(%s): removing client...", c.name)
	doneChan := make(chan interface{})
	c.removeClientChan <- util.ChanReq{
		Variable: client,
		RetChan:  doneChan,
	}
	<-doneChan
}

func (c *Channel) PutMessage(msg *Message) {
	c.incomingMessageChan <- msg
}

// Router handles the events of Channel
func (c *Channel) Router() {
	var (
		clientReq            util.ChanReq
		messagePumpCloseChan = make(chan struct{})
	)

	go c.MessagePump(messagePumpCloseChan)

	for {
		select {
		case clientReq = <-c.addClientChan:
			client := clientReq.Variable.(Consumer)
			c.clients = append(c.clients, client)
			log.Printf("CHANNEL(%s) added client %#v", c.name, client)
			clientReq.RetChan <- struct{}{}
		case clientReq = <-c.removeClientChan:
			client := clientReq.Variable.(Consumer)
			indexToRemove := -1
			for k, v := range c.clients {
				if v == client {
					indexToRemove = k
					break
				}
			}
			if indexToRemove == -1 {
				log.Printf("ERROR: could not find client(%#v) in clients(%#v)", client, c.clients)
			} else {
				c.clients = append(c.clients[:indexToRemove], c.clients[indexToRemove+1:]...)
				log.Printf("CHANNEL(%s) removed client %#v", c.name, client)
			}
			clientReq.RetChan <- struct{}{}
		case msg := <-c.incomingMessageChan:
			select {
			case c.msgChan <- msg:
				log.Printf("CHANNEL(%s) wrote message", c.name)
			default:
			}
		case <-c.exitChan:
			log.Printf("CHANNEL(%s) is closing", c.name)
			messagePumpCloseChan <- struct{}{}

			for _, consumer := range c.clients {
				consumer.Close()
			}
		}
	}
}

// MessagePump send messages to ClientMessageChan
func (c *Channel) MessagePump(closeChan chan struct{}) {
	var msg *Message

	for {
		select {
		case msg = <-c.msgChan:
		case <-closeChan:
			return
		}

		c.ClientMessageChan <- msg
	}
}

func (c *Channel) Close() error {
	errChan := make(chan interface{})
	c.exitChan <- util.ChanReq{
		RetChan: errChan,
	}

	err, _ := (<-errChan).(error)
	return err
}
