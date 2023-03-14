package message

import (
	"github.com/yhao1206/SMQ/queue"
	"github.com/yhao1206/SMQ/util"
	"log"
)

type Topic struct {
	name                string
	newChannelChan      chan util.ChanReq
	channelMap          map[string]*Channel
	incomingMessageChan chan *Message
	msgChan             chan *Message
	readSyncChan        chan struct{}
	routerSyncChan      chan struct{}
	exitChan            chan util.ChanReq
	channelWriteStarted bool
	backend             queue.Queue
}

var (
	TopicMap     = make(map[string]*Topic)
	newTopicChan = make(chan util.ChanReq)
)

func NewTopic(name string, inMemSize int) *Topic {
	topic := &Topic{
		name:                name,
		newChannelChan:      make(chan util.ChanReq),
		channelMap:          make(map[string]*Channel),
		incomingMessageChan: make(chan *Message),
		msgChan:             make(chan *Message, inMemSize),
		readSyncChan:        make(chan struct{}),
		routerSyncChan:      make(chan struct{}),
		exitChan:            make(chan util.ChanReq),
		backend:             queue.NewDiskQueue(name),
	}
	go topic.Router(inMemSize)
	return topic
}

func GetTopic(name string) *Topic {
	topicChan := make(chan interface{})
	newTopicChan <- util.ChanReq{
		Variable: name,
		RetChan:  topicChan,
	}
	return (<-topicChan).(*Topic)
}

func TopicFactory(inMemSize int) {
	var (
		topicReq util.ChanReq
		name     string
		topic    *Topic
		ok       bool
	)
	for {
		topicReq = <-newTopicChan
		name = topicReq.Variable.(string)
		if topic, ok = TopicMap[name]; !ok {
			topic = NewTopic(name, inMemSize)
			TopicMap[name] = topic
			log.Printf("TOPIC %s CREATED", name)
		}
		topicReq.RetChan <- topic
	}
}

func (t *Topic) GetChannel(channelName string) *Channel {
	channelRet := make(chan interface{})
	t.newChannelChan <- util.ChanReq{
		Variable: channelName,
		RetChan:  channelRet,
	}
	return (<-channelRet).(*Channel)
}

func (t *Topic) PutMessage(msg *Message) {
	t.incomingMessageChan <- msg
}

func (t *Topic) MessagePump(closeChan <-chan struct{}) {
	var msg *Message
	for {
		select {
		case msg = <-t.msgChan:
		case <-t.backend.ReadReadyChan():
			bytes, err := t.backend.Get()
			if err != nil {
				log.Printf("ERROR: t.backend.Get() - %s", err.Error())
				continue
			}
			msg = NewMessage(bytes)
		case <-closeChan:
			return
		}

		t.readSyncChan <- struct{}{}

		for _, channel := range t.channelMap {
			go func(ch *Channel) {
				ch.PutMessage(msg)
			}(channel)
		}

		t.routerSyncChan <- struct{}{}
	}
}

func (t *Topic) Router(inMemSize int) {
	var (
		msg       *Message
		closeChan = make(chan struct{})
	)
	for {
		select {
		case channelReq := <-t.newChannelChan:
			channelName := channelReq.Variable.(string)
			channel, ok := t.channelMap[channelName]
			if !ok {
				channel = NewChannel(channelName, inMemSize)
				t.channelMap[channelName] = channel
				log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
			}
			channelReq.RetChan <- channel
			if !t.channelWriteStarted {
				go t.MessagePump(closeChan)
				t.channelWriteStarted = true
			}
		case msg = <-t.incomingMessageChan:
			select {
			case t.msgChan <- msg:
				log.Printf("TOPIC(%s) wrote message", t.name)
			default:
				err := t.backend.Put(msg.data)
				if err != nil {
					log.Printf("ERROR: t.backend.Put() - %s", err.Error())
				}
				log.Printf("TOPIC(%s): wrote to backend", t.name)
			}
		case <-t.readSyncChan:
			<-t.routerSyncChan
		case closeReq := <-t.exitChan:
			log.Printf("TOPIC(%s): closing", t.name)

			for _, channel := range t.channelMap {
				err := channel.Close()
				if err != nil {
					log.Printf("ERROR: channel(%s) close - %s", channel.name, err.Error())
				}
			}

			close(closeChan)
			closeReq.RetChan <- t.backend.Close()
		}
	}
}

func (t *Topic) Close() error {
	errChan := make(chan interface{})
	t.exitChan <- util.ChanReq{
		RetChan: errChan,
	}
	err, _ := (<-errChan).(error)
	return err
}
