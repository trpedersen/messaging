package messaging

import (
	"errors"
	"sync/atomic"
)

type Message struct {
	subject string
	data    []byte
}

func NewMessage(subject string, data interface{}) (msg *Message, err error) {
	switch data := data.(type) {
	case string:
		return &Message{subject: subject, data: []byte(data)}, nil
	case []byte:
		return &Message{subject: subject, data: data}, nil
	default:
		return nil, errors.New("messaging: invalid data type")
	}
}

type MessageHandler func(msg *Message) (err error)

type Subscription struct {
	id       int64
	Subject  string
	handler  MessageHandler
	exchange *Exchange
	ch       chan *Message
}

type Exchanger interface {
	Publish(subject string, data []byte) error
	Subscribe(subject string, handler MessageHandler) (*Subscription, error)

	// bind consumers
	BindPublishChan(subject string, ch chan []byte) (*Subscription, error)
	BindSubscribeChan(subject string, ch chan []byte) (*Subscription, error)

	Run()
	Halt()
}

type Exchange struct {
	// registered message consumers
	subscriptions map[int64]*Subscription
	subjects      map[string][]*Subscription

	// inbound messages from the consumers
	publish   chan *Message
	pub_chans map[chan []byte]bool

	halt chan bool

	ssid     int64
	msg_sent int64
	msg_pub  int64
}

func NewExchange() *Exchange {
	return &Exchange{
		publish:       make(chan *Message, 20),
		subscriptions: make(map[int64]*Subscription),
		subjects:      make(map[string][]*Subscription),
		halt:          make(chan bool),
		pub_chans:     make(map[chan []byte]bool),
	}
}

func (this *Exchange) Publish(subject string, data interface{}) (err error) {
	msg, err := NewMessage(subject, data)
	if err != nil {
		return err
	}
	this.publish <- msg
	atomic.AddInt64(&this.msg_pub, 1)
	return nil
}

func (this *Exchange) Subscribe(subject string, handler MessageHandler) (*Subscription, error) {
	sub := &Subscription{
		id:       atomic.AddInt64(&this.ssid, 1),
		Subject:  subject,
		handler:  handler,
		exchange: this,
		ch:       make(chan *Message),
	}
	this.subscriptions[sub.id] = sub
	if _, exists := this.subjects[subject]; !exists {
		this.subjects[subject] = make([]*Subscription, 0)
	}
	this.subjects[subject] = append(this.subjects[subject], sub)
	go this.handleMessages(sub)
	return sub, nil
}

func (this *Exchange) BindSubscribeChan(subject string, ch chan []byte) (*Subscription, error) {
	handler := func(msg *Message) error {
		ch <- msg.data
		return nil
	}
	sub, err := this.Subscribe(subject, handler)
	return sub, err
}

func (this *Exchange) publishMessages(subject string, ch chan []byte) {
	for data := range ch {
		this.Publish(subject, data)
	}
}

func (this *Exchange) BindPublishChan(subject string, ch chan []byte) error {
	if _, exists := this.pub_chans[ch]; exists {
		return errors.New("messaging: publish channel already bound")
	}
	go this.publishMessages(subject, ch)
	return nil
}

func (this *Exchange) handleMessages(sub *Subscription) {
	for msg := range sub.ch {
		if err := sub.handler(msg); err != nil {
			break
		}
	}
}

func (this *Exchange) cleanup() {
	for subject, subs := range this.subjects {
		for _, sub := range subs {
			close(sub.ch)
		}
		delete(this.subjects, subject)
	}
}

func (this *Exchange) Run() {
	running := true
	for running {
		select {
		case msg := <-this.publish:
			if subs, exists := this.subjects[msg.subject]; exists {
				for _, sub := range subs {
					sub.ch <- msg
					atomic.AddInt64(&this.msg_sent, 1)
				}
			}
		case <-this.halt:
			this.cleanup()
			close(this.publish)
			this.halt <- true
			running = false
		}
	}
}

func (this *Exchange) Halt() {
	this.halt <- true
	<-this.halt
}
