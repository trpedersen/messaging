package messaging

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	// "time"
	"runtime"
)

func TestPubSub(t *testing.T) {
	SUBSCRIBERS := 1 //00
	MESSAGE_COUNT := 1000000
	SUBJECTS := 1

	nCPU := runtime.NumCPU()
	fmt.Println("nCPU:", nCPU)
	runtime.GOMAXPROCS(nCPU)

	var exchange = NewExchange()
	go exchange.Run()

	var wg sync.WaitGroup

	messagesBack := make(chan *Message)

	handler := func(msg *Message) error {
		//fmt.Println("handler msg: ", msg)
		messagesBack <- msg
		return nil
	}

	publish := func(subject string) {
		wg.Add(1)
		defer wg.Done()
		for i := 0; i < MESSAGE_COUNT; i++ {
			data := []byte(strconv.Itoa(i))
			exchange.Publish(subject, data)
		}
	}

	for s := 1; s <= SUBJECTS; s++ {
		subject := fmt.Sprintf("SUBJECT.%d", s)
		for i := 1; i <= SUBSCRIBERS; i++ {
			exchange.Subscribe(subject, handler)
		}
		// wg.Add(1)
		go publish(subject)
	}

	fmt.Println(len(exchange.subscriptions))
	count := 0
	for range messagesBack {
		//fmt.Println("messageBack: ", msg)
		count += 1
		if count == (SUBJECTS * SUBSCRIBERS * MESSAGE_COUNT) {
			break
		}
	}
	exchange.halt <- true
	<-exchange.halt
	wg.Wait()
	fmt.Println("exchange.msg_pub: ", exchange.msg_pub, "exchange.msg_sent: ", exchange.msg_sent, "messages back: ", count)

}

func xTestPubSubChan(t *testing.T) {
	const SUBSCRIBERS = 1 //00
	const MESSAGE_COUNT = 1000000
	const SUBJECTS = 1

	nCPU := runtime.NumCPU()
	fmt.Println("nCPU:", nCPU)
	runtime.GOMAXPROCS(nCPU)

	var exchange = NewExchange()
	go exchange.Run()

	var wg sync.WaitGroup

	sub := make(chan []byte)

	publish := func(subject string) {
		wg.Add(1)
		defer wg.Done()
		pub := make(chan []byte)
		exchange.BindPublishChan(subject, pub)
		for i := 0; i < MESSAGE_COUNT; i++ {
			data := []byte(strconv.Itoa(i))
			pub <- data
		}
	}

	var subscribers [SUBSCRIBERS]chan []byte
	for i := range subscribers {
		subscribers[i] = make(chan []byte)
		go func(ch chan []byte) {
			for m := range ch {
				sub <- m
			}
		}(subscribers[i])
	}

	for s := 1; s <= SUBJECTS; s++ {
		subject := fmt.Sprintf("SUBJECT.%d", s)
		for i := 0; i < SUBSCRIBERS; i++ {
			exchange.BindSubscribeChan(subject, subscribers[i])
		}
		// wg.Add(1)
		go publish(subject)
	}

	fmt.Println(len(exchange.subscriptions))
	count := 0
	for range sub {
		//fmt.Println("messageBack: ", msg)
		count += 1
		if count == (SUBJECTS * SUBSCRIBERS * MESSAGE_COUNT) {
			break
		}
	}
	exchange.halt <- true
	<-exchange.halt
	wg.Wait()
	fmt.Println("exchange.msg_pub: ", exchange.msg_pub, "exchange.msg_sent: ", exchange.msg_sent, "messages back: ", count)

}
