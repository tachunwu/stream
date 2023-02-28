package main

import (
	"fmt"
	"sync"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	natsclient "github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

var InProcessPubNum = 64
var PublisherMsgs = 10000
var wg = sync.WaitGroup{}
var payload = "Hello World!Hello World!Hello World!Hello World!Hello World!Hello World!"

// Test result:
// NATS Throughtput in process: 1.1509639323634913e+06
// JetStream Throughtput in process: 1.0280622211398152e+06

func main() {
	s, err := natsserver.NewServer(&natsserver.Options{
		ServerName:      "monolith",
		DontListen:      true,
		JetStream:       true,
		StoreDir:        "./",
		NoSystemAccount: true,
		MaxPayload:      16 * 1024 * 1024,
		NoSigs:          true,
		NoLog:           false,
		// Trace:           true,
	})
	if err != nil {
		panic(err)
	}
	s.ConfigureLogger()
	s.Start()

	if !s.ReadyForConnections(time.Second * 10) {
		logrus.Fatalln("NATS did not start in time")
	}

	// In-Process
	nc, err := natsclient.Connect("", natsclient.InProcessServer(s))
	if err != nil || nc == nil {
		logrus.Fatalln("Failed to create NATS client")
	}

	// Test basic NATS
	// Simple Async Subscriber
	nc.Subscribe("foo", func(m *nats.Msg) {
		_ = m.Data
	})

	wg.Add(InProcessPubNum)
	start := time.Now()
	for i := 0; i < InProcessPubNum; i++ {
		go InProcessNATSPublisher(nc)
	}
	wg.Wait()
	fmt.Println("NATS Throughtput in process:", float64(InProcessPubNum*PublisherMsgs)/time.Since(start).Seconds())

	// Test HetStream
	js, err := nc.JetStream()
	if err != nil {
		logrus.WithError(err).Panic("Unable to get JetStream context")
	}

	// Create a Stream
	js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"ORDERS.*"},
	})

	// Simple Async Ephemeral Consumer
	js.Subscribe("ORDERS.*", func(m *nats.Msg) {
		_ = m.Data
	})

	wg.Add(InProcessPubNum)
	start = time.Now()
	for i := 0; i < InProcessPubNum; i++ {
		go InProcessJetStreamPublisher(js)
	}
	wg.Wait()
	fmt.Println("JetStream Throughtput in process:", float64(InProcessPubNum*PublisherMsgs)/time.Since(start).Seconds())
}

func InProcessNATSPublisher(nc *natsclient.Conn) {
	defer wg.Done()
	for i := 0; i < PublisherMsgs; i++ {
		nc.Publish("foo", []byte(payload))
	}
}

func InProcessJetStreamPublisher(js natsclient.JetStreamContext) {
	defer wg.Done()
	for i := 0; i < PublisherMsgs; i++ {
		// Simple Stream Publisher
		js.Publish("ORDERS.scratch", []byte(payload))
	}
}
