package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	natsclient "github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

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
		Trace:           true,
	})
	if err != nil {
		panic(err)
	}
	s.ConfigureLogger()
	s.Start()

	if !s.ReadyForConnections(time.Second * 10) {
		logrus.Fatalln("NATS did not start in time")
	}
	nc, err := natsclient.Connect("", natsclient.InProcessServer(s))
	if err != nil {
		logrus.Fatalln("Failed to create NATS client")
	}
	if nc == nil {
		var err error
		opts := []natsclient.Option{}
		opts = append(opts, natsclient.Secure(&tls.Config{
			InsecureSkipVerify: true,
		}))

		nc, err = natsclient.Connect(strings.Join([]string{}, ","), opts...)
		if err != nil {
			logrus.WithError(err).Panic("Unable to connect to NATS")
		}
	}

	js, err := nc.JetStream()
	if err != nil {
		logrus.WithError(err).Panic("Unable to get JetStream context")
	}

	js.AddStream(&natsclient.StreamConfig{
		Name:      "InProcess",
		Retention: natsclient.InterestPolicy,
		Storage:   natsclient.FileStorage,
	})
	info := <-js.Streams()
	log.Println(
		"Stream Name:", info.Config.Name,
		"Stream Subject:", info.Config.Subjects,
	)
	// Test pub/sub
	js.Publish("ORDERS.scratch", []byte("hello"))
	js.Subscribe("ORDERS.*", func(m *nats.Msg) {
		fmt.Printf("Received a JetStream message: %s\n", string(m.Data))
	}, natsclient.OrderedConsumer())

	for {
		runtime.Gosched()
	}

}
