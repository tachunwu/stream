package main

import (
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

var microBenchmarkSize int = 1000000 / batch
var batch int = 8

var wg sync.WaitGroup

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	nc, err := nats.Connect(nats.DefaultURL)
	defer nc.Close()
	if err != nil {
		log.Fatal(err)
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()

	for i := 0; i < batch; i++ {
		wg.Add(1)
		go MicroBench(js)
	}
	wg.Wait()
	logger.Info(
		"KV_Benchmark",
		zap.Float32(
			"TPS",
			float32(microBenchmarkSize*batch)/float32(time.Since(start).Seconds())),
		zap.Int("connections", batch),
		zap.Int("request per connection", microBenchmarkSize),
	)

}

func MicroBench(js nats.JetStreamContext) {
	defer wg.Done()
	for i := 0; i < microBenchmarkSize; i++ {
		kv, _ := js.KeyValue("Entity")
		k := uuid.New().String()
		kv.Create(k, []byte("value"))
	}
}
