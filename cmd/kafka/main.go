/*
Copyright 2018 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
)

var (
	bootstrapservers string
	topic            string
	sink             string
)

func init() {
	flag.StringVar(&bootstrapservers, "bootstrapservers", "", "the host url to Kafka")
	flag.StringVar(&topic, "topic", "", "a topic for receiving")
	flag.StringVar(&sink, "sink", "", "the host url to send data to")
}

func main() {
	flag.Parse()

	consumer, err := sarama.NewConsumer([]string{bootstrapservers}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// OffsetNewest
	initialOffset := sarama.OffsetOldest //get offset for the oldest message on the topic
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, initialOffset)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0

	running := true
	for running {
		select {
		case msg := <-partitionConsumer.Messages():
			send(string(msg.Value))
			log.Printf("Consumed message: %s", msg.Value)
			consumed++
		case <-signals:
			running = false
		}
	}

	log.Printf("Consumed: %d\n", consumed)

}

func send(val string) {
	resp, err := http.Post(sink, "application/json", body(val))
	if err != nil {
		log.Printf("Unable to make request: %v", err)
		return
	} else {
		log.Printf("[%d]", resp.StatusCode)
	}
	defer resp.Body.Close()
}

func body(val string) io.Reader {
	b, err := json.Marshal(val)
	if err != nil {
		return strings.NewReader("{\"error\":\"true\"}")
	}
	return bytes.NewBuffer(b)
}
