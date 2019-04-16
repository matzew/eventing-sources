/*
Copyright 2019 The Knative Authors

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

package kafka

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/cloudevents/sdk-go/pkg/cloudevents"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/client"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/types"
	"github.com/knative/eventing-sources/pkg/kncloudevents"
	"github.com/knative/pkg/logging"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

const (
	eventType = "dev.knative.kafka.event"
)

type AdapterSASL struct {
	Enable   bool
	User     string
	Password string
}

type AdapterTLS struct {
	Enable bool
}

type AdapterNet struct {
	SASL AdapterSASL
	TLS  AdapterTLS
}

type Adapter struct {
	BootstrapServers string
	Topics           string
	ConsumerGroup    string
	Net              AdapterNet
	SinkURI          string
	client           client.Client
	kafkaCluster     KafkaCluster
}

type KafkaConsumer interface {
	Partitions() <-chan cluster.PartitionConsumer
	MarkOffset(msg *sarama.ConsumerMessage, metadata string)
	Close() (err error)
}

type KafkaCluster interface {
	NewConsumer(a *Adapter) (KafkaConsumer, error)
}

func (a *Adapter) initClient() error {
	if a.client == nil {
		var err error
		if a.client, err = kncloudevents.NewDefaultClient(a.SinkURI); err != nil {
			return err
		}
	}

	if a.kafkaCluster == nil {
		a.kafkaCluster = &saramaCluster{kafkaBrokers: strings.Split(a.BootstrapServers, ",")}
	}
	return nil
}

type saramaCluster struct {
	kafkaBrokers []string

	consumerMode cluster.ConsumerMode
}

func (c *saramaCluster) NewConsumer(a *Adapter) (KafkaConsumer, error) {
	kafkaConfig := cluster.NewConfig()
	kafkaConfig.Group.Mode = cluster.ConsumerModePartitions
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Consumer.Return.Errors = true
	kafkaConfig.Net.SASL.Enable = a.Net.SASL.Enable
	kafkaConfig.Net.SASL.User = a.Net.SASL.User
	kafkaConfig.Net.SASL.Password = a.Net.SASL.Password
	kafkaConfig.Net.TLS.Enable = a.Net.TLS.Enable

	return cluster.NewConsumer(c.kafkaBrokers, a.ConsumerGroup, []string{a.Topics}, kafkaConfig)
}

func (a *Adapter) Start(ctx context.Context, stopCh <-chan struct{}) error {
	logger := logging.FromContext(ctx)

	logger.Info("Starting with config: ", zap.Any("adapter", a))

	if err := a.initClient(); err != nil {
		logger.Error("Failed to create cloudevent client", zap.Error(err))
		return err
	}

	consumer, err := a.kafkaCluster.NewConsumer(a)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	go a.consumerMessages(ctx, consumer)

	for {
		select {
		case <-stopCh:
			logger.Info("Shutting down...")
			return nil
		}
	}
}

func (a *Adapter) consumerMessages(ctx context.Context, consumer KafkaConsumer) {
	logger := logging.FromContext(ctx)

	for {
		pc, more := <-consumer.Partitions()
		if !more {
			break
		}
		go func(pc cluster.PartitionConsumer) {
			for msg := range pc.Messages() {
				if err := a.postMessage(ctx, msg); err == nil {
					consumer.MarkOffset(msg, "")
					logger.Debug("Successfully sent event to sink")
				} else {
					logger.Error("Sending event to sink failed: ", zap.Error(err))
				}

			}
		}(pc)
	}
}

func (a *Adapter) postMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {

	extensions := map[string]interface{}{
		"key": string(msg.Key),
	}
	event := cloudevents.Event{
		Context: cloudevents.EventContextV02{
			SpecVersion: cloudevents.CloudEventsVersionV02,
			Type:        eventType,
			ID:          "partition:" + strconv.Itoa(int(msg.Partition)) + "/offset:" + strconv.FormatInt(msg.Offset, 10),
			Time:        &types.Timestamp{Time: msg.Timestamp},
			Source:      *types.ParseURLRef(msg.Topic),
			ContentType: cloudevents.StringOfApplicationJSON(),
			Extensions:  extensions,
		}.AsV02(),
		Data: a.jsonEncode(ctx, msg.Value),
	}

	_, err := a.client.Send(ctx, event)
	return err
}

func (a *Adapter) jsonEncode(ctx context.Context, value []byte) interface{} {
	var payload map[string]interface{}

	logger := logging.FromContext(ctx)

	if err := json.Unmarshal(value, &payload); err != nil {
		logger.Info("Error unmarshalling JSON: ", zap.Error(err))
		return value
	} else {
		return payload
	}
}
