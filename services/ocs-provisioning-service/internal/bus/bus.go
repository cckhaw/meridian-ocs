// Package bus is the Kafka transport for provisioning commands and events.
package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes a message. Key decides the partition, so all messages for
// one subscriber stay in order.
type Producer interface {
	Produce(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
}

// Config names the topics and the consumer group.
type Config struct {
	Brokers       []string
	CommandsTopic string // default ocs.provisioning.commands
	EventsTopic   string // default ocs.provisioning.events
	DLQTopic      string // default ocs.provisioning.dlq
	Group         string // default ocs-provisioning
}

// WithDefaults fills the topic and group names.
func (c Config) WithDefaults() Config {
	if c.CommandsTopic == "" {
		c.CommandsTopic = "ocs.provisioning.commands"
	}
	if c.EventsTopic == "" {
		c.EventsTopic = "ocs.provisioning.events"
	}
	if c.DLQTopic == "" {
		c.DLQTopic = "ocs.provisioning.dlq"
	}
	if c.Group == "" {
		c.Group = "ocs-provisioning"
	}
	return c
}

// Validate checks the configuration.
func (c Config) Validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("bus: at least one broker is required")
	}
	for _, t := range []string{c.CommandsTopic, c.EventsTopic, c.DLQTopic} {
		if t == "" || strings.ContainsAny(t, " /") {
			return fmt.Errorf("bus: bad topic name %q", t)
		}
	}
	return nil
}

// KafkaProducer produces synchronously with all in-sync replicas
// acknowledging (franz-go's default) and an idempotent producer, so a retried
// produce does not duplicate a message.
type KafkaProducer struct{ cl *kgo.Client }

// NewProducer connects a producer. Extra options (TLS, SASL, ...) are passed through.
func NewProducer(brokers []string, opts ...kgo.Opt) (*KafkaProducer, error) {
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks())}, opts...)...)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{cl: cl}, nil
}

// Produce implements Producer.
func (p *KafkaProducer) Produce(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	r := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}
	for k, v := range headers {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return p.cl.ProduceSync(ctx, r).FirstErr()
}

// Close flushes and closes the client.
func (p *KafkaProducer) Close() { p.cl.Close() }

// NewConsumer connects a consumer-group client that commits only when told to
// and does not rebalance while a batch is being processed.
func NewConsumer(c Config, opts ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumerGroup(c.Group),
		kgo.ConsumeTopics(c.CommandsTopic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	}, opts...)...)
}
