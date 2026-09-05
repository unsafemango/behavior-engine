// Package kafkax wraps segmentio/kafka-go with the small set of
// behaviors behavior-engine needs: a producer that keys messages by
// user id (so all of one user's events land in the same partition and
// are processed in order) and a consumer configured for at-least-once
// delivery with explicit offset commits.
package kafkax

import (
	"context"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// Producer publishes events to a single topic, partitioned by key.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer builds a producer with sane production defaults: batching
// up to 100 messages or 50ms (whichever comes first) to keep throughput
// high under the load test without adding much latency, and
// RequireOne acknowledgement so a publish only succeeds once the
// partition leader has it.
func NewProducer(brokers []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // key-based partitioning -> per-user ordering
		RequiredAcks: kafka.RequireOne,
		BatchSize:    100,
		BatchTimeout: 50 * time.Millisecond,
		Async:        false,
	}
	return &Producer{writer: w}
}

// Publish writes one message keyed by key with the given value.
func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	})
}

// Close flushes and closes the underlying writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// Consumer reads messages from a topic as part of a consumer group,
// committing offsets explicitly after the caller has processed a
// message so a crash mid-batch results in re-delivery, not data loss.
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer builds a group consumer for topic.
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		StartOffset:    kafka.FirstOffset, // a brand new consumer group starts from the beginning of the topic, not the tail
		CommitInterval: 0,                // we commit explicitly per message for at-least-once semantics
		ErrorLogger:    kafka.LoggerFunc(func(msg string, args ...interface{}) { log.Printf("[kafka-go][ERROR] "+msg, args...) }),
	})
	return &Consumer{reader: r}
}

// FetchMessage blocks until the next message is available or ctx is
// done. Callers must call CommitMessage after successfully processing
// it.
func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

// CommitMessage marks msg (and everything before it in its partition)
// as processed.
func (c *Consumer) CommitMessage(ctx context.Context, msg kafka.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

// Close closes the underlying reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
