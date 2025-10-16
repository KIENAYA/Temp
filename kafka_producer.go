package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Cấu hình producer
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		
		// Tăng queue buffer
		"queue.buffering.max.messages": 500000,
		"queue.buffering.max.kbytes":   10485760, // 10GB
		
		// Tối ưu performance
		"linger.ms":        10,
		"batch.size":       1000000,
		"compression.type": "snappy",
		
		// Timeout
		"request.timeout.ms": 30000,
		"message.timeout.ms": 300000,
		
		// Delivery semantics
		"acks": "all",
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// Channel để xử lý graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Goroutine để đọc delivery reports
	deliveryChan := make(chan kafka.Event)
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("❌ Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					log.Printf("✅ Delivered to partition %d at offset %v\n",
						ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			case kafka.Error:
				log.Printf("⚠️  Error: %v\n", ev)
			}
		}
	}()

	// Produce messages
	topic := "my-topic"
	messageCount := 10000

	go func() {
		for i := 0; i < messageCount; i++ {
			value := fmt.Sprintf("Message number %d - timestamp: %d", i, time.Now().Unix())
			
			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				},
				Value: []byte(value),
				Key:   []byte(fmt.Sprintf("key-%d", i)),
			}

			// Produce với retry logic
			err := produceWithRetry(producer, msg, 5)
			if err != nil {
				log.Printf("Failed to produce message %d: %v\n", i, err)
			}

			// Hiển thị progress
			if i%1000 == 0 {
				log.Printf("Produced %d messages\n", i)
			}
		}
		
		log.Println("All messages queued. Waiting for delivery...")
	}()

	// Đợi signal hoặc timeout
	select {
	case sig := <-sigchan:
		log.Printf("Caught signal %v: terminating\n", sig)
	case <-time.After(60 * time.Second):
		log.Println("Timeout reached")
	}

	// Flush tất cả messages còn pending
	log.Println("Flushing remaining messages...")
	remaining := producer.Flush(30 * 1000) // 30 seconds
	log.Printf("Flush complete. %d messages remaining\n", remaining)
}

// produceWithRetry thử gửi message với retry khi queue full
func produceWithRetry(producer *kafka.Producer, msg *kafka.Message, maxRetries int) error {
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := producer.Produce(msg, nil)
		
		if err == nil {
			return nil
		}

		// Kiểm tra loại lỗi
		kafkaErr, ok := err.(kafka.Error)
		if !ok {
			return err
		}

		// Nếu là queue full, retry
		if kafkaErr.Code() == kafka.ErrQueueFull {
			log.Printf("Queue full, attempt %d/%d - waiting...\n", attempt+1, maxRetries)
			
			// Backoff strategy: đợi lâu dần
			sleepDuration := time.Duration(100*(attempt+1)) * time.Millisecond
			time.Sleep(sleepDuration)
			continue
		}

		// Lỗi khác, return ngay
		return err
	}

	return fmt.Errorf("failed to produce after %d retries: queue full", maxRetries)
}
