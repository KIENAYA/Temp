package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// ProducerMonitor theo dõi chi tiết internal queue của producer
type ProducerMonitor struct {
	producer    *kafka.Producer
	stats       *ProducerStats
	interval    time.Duration
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	logger      *log.Logger
	alerts      []Alert
	alertsMutex sync.RWMutex
}

// ProducerStats lưu trữ statistics
type ProducerStats struct {
	mu sync.RWMutex
	
	// Snapshot hiện tại
	current StatSnapshot
	
	// History để tính rate
	history []StatSnapshot
	maxHistory int
}

type StatSnapshot struct {
	Timestamp time.Time
	
	// Queue metrics
	MsgCount      int64   // Messages trong queue
	MsgSize       int64   // Bytes trong queue
	QueueCapacity int64   // Max capacity
	QueueUsagePct float64 // % usage
	
	// Producer metrics
	TxMsgs        int64 // Total messages sent
	TxBytes       int64 // Total bytes sent
	TxErrors      int64 // Total errors
	
	// Rate metrics (tính từ history)
	TxMsgsRate    float64 // Messages/second
	TxBytesRate   float64 // Bytes/second
	
	// Broker metrics
	OutQueueCount int64 // Messages waiting for broker response
	OutQueueBytes int64
	
	// Metadata
	BrokerCount   int
	TopicCount    int
	PartitionCount int
}

type Alert struct {
	Timestamp time.Time
	Level     string // INFO, WARNING, CRITICAL
	Message   string
	Value     float64
}

func NewProducerMonitor(producer *kafka.Producer, interval time.Duration) *ProducerMonitor {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &ProducerMonitor{
		producer: producer,
		stats: &ProducerStats{
			history:    make([]StatSnapshot, 0, 60), // Keep 60 snapshots
			maxHistory: 60,
		},
		interval: interval,
		ctx:      ctx,
		cancel:   cancel,
		logger:   log.New(os.Stdout, "[MONITOR] ", log.LstdFlags),
		alerts:   make([]Alert, 0),
	}
}

// Start bắt đầu monitoring
func (pm *ProducerMonitor) Start() {
	pm.wg.Add(1)
	go pm.monitorLoop()
	
	pm.logger.Println("✅ Producer monitoring started")
}

// monitorLoop vòng lặp chính để collect metrics
func (pm *ProducerMonitor) monitorLoop() {
	defer pm.wg.Done()
	
	ticker := time.NewTicker(pm.interval)
	defer ticker.Stop()
	
	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			pm.collectMetrics()
		}
	}
}

// collectMetrics thu thập metrics từ producer
func (pm *ProducerMonitor) collectMetrics() {
	// Lấy statistics từ librdkafka
	stats := pm.producer.Stats()
	
	var statsData map[string]interface{}
	if err := json.Unmarshal([]byte(stats), &statsData); err != nil {
		pm.logger.Printf("❌ Failed to parse stats: %v", err)
		return
	}
	
	snapshot := pm.parseStats(statsData)
	
	// Tính rate metrics
	pm.stats.mu.Lock()
	pm.calculateRates(&snapshot)
	pm.stats.current = snapshot
	pm.stats.history = append(pm.stats.history, snapshot)
	
	// Giữ chỉ maxHistory snapshots
	if len(pm.stats.history) > pm.stats.maxHistory {
		pm.stats.history = pm.stats.history[1:]
	}
	pm.stats.mu.Unlock()
	
	// Check alerts
	pm.checkAlerts(snapshot)
	
	// Log summary
	pm.logSummary(snapshot)
}

// parseStats parse JSON statistics từ librdkafka
func (pm *ProducerMonitor) parseStats(data map[string]interface{}) StatSnapshot {
	snapshot := StatSnapshot{
		Timestamp: time.Now(),
	}
	
	// Parse queue metrics
	if msgCnt, ok := data["msg_cnt"].(float64); ok {
		snapshot.MsgCount = int64(msgCnt)
	}
	if msgSize, ok := data["msg_size"].(float64); ok {
		snapshot.MsgSize = int64(msgSize)
	}
	if msgMax, ok := data["msg_max"].(float64); ok {
		snapshot.QueueCapacity = int64(msgMax)
	}
	
	// Tính queue usage percentage
	if snapshot.QueueCapacity > 0 {
		snapshot.QueueUsagePct = float64(snapshot.MsgCount) / float64(snapshot.QueueCapacity) * 100
	}
	
	// Parse producer metrics
	if txmsgs, ok := data["txmsgs"].(float64); ok {
		snapshot.TxMsgs = int64(txmsgs)
	}
	if txbytes, ok := data["txmsg_bytes"].(float64); ok {
		snapshot.TxBytes = int64(txbytes)
	}
	if txerrs, ok := data["txerrs"].(float64); ok {
		snapshot.TxErrors = int64(txerrs)
	}
	
	// Parse outqueue (messages waiting for broker ack)
	if outqCnt, ok := data["outq_cnt"].(float64); ok {
		snapshot.OutQueueCount = int64(outqCnt)
	}
	if outqMsgCnt, ok := data["outq_msg_cnt"].(float64); ok {
		snapshot.OutQueueBytes = int64(outqMsgCnt)
	}
	
	// Parse broker info
	if brokers, ok := data["brokers"].(map[string]interface{}); ok {
		snapshot.BrokerCount = len(brokers)
	}
	
	// Parse topic info
	if topics, ok := data["topics"].(map[string]interface{}); ok {
		snapshot.TopicCount = len(topics)
		
		// Count partitions
		for _, topicData := range topics {
			if topicMap, ok := topicData.(map[string]interface{}); ok {
				if partitions, ok := topicMap["partitions"].(map[string]interface{}); ok {
					snapshot.PartitionCount += len(partitions)
				}
			}
		}
	}
	
	return snapshot
}

// calculateRates tính toán rates từ history
func (pm *ProducerMonitor) calculateRates(current *StatSnapshot) {
	if len(pm.stats.history) < 2 {
		return
	}
	
	// So sánh với snapshot trước đó
	prev := pm.stats.history[len(pm.stats.history)-1]
	timeDiff := current.Timestamp.Sub(prev.Timestamp).Seconds()
	
	if timeDiff > 0 {
		current.TxMsgsRate = float64(current.TxMsgs-prev.TxMsgs) / timeDiff
		current.TxBytesRate = float64(current.TxBytes-prev.TxBytes) / timeDiff
	}
}

// checkAlerts kiểm tra và tạo alerts
func (pm *ProducerMonitor) checkAlerts(snapshot StatSnapshot) {
	pm.alertsMutex.Lock()
	defer pm.alertsMutex.Unlock()
	
	// Alert: Queue usage cao
	if snapshot.QueueUsagePct > 90 {
		pm.addAlert(Alert{
			Timestamp: time.Now(),
			Level:     "CRITICAL",
			Message:   fmt.Sprintf("Queue usage critical: %.2f%%", snapshot.QueueUsagePct),
			Value:     snapshot.QueueUsagePct,
		})
	} else if snapshot.QueueUsagePct > 70 {
		pm.addAlert(Alert{
			Timestamp: time.Now(),
			Level:     "WARNING",
			Message:   fmt.Sprintf("Queue usage high: %.2f%%", snapshot.QueueUsagePct),
			Value:     snapshot.QueueUsagePct,
		})
	}
	
	// Alert: OutQueue cao (messages chưa được broker ack)
	if snapshot.OutQueueCount > 50000 {
		pm.addAlert(Alert{
			Timestamp: time.Now(),
			Level:     "WARNING",
			Message:   fmt.Sprintf("OutQueue high: %d messages waiting for broker", snapshot.OutQueueCount),
			Value:     float64(snapshot.OutQueueCount),
		})
	}
	
	// Alert: Error rate cao
	if len(pm.stats.history) >= 2 {
		prev := pm.stats.history[len(pm.stats.history)-1]
		errors := snapshot.TxErrors - prev.TxErrors
		total := snapshot.TxMsgs - prev.TxMsgs
		
		if total > 100 {
			errorRate := float64(errors) / float64(total) * 100
			if errorRate > 5 {
				pm.addAlert(Alert{
					Timestamp: time.Now(),
					Level:     "WARNING",
					Message:   fmt.Sprintf("Error rate high: %.2f%%", errorRate),
					Value:     errorRate,
				})
			}
		}
	}
}

func (pm *ProducerMonitor) addAlert(alert Alert) {
	pm.alerts = append(pm.alerts, alert)
	
	// Giữ chỉ 100 alerts gần nhất
	if len(pm.alerts) > 100 {
		pm.alerts = pm.alerts[len(pm.alerts)-100:]
	}
	
	// Log alert
	emoji := "ℹ️"
	if alert.Level == "WARNING" {
		emoji = "⚠️"
	} else if alert.Level == "CRITICAL" {
		emoji = "🚨"
	}
	
	pm.logger.Printf("%s [%s] %s", emoji, alert.Level, alert.Message)
}

// logSummary log summary metrics
func (pm *ProducerMonitor) logSummary(snapshot StatSnapshot) {
	pm.logger.Printf(
		"📊 Queue: %d/%d (%.1f%%) | TxRate: %.0f msg/s (%.2f MB/s) | OutQueue: %d | Errors: %d",
		snapshot.MsgCount,
		snapshot.QueueCapacity,
		snapshot.QueueUsagePct,
		snapshot.TxMsgsRate,
		snapshot.TxBytesRate/1024/1024,
		snapshot.OutQueueCount,
		snapshot.TxErrors,
	)
}

// GetCurrentStats trả về stats hiện tại
func (pm *ProducerMonitor) GetCurrentStats() StatSnapshot {
	pm.stats.mu.RLock()
	defer pm.stats.mu.RUnlock()
	return pm.stats.current
}

// GetHistory trả về history
func (pm *ProducerMonitor) GetHistory() []StatSnapshot {
	pm.stats.mu.RLock()
	defer pm.stats.mu.RUnlock()
	
	history := make([]StatSnapshot, len(pm.stats.history))
	copy(history, pm.stats.history)
	return history
}

// GetRecentAlerts trả về alerts gần đây
func (pm *ProducerMonitor) GetRecentAlerts(count int) []Alert {
	pm.alertsMutex.RLock()
	defer pm.alertsMutex.RUnlock()
	
	if count > len(pm.alerts) {
		count = len(pm.alerts)
	}
	
	alerts := make([]Alert, count)
	copy(alerts, pm.alerts[len(pm.alerts)-count:])
	return alerts
}

// PrintDetailedReport in báo cáo chi tiết
func (pm *ProducerMonitor) PrintDetailedReport() {
	pm.stats.mu.RLock()
	defer pm.stats.mu.RUnlock()
	
	snapshot := pm.stats.current
	
	fmt.Println("\n" + "="*80)
	fmt.Println("📊 KAFKA PRODUCER DETAILED REPORT")
	fmt.Println("="*80)
	fmt.Printf("Timestamp: %s\n\n", snapshot.Timestamp.Format("2006-01-02 15:04:05"))
	
	// Internal Queue Status
	fmt.Println("🗄️  INTERNAL QUEUE STATUS:")
	fmt.Printf("  Messages in queue:    %d / %d (%.2f%%)\n", 
		snapshot.MsgCount, snapshot.QueueCapacity, snapshot.QueueUsagePct)
	fmt.Printf("  Queue size (bytes):   %s\n", formatBytes(snapshot.MsgSize))
	fmt.Printf("  Queue capacity:       %d messages\n", snapshot.QueueCapacity)
	
	// Queue health indicator
	queueHealth := "🟢 HEALTHY"
	if snapshot.QueueUsagePct > 90 {
		queueHealth = "🔴 CRITICAL"
	} else if snapshot.QueueUsagePct > 70 {
		queueHealth = "🟡 WARNING"
	}
	fmt.Printf("  Status:               %s\n\n", queueHealth)
	
	// Producer Performance
	fmt.Println("📤 PRODUCER PERFORMANCE:")
	fmt.Printf("  Messages sent:        %d\n", snapshot.TxMsgs)
	fmt.Printf("  Bytes sent:           %s\n", formatBytes(snapshot.TxBytes))
	fmt.Printf("  Errors:               %d\n", snapshot.TxErrors)
	fmt.Printf("  Send rate:            %.0f msg/s\n", snapshot.TxMsgsRate)
	fmt.Printf("  Throughput:           %s/s\n\n", formatBytes(int64(snapshot.TxBytesRate)))
	
	// OutQueue (waiting for broker ack)
	fmt.Println("⏳ MESSAGES WAITING FOR BROKER ACK:")
	fmt.Printf("  OutQueue count:       %d messages\n", snapshot.OutQueueCount)
	fmt.Printf("  OutQueue bytes:       %s\n\n", formatBytes(snapshot.OutQueueBytes))
	
	// Cluster info
	fmt.Println("🌐 CLUSTER INFO:")
	fmt.Printf("  Connected brokers:    %d\n", snapshot.BrokerCount)
	fmt.Printf("  Topics:               %d\n", snapshot.TopicCount)
	fmt.Printf("  Partitions:           %d\n\n", snapshot.PartitionCount)
	
	// Recent alerts
	pm.alertsMutex.RLock()
	recentAlerts := pm.alerts
	if len(recentAlerts) > 5 {
		recentAlerts = recentAlerts[len(recentAlerts)-5:]
	}
	pm.alertsMutex.RUnlock()
	
	if len(recentAlerts) > 0 {
		fmt.Println("🚨 RECENT ALERTS:")
		for _, alert := range recentAlerts {
			emoji := "ℹ️"
			if alert.Level == "WARNING" {
				emoji = "⚠️"
			} else if alert.Level == "CRITICAL" {
				emoji = "🚨"
			}
			fmt.Printf("  %s [%s] %s - %s\n", 
				emoji, 
				alert.Level, 
				alert.Timestamp.Format("15:04:05"),
				alert.Message)
		}
		fmt.Println()
	}
	
	// Performance trends
	if len(pm.stats.history) > 10 {
		fmt.Println("📈 PERFORMANCE TRENDS (last 10 samples):")
		
		// Queue usage trend
		queueUsages := make([]float64, 0, 10)
		for i := len(pm.stats.history) - 10; i < len(pm.stats.history); i++ {
			queueUsages = append(queueUsages, pm.stats.history[i].QueueUsagePct)
		}
		fmt.Printf("  Queue usage:          %s\n", formatTrend(queueUsages))
		
		// Throughput trend
		throughputs := make([]float64, 0, 10)
		for i := len(pm.stats.history) - 10; i < len(pm.stats.history); i++ {
			throughputs = append(throughputs, pm.stats.history[i].TxMsgsRate)
		}
		fmt.Printf("  Throughput:           %s\n", formatTrend(throughputs))
	}
	
	fmt.Println("="*80 + "\n")
}

// PrintSimpleDashboard in dashboard đơn giản
func (pm *ProducerMonitor) PrintSimpleDashboard() {
	snapshot := pm.GetCurrentStats()
	
	fmt.Print("\033[H\033[2J") // Clear screen
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║           KAFKA PRODUCER MONITOR DASHBOARD                     ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Printf("Time: %s\n\n", time.Now().Format("15:04:05"))
	
	// Queue visualization
	fmt.Println("Internal Queue:")
	queueBar := createProgressBar(snapshot.QueueUsagePct, 50)
	fmt.Printf("%s %.1f%%\n", queueBar, snapshot.QueueUsagePct)
	fmt.Printf("%d / %d messages\n\n", snapshot.MsgCount, snapshot.QueueCapacity)
	
	// Metrics grid
	fmt.Printf("┌─────────────────────┬─────────────────────┐\n")
	fmt.Printf("│ Send Rate           │ %-19.0f │\n", snapshot.TxMsgsRate)
	fmt.Printf("│ Throughput          │ %-19s │\n", formatBytes(int64(snapshot.TxBytesRate))+"/s")
	fmt.Printf("│ OutQueue            │ %-19d │\n", snapshot.OutQueueCount)
	fmt.Printf("│ Errors              │ %-19d │\n", snapshot.TxErrors)
	fmt.Printf("└─────────────────────┴─────────────────────┘\n")
}

// Stop dừng monitoring
func (pm *ProducerMonitor) Stop() {
	pm.cancel()
	pm.wg.Wait()
	pm.logger.Println("✅ Producer monitoring stopped")
}

// Helper functions
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatTrend(values []float64) string {
	if len(values) < 2 {
		return "N/A"
	}
	
	trend := ""
	for i := 1; i < len(values); i++ {
		if values[i] > values[i-1]*1.1 {
			trend += "↗"
		} else if values[i] < values[i-1]*0.9 {
			trend += "↘"
		} else {
			trend += "→"
		}
	}
	
	min := values[0]
	max := values[0]
	sum := values[0]
	for i := 1; i < len(values); i++ {
		if values[i] < min {
			min = values[i]
		}
		if values[i] > max {
			max = values[i]
		}
		sum += values[i]
	}
	avg := sum / float64(len(values))
	
	return fmt.Sprintf("%s (avg: %.1f, min: %.1f, max: %.1f)", trend, avg, min, max)
}

func createProgressBar(percentage float64, width int) string {
	filled := int(percentage / 100 * float64(width))
	if filled > width {
		filled = width
	}
	
	bar := "["
	for i := 0; i < width; i++ {
		if i < filled {
			if percentage > 90 {
				bar += "█" // Red zone
			} else if percentage > 70 {
				bar += "▓" // Yellow zone
			} else {
				bar += "▒" // Green zone
			}
		} else {
			bar += "░"
		}
	}
	bar += "]"
	return bar
}

// ==================== EXAMPLE USAGE ====================

func main() {
	// Tạo producer
	config := &kafka.ConfigMap{
		"bootstrap.servers":            "localhost:9092",
		"queue.buffering.max.messages": 100000,
		"queue.buffering.max.kbytes":   1048576,
		"linger.ms":                    5,
		"compression.type":             "lz4",
		"acks":                         "1",
		"statistics.interval.ms":       1000, // QUAN TRỌNG: Enable statistics
	}
	
	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	
	// Tạo monitor
	monitor := NewProducerMonitor(producer, 2*time.Second)
	monitor.Start()
	defer monitor.Stop()
	
	// Start detailed report every 10 seconds
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			monitor.PrintDetailedReport()
		}
	}()
	
	// Start simple dashboard updates
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			monitor.PrintSimpleDashboard()
		}
	}()
	
	// Handle delivery reports
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()
	
	// Simulate sending messages
	topic := "test-topic"
	log.Println("🚀 Starting to send messages...")
	
	for i := 0; i < 100000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("message-%d-with-some-data", i))
		
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            key,
			Value:          value,
		}
		
		producer.Produce(msg, nil)
		
		// Variable rate để test queue behavior
		if i%1000 == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	
	log.Println("✅ All messages sent, waiting for delivery...")
	producer.Flush(30 * 1000)
	
	// Print final report
	monitor.PrintDetailedReport()
}
