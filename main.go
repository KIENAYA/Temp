package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ticklen      = 50
	testDuration = 5 * time.Second
	sendingRate  = 5000
)

var Send uint64

// PacketInfo lưu thông tin về packet được gửi
type PacketInfo struct {
	ID       uint64
	SendTime time.Time
	Received bool
	RTT      time.Duration
}

// RTTMeasurer quản lý việc đo RTT
type RTTMeasurer struct {
	packets         sync.Map // map[uint64]*PacketInfo
	packetCounter   uint64
	sentCounter     int64
	receivedCounter int64
	conn            *net.UDPConn
	mu              sync.RWMutex
	running         bool
}

// RTTStats chứa thống kê RTT
type RTTStats struct {
	TotalSent     int64
	TotalReceived int64
	PacketLoss    float64
	MinRTT        time.Duration
	MaxRTT        time.Duration
	AvgRTT        time.Duration
	StdDevRTT     time.Duration
	P50RTT        time.Duration // Median
	P90RTT        time.Duration
	P95RTT        time.Duration
	P99RTT        time.Duration
}

func (s *RTTStats) String() string {
	if s.TotalReceived == 0 {
		return fmt.Sprintf("Packets sent: %d, No responses received (Loss: 100%%)", s.TotalSent)
	}

	return fmt.Sprintf(`RTT Statistics:
  Packets sent: %d
  Packets received: %d
  Packet loss: %.2f%%
  Min RTT: %v
  Max RTT: %v
  Avg RTT: %v
  Std Dev: %v
  Median (P50): %v
  P90: %v
  P95: %v
  P99: %v`,
		s.TotalSent, s.TotalReceived, s.PacketLoss,
		s.MinRTT, s.MaxRTT, s.AvgRTT, s.StdDevRTT,
		s.P50RTT, s.P90RTT, s.P95RTT, s.P99RTT)
}

// NewRTTMeasurer tạo RTT measurer mới
func NewRTTMeasurer(conn *net.UDPConn) *RTTMeasurer {
	measurer := &RTTMeasurer{
		conn:    conn,
		running: true,
	}

	// Khởi động receiver goroutine
	go measurer.startReceiver()

	return measurer
}

// startReceiver khởi động goroutine để nhận response packets
func (r *RTTMeasurer) startReceiver() {
	buffer := make([]byte, 1024)

	for {
		r.mu.RLock()
		if !r.running {
			r.mu.RUnlock()
			break
		}
		r.mu.RUnlock()

		// Set read timeout ngắn để có thể check running status
		r.conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))

		n, err := r.conn.Read(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			continue
		}

		receiveTime := time.Now()

		if n < 8 {
			continue
		}

		// Extract packet ID từ response
		packetID := binary.BigEndian.Uint64(buffer[:8])

		// Tìm packet info và update RTT
		if packetInfo, ok := r.packets.Load(packetID); ok {
			pInfo := packetInfo.(*PacketInfo)
			if !pInfo.Received {
				pInfo.Received = true
				pInfo.RTT = receiveTime.Sub(pInfo.SendTime)
				atomic.AddInt64(&r.receivedCounter, 1)
			}
		}
	}
}

// TrackPacket track một packet được gửi
func (r *RTTMeasurer) TrackPacket(packet []byte) []byte {
	packetID := atomic.AddUint64(&r.packetCounter, 1)
	sendTime := time.Now()

	// Tạo packet mới với ID ở đầu
	newPacket := make([]byte, 8+len(packet))
	binary.BigEndian.PutUint64(newPacket[:8], packetID)
	copy(newPacket[8:], packet)

	// Lưu packet info
	packetInfo := &PacketInfo{
		ID:       packetID,
		SendTime: sendTime,
		Received: false,
	}
	r.packets.Store(packetID, packetInfo)

	atomic.AddInt64(&r.sentCounter, 1)

	return newPacket
}

// GetStatistics tính toán và trả về thống kê RTT
func (r *RTTMeasurer) GetStatistics() *RTTStats {
	var rtts []time.Duration
	var minRTT, maxRTT time.Duration
	var totalRTT time.Duration
	receivedCount := 0

	// Thu thập RTT data
	r.packets.Range(func(key, value interface{}) bool {
		pInfo := value.(*PacketInfo)
		if pInfo.Received {
			rtt := pInfo.RTT
			rtts = append(rtts, rtt)
			totalRTT += rtt
			receivedCount++

			if receivedCount == 1 || rtt < minRTT {
				minRTT = rtt
			}
			if receivedCount == 1 || rtt > maxRTT {
				maxRTT = rtt
			}
		}
		return true
	})

	sentCount := atomic.LoadInt64(&r.sentCounter)

	if receivedCount == 0 {
		return &RTTStats{
			TotalSent:     sentCount,
			TotalReceived: 0,
			PacketLoss:    100.0,
		}
	}

	// Sắp xếp RTTs để tính percentiles
	sort.Slice(rtts, func(i, j int) bool {
		return rtts[i] < rtts[j]
	})

	avgRTT := totalRTT / time.Duration(receivedCount)

	// Tính percentiles
	p50 := rtts[len(rtts)/2]
	p90 := rtts[int(float64(len(rtts))*0.9)]
	p95 := rtts[int(float64(len(rtts))*0.95)]
	p99 := rtts[int(float64(len(rtts))*0.99)]

	// Tính standard deviation
	var variance float64
	for _, rtt := range rtts {
		diff := float64(rtt - avgRTT)
		variance += diff * diff
	}
	variance /= float64(len(rtts))
	stdDev := time.Duration(math.Sqrt(variance))

	packetLoss := float64(sentCount-int64(receivedCount)) / float64(sentCount) * 100

	return &RTTStats{
		TotalSent:     sentCount,
		TotalReceived: int64(receivedCount),
		PacketLoss:    packetLoss,
		MinRTT:        minRTT,
		MaxRTT:        maxRTT,
		AvgRTT:        avgRTT,
		StdDevRTT:     stdDev,
		P50RTT:        p50,
		P90RTT:        p90,
		P95RTT:        p95,
		P99RTT:        p99,
	}
}

// Stop dừng RTT measurer
func (r *RTTMeasurer) Stop() {
	r.mu.Lock()
	r.running = false
	r.mu.Unlock()
}

// PrintRealTimeStats in thống kê real-time
func (r *RTTMeasurer) PrintRealTimeStats(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		r.mu.RLock()
		if !r.running {
			r.mu.RUnlock()
			break
		}
		r.mu.RUnlock()

		select {
		case <-ticker.C:
			sent := atomic.LoadInt64(&r.sentCounter)
			received := atomic.LoadInt64(&r.receivedCounter)
			loss := float64(sent-received) / float64(sent) * 100
			if sent == 0 {
				loss = 0
			}
			log.Printf("\rReal-time: Sent: %d, Received: %d, Loss: %.1f%%", sent, received, loss)
		}
	}
}

var wg sync.WaitGroup
var rttMeasurer *RTTMeasurer

func sendUDP(conn *net.UDPConn) {
	defer wg.Done()
	atomic.AddUint64(&Send, 1)

	// Tạo packet gốc
	packet := generateMessage()

	// Track packet và thêm ID vào đầu
	trackedPacket := rttMeasurer.TrackPacket(packet)

	// Gửi packet
	conn.Write(trackedPacket)
}

func generateMessage() []byte {
	random10 := rand.Intn(1 << 10)
	value := random10 << 6

	message := []byte{
		0x48,
		0x04,
		byte((value >> 8) & 0xFF),
		byte(value & 0xFF),
		0x16,
	}

	return message
}

// SimpleUDPEchoServer tạo echo server để test
func SimpleUDPEchoServer(addr string) error {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	fmt.Printf("UDP Echo Server listening on %s\n", addr)

	buffer := make([]byte, 1024)
	for {
		n, clientAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			continue
		}

		// Echo back the received data
		go func(data []byte, addr *net.UDPAddr) {
			conn.WriteToUDP(data, addr)
		}(buffer[:n], clientAddr)
	}
}

func main() {
	// Khởi động echo server cho test (chạy trong background)
	go func() {
		if err := SimpleUDPEchoServer("127.0.0.1:2123"); err != nil {
			fmt.Printf("Echo server error: %v\n", err)
		}
	}()

	// Đợi server khởi động
	time.Sleep(1 * time.Second)

	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:2123")
	if err != nil {
		panic(err)
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Khởi tạo RTT measurer
	rttMeasurer = NewRTTMeasurer(conn)
	defer rttMeasurer.Stop()

	// Bắt đầu real-time monitoring
	go rttMeasurer.PrintRealTimeStats(1 * time.Second)

	fmt.Printf("Starting UDP test - Sending %d packets/second for %v\n", sendingRate, testDuration)

	rand.Seed(time.Now().UnixNano())
	tickpersec := 1000 / ticklen
	tick := time.NewTicker(ticklen * time.Millisecond)
	defer tick.Stop()
	stop := time.After(testDuration)

	startTime := time.Now()

	for {
		select {
		case <-tick.C:
			for i := 0; i < (sendingRate / tickpersec); i++ {
				wg.Add(1)
				go sendUDP(conn)
			}
		case <-stop:
			fmt.Println("\nTest completed! Waiting for remaining responses...")
			wg.Wait()

			// Đợi thêm một chút để nhận response cuối cùng
			time.Sleep(2 * time.Second)

			// Dừng real-time monitoring
			rttMeasurer.Stop()

			// Lấy và in thống kê cuối cùng
			stats := rttMeasurer.GetStatistics()
			actualDuration := time.Since(startTime)
			actualRate := float64(stats.TotalSent) / actualDuration.Seconds()

			fmt.Printf("TEST RESULTS\n")

			fmt.Printf("Actual test duration: %v\n", actualDuration)
			fmt.Printf("Actual sending rate: %.0f packets/second\n", actualRate)
			fmt.Printf("\n%s\n", stats.String())

			return
		}
	}
}
