package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"industrial-telemetry-engine/pkg/compressor"
)

type SensorPayload struct {
	SensorID  string    `json:"sensor_id"`
	Type      string    `json:"type"`
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

type StatsResponse struct {
	Status             string  `json:"status"`
	TotalIngested      int64   `json:"total_ingested"`
	CriticalAlerts     int64   `json:"critical_alerts"`
	QueueDepth         int     `json:"queue_depth"`
	RawBits            int64   `json:"raw_bits"`
	CompressedBits     int64   `json:"compressed_bits"`
	CompressionRatio   float64 `json:"compression_ratio"`
}

var (
	dataChannel        = make(chan SensorPayload, 10000)
	totalIngestedCount int64
	alertCount         int64
	rawBitsCount       int64
	compressedBitsCount int64

	compressorMap   = make(map[string]*compressor.GorillaCompressor)
	compressorMutex sync.Mutex
)

func getOrCreateCompressor(sensorID string) *compressor.GorillaCompressor {
	compressorMutex.Lock()
	defer compressorMutex.Unlock()
	c, exists := compressorMap[sensorID]
	if !exists {
		c = compressor.NewGorillaCompressor()
		compressorMap[sensorID] = c
	}
	return c
}

func telemetryWorker(id int) {
	for payload := range dataChannel {
		// 1. Check critical threshold
		if payload.Type == "Temperature" && payload.Value > 85.0 {
			atomic.AddInt64(&alertCount, 1)
			fmt.Printf("[ALERT Worker %d] Critical Temp on %s: %.2f C\n", id, payload.SensorID, payload.Value)
		}

		// 2. Stream through Gorilla Time-Series Compressor
		c := getOrCreateCompressor(payload.SensorID)
		bits, _ := c.CompressPair(payload.Timestamp.UnixMilli(), payload.Value)

		atomic.AddInt64(&rawBitsCount, 128)
		atomic.AddInt64(&compressedBitsCount, int64(bits))
	}
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload SensorPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid JSON payload: "+err.Error(), http.StatusBadRequest)
		return
	}

	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now()
	}

	// Non-blocking channel push with backpressure
	select {
	case dataChannel <- payload:
		atomic.AddInt64(&totalIngestedCount, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"status":"queued"}`))
	default:
		// Queue full - return backpressure 429
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":"queue_full", "status":"rejected"}`))
	}
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	raw := atomic.LoadInt64(&rawBitsCount)
	comp := atomic.LoadInt64(&compressedBitsCount)
	var ratio float64
	if comp > 0 {
		ratio = float64(raw) / float64(comp)
	}

	resp := StatsResponse{
		Status:           "online",
		TotalIngested:    atomic.LoadInt64(&totalIngestedCount),
		CriticalAlerts:   atomic.LoadInt64(&alertCount),
		QueueDepth:       len(dataChannel),
		RawBits:          raw,
		CompressedBits:   comp,
		CompressionRatio: ratio,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func main() {
	for i := 1; i <= 8; i++ {
		go telemetryWorker(i)
	}

	http.HandleFunc("/api/telemetry", ingestHandler)
	http.HandleFunc("/api/stats", statsHandler)

	log.Println("Industrial Telemetry Ingestion Engine running on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
