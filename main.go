package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type SensorPayload struct {
	SensorID  string    `json:"sensor_id"`
	Type      string    `json:"type"`
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

var (
	dataChannel = make(chan SensorPayload, 10000)
	alertCount  int64
	mu          sync.Mutex
)

func telemetryWorker(id int) {
	for payload := range dataChannel {
		if payload.Type == "Temperature" && payload.Value > 85.0 {
			mu.Lock()
			alertCount++
			mu.Unlock()
			fmt.Printf("[ALERT Worker %d] Critical Temp on %s: %.2f C\n", id, payload.SensorID, payload.Value)
		}
	}
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload SensorPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	payload.Timestamp = time.Now()
	dataChannel <- payload
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status":"queued"}`))
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	count := alertCount
	mu.Unlock()
	response := fmt.Sprintf(`{"status":"online", "critical_alerts":%d, "queue_depth":%d}`, count, len(dataChannel))
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(response))
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
