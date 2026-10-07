package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIngestHandler_Success(t *testing.T) {
	payload := SensorPayload{
		SensorID:  "TEMP-01",
		Type:      "Temperature",
		Value:     42.5,
		Timestamp: time.Now(),
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/api/telemetry", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	ingestHandler(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("Expected status 202 Accepted, got %d", rec.Code)
	}
}

func TestIngestHandler_InvalidJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/telemetry", bytes.NewReader([]byte("{invalid-json")))
	rec := httptest.NewRecorder()

	ingestHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("Expected status 400 Bad Request, got %d", rec.Code)
	}
}

func TestStatsHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rec := httptest.NewRecorder()

	statsHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK, got %d", rec.Code)
	}

	var stats StatsResponse
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("Failed to decode stats response: %v", err)
	}

	if stats.Status != "online" {
		t.Fatalf("Expected status 'online', got %s", stats.Status)
	}
}
