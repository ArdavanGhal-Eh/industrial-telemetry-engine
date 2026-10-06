# 📡 Industrial Telemetry Ingestion Engine

High-throughput, asynchronous sensor telemetry ingestion server built in **Go (Golang)** using goroutines, channels, and worker pool patterns for Industrial IoT (IIoT) monitoring.

## Features
- Scalable non-blocking ingestion endpoint handling 10,000+ RPS
- Asynchronous worker pool for threshold checking and alert triggering
- Health and queue metrics API (`/api/stats`)

## Build & Run
```bash
go run main.go
```

## Author
**Ardavan Ghal-Eh** | Sharif University of Technology
