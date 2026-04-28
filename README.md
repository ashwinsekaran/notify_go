# notify

A CLI tool that reads lines from stdin, batches them, and delivers each line
as an HTTP POST to a configured URL on a fixed interval.

## Design

- **Batching** — messages are collected and flushed on every tick (default 5s)
- **Worker pool** — 10 concurrent HTTP workers drain the internal queue
- **Non-blocking** — `Notify` never stalls the caller; if the queue is full, messages are parked in a DLQ file for retry
- **DLQ** — failed or dropped messages are written to `dlq/failed.log` and replayed every 30s, and on shutdown
- **Graceful shutdown** — `SIGINT`/`SIGTERM` flush pending messages and replay DLQ before exit

## Build

```bash
go build -o notify .
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | required | URL to POST notifications to |
| `--interval` / `-i` | `5s` | How often to flush and send batched messages |
| `--workers` | `10` | Number of concurrent HTTP workers |
| `--queue-size` | `1000` | Internal message queue size |

## Quick start

**Terminal 1 — start HTTP server:**
```bash
go run ./httpserver

# optional: custom port
go run ./httpserver 9090
```

**Terminal 2 — build and run:**
```bash
go build -o notify .
```

## Scenarios

### Scenario 1 — pipe a file
```bash
# 100 messages
cat messages_100.txt | ./notify --url=http://localhost:8080 -i 2s

# 5000 messages
cat messages_5000.txt | ./notify --url=http://localhost:8080 -i 2s
```
Messages are batched and flushed every 2s. Watch Terminal 1 to see them arrive.

### Scenario 2 — interactive typing
```bash
./notify --url=http://localhost:8080 -i 5s
```
Type lines manually. Every 5s, all typed lines are sent. Press `Ctrl+C` to exit.

### Scenario 3 — graceful shutdown
```bash
./notify --url=http://localhost:8080 -i 60s
```
Type some lines, press `Ctrl+C` before 60s — pending messages are still flushed before exit.

### Scenario 4 — DLQ (queue overflow)
```bash
./notify --url=http://localhost:8080 -i 60s --workers=1 --queue-size=10
```
Type more than 10 lines quickly. Messages that overflow the queue are parked in `dlq/failed.log` and replayed automatically every 30s.

### Scenario 5 — DLQ (server down)
```bash
# start notify WITHOUT the mock server running
./notify --url=http://localhost:8080 -i 5s --workers=1 --queue-size=10
```
Type some lines. HTTP POSTs fail → messages written to `dlq/failed.log`.

Check the file:
```bash
cat dlq/failed.log
```

Then start the HTTP server:
```bash
go run ./httpserver
```
Wait 30s — DLQ worker replays all messages automatically. File is cleared after successful delivery.

### Scenario 6 — large file with overflow
```bash
# 100 messages — small queue to trigger DLQ overflow
cat messages_100.txt | ./notify --url=http://localhost:8080 -i 3s --workers=1 --queue-size=10

# 5000 messages — default queue, overflow expected
cat messages_5000.txt | ./notify --url=http://localhost:8080 -i 3s
```
Some messages overflow to DLQ and are replayed on shutdown.

