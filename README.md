# notify

A CLI tool that reads lines from stdin, batches them, and delivers each line
as an HTTP POST to a configured URL on a fixed interval.

## Design

- **Batching** — messages are collected and flushed on every tick (default 5s)
- **Worker pool** — 10 concurrent HTTP workers drain the internal queue
- **Non-blocking** — `Notify` never stalls the caller; drops messages if the queue is full and invokes the error handler
- **Graceful shutdown** — `SIGINT`/`SIGTERM` flush pending messages before exit; `Close` waits for all in-flight requests to finish

## Build

```bash
go build -o notify .
```

## Usage

```bash
# basic
echo "hello world" | ./notify --url=http://localhost:8080

# custom interval
echo "hello world" | ./notify --url=http://localhost:8080 -i 2s

# interactive — type lines, they are flushed every 5s
./notify --url=http://localhost:8080 -i 5s

# pipe a file
cat messages.txt | ./notify --url=http://localhost:8080 -i 1s
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | required | URL to POST notifications to |
| `--interval` / `-i` | `5s` | How often to flush and send batched messages |

## Testing manually

Start the mock server in one terminal:

```bash
go run ./mockserver
# optional: custom port
go run ./mockserver 9090
```

Then in another terminal:

```bash
# build the binary first
go build -o notify .

# scenario 1 — basic pipe
echo -e "hello\nworld\nfoo" | ./notify --url=http://localhost:8080 -i 2s

# scenario 2 — batch of 20, all flushed at once after interval
seq 1 20 | ./notify --url=http://localhost:8080 -i 3s

# scenario 3 — interactive typing
./notify --url=http://localhost:8080 -i 5s
# type lines manually, wait 5s, see them appear in Terminal 1
# press Ctrl+C to exit

# scenario 4 — graceful shutdown (Ctrl+C flushes pending messages)
./notify --url=http://localhost:8080 -i 60s
# type some lines, press Ctrl+C before 60s — messages still flush before exit

# run tests
go test -race -v ./...
```

Mock server prints each received message with timestamp and sequence number:

```
2026/04/17 09:00:00 mock server listening on :8080 — waiting for notifications...
[09:00:02] #1: hello
[09:00:02] #2: world
[09:00:02] #3: foo
```

## Running tests

```bash
# run all tests
go test ./...

# with race detector (recommended)
go test -race ./...

# verbose output
go test -v ./...
```

### Test coverage

| Test | What it verifies |
|------|-----------------|
| `TestNotify_MessageDelivered` | single message is delivered successfully |
| `TestNotify_MultipleMessages` | all messages in a batch are delivered |
| `TestNotify_QueueFull` | error handler is called when queue is full |
| `TestClose_DrainsInFlightMessages` | `Close` waits for all in-flight requests |
| `TestNotify_ErrorHandler` | error handler is called on HTTP failure |
