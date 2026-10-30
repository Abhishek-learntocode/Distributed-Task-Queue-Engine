# Distributed Task Queue Management

## Prerequisites

```bash
pip install grpcio grpcio-tools numpy
```

## Generating gRPC Stubs

Before running the system, generate the `task_queue_pb2.py` and `task_queue_pb2_grpc.py` files:

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. task_queue.proto
```

*(Note: These generated files are not included in the submission as per the Section 2.7 deliverables.)*

## Running the System

### Step 1: Start the Server

```bash
python server.py localhost:50051
```

### Step 2: Register Workers

Each worker declares its capability (CPU, IO, or BOTH):

```bash
python worker.py localhost:50051 CPU
python worker.py localhost:50051 IO
python worker.py localhost:50051 BOTH
```

### Step 3: Submit Tasks via the Job Client

```bash
python client.py localhost:50051
```

Available commands:
- `submit CPU 100` — Submit a CPU task (100x100 matrix multiplication)
- `submit IO 5` — Submit an IO task (sleep 5 seconds)
- `status T1` — Check the status of task T1
- `poll T1` — Poll T1 until completion
- `exit` — Quit

### Example Session

```
> submit CPU 100
[Client] Task submitted. ID: T1 (CPU, payload=100)
> submit IO 5
[Client] Task submitted. ID: T2 (IO, payload=5)
> status T1
[Client] Task T1: Processing (Worker W1)
> status T2
[Client] Task T2: Processing (Worker W2)
> status T1
[Client] Task T1: Completed
```

### Worker Failure Test

Kill a worker with Ctrl+C while it processes a task. After 15 seconds
(3 missed heartbeats), the server detects the failure, marks the task
as Failed, re-queues it at the front, and assigns it to the next
available worker.

## Files

- `task_queue.proto` — Service and message definitions
- `server.py` — Queue logic, failure detection, and all RPCs
- `worker.py` — Worker logic, heartbeats, and task processing
- `client.py` — Task submission and status polling
