"""
Task Queue Worker Client

This program represents a single worker in the distributed task queue.
A worker's lifecycle is:
  1. Register with the central server, declaring its capability
  2. Start sending heartbeats every 5 seconds in a background thread
  3. Open a streaming connection to receive tasks from the server
  4. Process each received task (CPU -> matrix multiply, IO -> sleep)
  5. Report task completion or failure back to the server

The heartbeat mechanism is critical: if the server doesn't hear from
us for 15 seconds, it assumes we crashed and reassigns our task.

Usage:
    python worker.py <server_address> <CPU|IO|BOTH>
    Example: python worker.py localhost:50051 CPU
"""

import sys
import time
import threading

import grpc
import numpy as np

import task_queue_pb2
import task_queue_pb2_grpc


def execute_cpu_workload(matrix_size_str):
    """
    Simulate a CPU-intensive operation by performing matrix multiplication.

    The assignment specifies: CPU task payload is the dimension N
    for an NxN matrix multiplication. We use numpy because it
    actually consumes CPU cycles (unlike a simple sleep), creating
    realistic computational load.
    """
    # FIX: Proactive payload validation to return a clean FAILED error immediately
    try:
        n = int(matrix_size_str)
        if n <= 0:
            raise ValueError
    except ValueError:
        raise ValueError(f"Invalid CPU payload '{matrix_size_str}': dimension must be a positive integer > 0")

    print(f"  Performing {n}x{n} matrix multiplication...")

    # Generate two random matrices and multiply them.
    # We don't need the result — the point is to create CPU load.
    matrix_a = np.random.rand(n, n)
    matrix_b = np.random.rand(n, n)
    _result = np.dot(matrix_a, matrix_b)


def execute_io_workload(sleep_duration_str):
    """
    Simulate an IO-bound operation by sleeping.

    The assignment specifies: IO task payload is the number of
    seconds to sleep, simulating a network call or database query.
    """
    # FIX: Proactive payload validation for sleep numbers
    try:
        seconds = int(sleep_duration_str)
        if seconds < 0:
            raise ValueError
    except ValueError:
        raise ValueError(f"Invalid IO payload '{sleep_duration_str}': duration must be a non-negative integer")

    print(f"  Simulating IO operation (sleeping {seconds} seconds)...")
    time.sleep(seconds)


def heartbeat_sender(rpc_stub, my_worker_id, shutdown_flag):
    """
    Background thread that sends heartbeats to the server every 5 seconds.

    The assignment requires heartbeats every 5 seconds. If we miss
    3 consecutive heartbeats (15 seconds), the server marks us as dead.
    We use an Event-based wait instead of time.sleep so that when the
    main thread sets the shutdown flag, we stop immediately rather
    than waiting for the next sleep cycle to finish.
    """
    while not shutdown_flag.is_set():
        try:
            rpc_stub.SendHeartbeat(
                task_queue_pb2.HeartbeatMessage(worker_id=my_worker_id)
            )
        except grpc.RpcError:
            print(f"[Worker {my_worker_id}] Failed to send heartbeat (server unreachable)")
            break

        # Wait 5 seconds or until shutdown is requested
        shutdown_flag.wait(timeout=5)


def run_worker(server_addr, capability_str):
    """
    Main worker execution: register, start heartbeats, process tasks.
    """
    # Map the command-line argument to the protobuf enum
    capability_mapping = {
        "CPU": task_queue_pb2.CPU_WORKER,
        "IO": task_queue_pb2.IO_WORKER,
        "BOTH": task_queue_pb2.BOTH_CAPABLE,
    }

    capability_upper = capability_str.upper()
    if capability_upper not in capability_mapping:
        print(f"Error: '{capability_str}' is not a valid worker type. Use CPU, IO, or BOTH.")
        sys.exit(1)

    chosen_capability = capability_mapping[capability_upper]

    # Connect to the task queue server
    channel = grpc.insecure_channel(server_addr)
    rpc_stub = task_queue_pb2_grpc.TaskQueueServiceStub(channel)

    # Step 1: Register this worker with the server
    registration_reply = rpc_stub.RegisterWorker(
        task_queue_pb2.WorkerConfig(capability=chosen_capability)
    )
    my_worker_id = registration_reply.worker_id
    print(f"[Worker] Registered with ID: {my_worker_id} (type: {capability_upper})")

    # Step 2: Start the heartbeat background thread
    # The shutdown_flag lets us cleanly stop the heartbeat thread
    # when the main loop finishes
    shutdown_flag = threading.Event()
    hb_thread = threading.Thread(
        target=heartbeat_sender,
        args=(rpc_stub, my_worker_id, shutdown_flag),
        daemon=True
    )
    hb_thread.start()

    # Step 3: Open the task stream and process tasks as they arrive
    print(f"[Worker {my_worker_id}] Waiting for tasks...")
    try:
        # The server-side streaming RPC keeps this connection open
        # and pushes tasks to us one at a time
        task_stream = rpc_stub.RequestTask(
            task_queue_pb2.WorkerIdentifier(worker_id=my_worker_id)
        )

        for incoming_task in task_stream:
            type_label = "CPU" if incoming_task.task_type == task_queue_pb2.CPU else "IO"
            print(
                f"\n[Worker {my_worker_id}] Received task {incoming_task.task_id} "
                f"({type_label}, payload={incoming_task.payload})"
            )

            # Process the task based on its type
            try:
                if incoming_task.task_type == task_queue_pb2.CPU:
                    execute_cpu_workload(incoming_task.payload)
                else:
                    execute_io_workload(incoming_task.payload)

                # Tell the server we finished successfully
                rpc_stub.UpdateTaskStatus(task_queue_pb2.TaskResult(
                    task_id=incoming_task.task_id,
                    worker_id=my_worker_id,
                    status=task_queue_pb2.COMPLETED
                ))
                print(f"[Worker {my_worker_id}] Task {incoming_task.task_id} completed. Waiting for next task...")

            except Exception as processing_error:
                # If the task processing itself fails, report it
                print(f"[Worker {my_worker_id}] Task {incoming_task.task_id} failed: {processing_error}")
                try:
                    rpc_stub.UpdateTaskStatus(task_queue_pb2.TaskResult(
                        task_id=incoming_task.task_id,
                        worker_id=my_worker_id,
                        status=task_queue_pb2.FAILED
                    ))
                except grpc.RpcError:
                    # If we can't even report the failure, the server
                    # will eventually detect our silence via heartbeat timeout
                    pass

    except grpc.RpcError as stream_error:
        print(f"[Worker {my_worker_id}] Task stream closed: {stream_error.code()}")
    finally:
        # Clean shutdown: stop heartbeats and close the channel
        shutdown_flag.set()
        print(f"[Worker {my_worker_id}] Shutting down.")
        channel.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python worker.py <server_address> <CPU|IO|BOTH>")
        print("Example: python worker.py localhost:50051 CPU")
        sys.exit(1)

    run_worker(sys.argv[1], sys.argv[2])
