"""
Task Queue Job Client

This client allows users to submit tasks to the distributed task queue
and monitor their progress. It connects to the central server and
provides a simple command-line interface.

The assignment specifies that the job client should:
  - Submit tasks with a type (CPU or IO) and a payload
  - Poll task status periodically and print when status changes

Usage:
    python client.py <server_address>
    Example: python client.py localhost:50051
"""

import sys
import time

import grpc
import task_queue_pb2
import task_queue_pb2_grpc


# Human-readable names for task lifecycle states
LIFECYCLE_LABELS = {
    task_queue_pb2.QUEUED: "Queued",
    task_queue_pb2.PROCESSING: "Processing",
    task_queue_pb2.COMPLETED: "Completed",
    task_queue_pb2.FAILED: "Failed",
}


def handle_submit(rpc_stub, task_type_str, payload):
    """
    Submit a new task to the server.

    Maps the user's text input (CPU/IO) to the protobuf enum
    and sends the request. The server returns a unique task ID
    that can be used to track the task later.
    """
    type_mapping = {
        "CPU": task_queue_pb2.CPU,
        "IO": task_queue_pb2.IO
    }

    upper_type = task_type_str.upper()
    if upper_type not in type_mapping:
        print(f"[Client] Invalid task type '{task_type_str}'. Use CPU or IO.")
        return

    reply = rpc_stub.SubmitTask(task_queue_pb2.TaskRequest(
        task_type=type_mapping[upper_type],
        payload=payload
    ))
    print(f"[Client] Task submitted. ID: {reply.task_id} ({upper_type}, payload={payload})")


def handle_status(rpc_stub, task_id):
    """
    Query the current status of a task and display it.
    """
    try:
        reply = rpc_stub.GetTaskStatus(
            task_queue_pb2.TaskStatusRequest(task_id=task_id)
        )
        status_text = LIFECYCLE_LABELS.get(reply.status, "Unknown")

        # Show which worker is handling the task, if any
        worker_info = ""
        if reply.assigned_worker:
            worker_info = f" (Worker {reply.assigned_worker})"

        print(f"[Client] Task {task_id}: {status_text}{worker_info}")
    except grpc.RpcError as rpc_err:
        print(f"[Client] Error checking status: {rpc_err.details()}")


def handle_poll(rpc_stub, task_id):
    """
    Continuously poll a task's status until it reaches a terminal state.

    The assignment says the client should "poll periodically and print
    the status whenever it changes." We poll every 2 seconds and only
    print when the status actually changes, avoiding redundant output.
    """
    print(f"[Client] Polling task {task_id} until completion (Ctrl+C to stop)...")
    previous_status = None
    failed_seen = False
    failure_count = 0

    try:
        while True:
            reply = rpc_stub.GetTaskStatus(
                task_queue_pb2.TaskStatusRequest(task_id=task_id)
            )
            current_status = reply.status

            # Only print when the status has changed
            if current_status != previous_status:
                status_text = LIFECYCLE_LABELS.get(current_status, "Unknown")
                worker_info = ""
                if reply.assigned_worker:
                    worker_info = f" (Worker {reply.assigned_worker})"
                print(f"[Client] Task {task_id}: {status_text}{worker_info}")
                previous_status = current_status

            if current_status == task_queue_pb2.FAILED:
                # Prevent counting the same failure multiple times during the 8s visibility window
                if not failed_seen:
                    failed_seen = True
                    failure_count += 1
                    if failure_count >= 3:
                        print(f"[Client] Task {task_id} FAILED PERMANENTLY after {failure_count} attempts. Check worker health.")
                        break
                    print(f"[Client] Task {task_id} failed — will be re-queued automatically. (failure {failure_count}/3)")

            # Only stop on COMPLETED (successful finish)
            if current_status == task_queue_pb2.COMPLETED:
                break

            # If we saw FAILED and now see QUEUED, inform user task was re-queued
            if failed_seen and current_status == task_queue_pb2.QUEUED:
                print(f"[Client] Task {task_id} re-queued after worker failure. "
                      f"Waiting for reassignment...")
                failed_seen = False  # Reset to properly track any subsequent failures

            time.sleep(2)
    except KeyboardInterrupt:
        print("\n[Client] Polling stopped.")


def run_job_client(server_addr):
    """
    Interactive command loop for submitting and monitoring tasks.
    """
    channel = grpc.insecure_channel(server_addr)
    rpc_stub = task_queue_pb2_grpc.TaskQueueServiceStub(channel)
    print(f"[Client] Connected to task queue server at {server_addr}")

    print("\nAvailable commands:")
    print("  submit <CPU|IO> <payload>  -- Submit a new task")
    print("  status <task_id>           -- Check task status")
    print("  poll <task_id>             -- Poll until completion")
    print("  exit                       -- Quit")

    while True:
        try:
            raw_input_line = input("\n> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not raw_input_line:
            continue

        tokens = raw_input_line.split()
        command = tokens[0].lower()

        if command == "submit" and len(tokens) == 3:
            handle_submit(rpc_stub, tokens[1], tokens[2])

        elif command == "status" and len(tokens) == 2:
            handle_status(rpc_stub, tokens[1])

        elif command == "poll" and len(tokens) == 2:
            handle_poll(rpc_stub, tokens[1])

        elif command == "exit":
            print("[Client] Exiting.")
            break

        else:
            print("[Client] Unrecognized command. Available commands:")
            print("  submit <CPU|IO> <payload>")
            print("  status <task_id>")
            print("  poll <task_id>")
            print("  exit")

    channel.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python client.py <server_address>")
        print("Example: python client.py localhost:50051")
        sys.exit(1)

    run_job_client(sys.argv[1])
