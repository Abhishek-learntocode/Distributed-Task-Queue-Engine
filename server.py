"""
Distributed Task Queue Server

This is the central coordinator for the task queue system. It manages:
  - Two separate FIFO queues: one for CPU tasks, one for IO tasks
  - Worker registration and capability tracking
  - Task assignment based on worker capability
  - Heartbeat-based failure detection (15-second timeout = 3 missed beats)
  - Automatic task re-queuing when a worker dies

Design decisions:
  - We use Python's collections.deque for our FIFO queues because it
    provides O(1) append and popleft operations, which is exactly
    what we need for a first-in-first-out queue.
  - We use threading.Lock to protect shared state because multiple
    gRPC threads may access the queues and worker registry simultaneously.
  - We use threading.Event to signal workers when new tasks arrive,
    avoiding busy-waiting in the RequestTask streaming loop.

Usage:
    python server.py <address>
    Example: python server.py localhost:50051
"""

import sys
import time
import threading
from collections import deque
from concurrent import futures

import grpc
import task_queue_pb2
import task_queue_pb2_grpc


# ---- Internal data structures to track tasks and workers ----

class ManagedTask:
    """
    Represents a single task tracked by the server.

    We need to track several properties for each task:
      - Its type (CPU/IO) determines which queue it goes into
      - Its status tracks the lifecycle (Queued -> Processing -> Completed/Failed)
      - Its arrival_time is used for FIFO ordering when a BOTH worker
        needs to choose between a CPU and IO task
      - Its assigned_worker links the task to whoever is processing it
    """
    def __init__(self, task_id, task_type, payload):
        self.task_id = task_id
        self.task_type = task_type
        self.payload = payload
        self.lifecycle_status = task_queue_pb2.QUEUED
        self.assigned_worker = ""
        self.arrival_time = time.time()
        # sequence_num ensures strict FIFO even if arrival_time is identical
        self.sequence_num = 0
        self.requeue_after = None   # float timestamp set when FAILED, cleared on requeue
        self.terminal_time = None   # set when task reaches terminal state

class ManagedWorker:
    """
    Tracks a registered worker's state.

    The assignment requires heartbeat monitoring, so we record when
    we last heard from each worker. If too much time passes, we
    consider the worker dead.

    The wakeup_signal (threading.Event) lets us push a notification
    to the worker's RequestTask stream when a new task becomes
    available, instead of having the worker busy-wait.
    """
    def __init__(self, worker_id, capability):
        self.worker_id = worker_id
        self.capability = capability
        self.is_alive = True
        self.last_heartbeat_time = time.time()
        self.active_task_id = ""
        # Used to wake up the worker's streaming loop when
        # a new task arrives or the worker needs to shut down
        self.wakeup_signal = threading.Event()


# ---- The gRPC service implementation ----

class TaskQueueHandler(task_queue_pb2_grpc.TaskQueueServiceServicer):
    """
    Implements all six RPCs defined in the task_queue.proto file.

    The server maintains two FIFO queues because the assignment
    requires workers with specific capabilities (CPU, IO, or BOTH)
    to only receive tasks matching their type. Keeping separate
    queues makes the matching logic straightforward.
    """

    def __init__(self):
        # We maintain separate CPU and IO queues because the assignment
        # requires workers to process tasks based on their capability.
        # Using deque gives us efficient FIFO operations.
        self.cpu_task_queue = deque()
        self.io_task_queue = deque()

        # These dictionaries let us look up any task or worker by ID
        self.all_tasks = {}
        self.all_workers = {}

        # A single lock protects all shared state. This is simpler than
        # using multiple fine-grained locks and sufficient for our
        # expected concurrency level (a handful of workers).
        self.state_lock = threading.Lock()

        # Sequential counters for generating readable task/worker IDs
        self.next_task_num = 0
        self.next_worker_num = 0

    # ---- RPCs called by Job Client ----

    def SubmitTask(self, request, context):
        """
        Accept a new task from a job client and place it in the
        appropriate FIFO queue based on its type.
        """
        with self.state_lock:
            self.next_task_num += 1
            new_id = f"T{self.next_task_num}"

            new_task = ManagedTask(new_id, request.task_type, request.payload)
            new_task.sequence_num = self.next_task_num
            self.all_tasks[new_id] = new_task

            # Route to the correct queue based on task type
            if request.task_type == task_queue_pb2.CPU:
                self.cpu_task_queue.append(new_task)
                type_label = "CPU"
            else:
                self.io_task_queue.append(new_task)
                type_label = "IO"

            print(f"[Server] Task {new_id} submitted ({type_label}, payload={request.payload})")

            # Wake up any idle workers so they check for available tasks
            self._signal_idle_workers()

        return task_queue_pb2.TaskResponse(task_id=new_id)

    def GetTaskStatus(self, request, context):
        """
        Return the current status of a task so the job client can track it.
        """
        with self.state_lock:
            task = self.all_tasks.get(request.task_id)
            if task is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"No task found with id {request.task_id}")
                return task_queue_pb2.TaskStatusResponse()

            return task_queue_pb2.TaskStatusResponse(
                task_id=task.task_id,
                status=task.lifecycle_status,
                assigned_worker=task.assigned_worker
            )

    # ---- RPCs called by Workers ----

    def RegisterWorker(self, request, context):
        """
        Register a new worker and assign it a unique ID.

        Workers must register before they can request tasks.
        The capability field determines which type of tasks
        this worker can handle.
        """
        with self.state_lock:
            self.next_worker_num += 1
            new_id = f"W{self.next_worker_num}"

            new_worker = ManagedWorker(new_id, request.capability)
            self.all_workers[new_id] = new_worker

            # Map enum to readable string for logging
            capability_labels = {
                task_queue_pb2.CPU_WORKER: "CPU",
                task_queue_pb2.IO_WORKER: "IO",
                task_queue_pb2.BOTH_CAPABLE: "BOTH"
            }
            label = capability_labels.get(request.capability, "UNKNOWN")
            print(f"[Server] Worker {new_id} registered (type: {label})")

        return task_queue_pb2.WorkerRegistration(worker_id=new_id)

    def RequestTask(self, request, context):
        """
        Server-side streaming RPC for task assignment.

        The assignment says "the server pushes the task to the worker
        when available", which is why we use server-side streaming.
        The worker opens this stream and we continuously push tasks
        to it, one at a time, waiting between tasks until the worker
        finishes the current one.

        If no matching task is available, the worker blocks here
        (via the wakeup_signal event) until a new task arrives.
        """
        wkr_id = request.worker_id

        with self.state_lock:
            worker = self.all_workers.get(wkr_id)
            if worker is None:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Worker {wkr_id} is not registered")
                return

        # Main task assignment loop — keeps running as long as
        # the worker is alive and the connection is active
        while True:
            assigned_task = None
            with self.state_lock:
                # Check if the worker has been marked as dead
                if not worker.is_alive:
                    print(f"[Server] Worker {wkr_id} is dead, closing stream")
                    return
                    
                # Try to find a task that matches this worker's capability
                # Only assign a new task if the worker isn't busy
                if worker.active_task_id == "":
                    assigned_task = self._dequeue_matching_task(worker)
                    if assigned_task is not None:
                        # Mark the task as being processed by this worker
                        assigned_task.lifecycle_status = task_queue_pb2.PROCESSING
                        assigned_task.assigned_worker = wkr_id
                        worker.active_task_id = assigned_task.task_id
                        print(f"[Server] Assigned task {assigned_task.task_id} to worker {wkr_id}")

            if assigned_task is not None:
                # Push the task to the worker through the stream
                yield task_queue_pb2.TaskAssignment(
                    task_id=assigned_task.task_id,
                    task_type=assigned_task.task_type,
                    payload=assigned_task.payload
                )
            else:
                # No task available right now — wait until signaled
                # We use a timeout so we can periodically check if the
                # connection is still alive or the worker was marked dead
                worker.wakeup_signal.clear()
                worker.wakeup_signal.wait(timeout=2.0)

                # Check if the gRPC connection dropped
                if not context.is_active():
                    print(f"[Server] Worker {wkr_id} disconnected")
                    return

                with self.state_lock:
                    if not worker.is_alive:
                        return

    def SendHeartbeat(self, request, context):
        """
        Record that a worker is still alive.

        Workers are expected to call this every 5 seconds. The
        heartbeat monitor thread checks timestamps and marks
        workers as dead if they go silent for 15+ seconds.
        """
        with self.state_lock:
            worker = self.all_workers.get(request.worker_id)
            if worker is None:
                return task_queue_pb2.ServerAck(
                    acknowledged=False,
                    info=f"Unknown worker {request.worker_id}"
                )
            worker.last_heartbeat_time = time.time()

        return task_queue_pb2.ServerAck(acknowledged=True, info="Heartbeat recorded")

    def UpdateTaskStatus(self, request, context):
        """
        Worker reports that it has finished (or failed) a task.

        After this call, the worker is freed up to receive a new task.
        We signal the worker's wakeup event so the RequestTask loop
        immediately checks for another available task.
        """
        with self.state_lock:
            task = self.all_tasks.get(request.task_id)
            if task is None:
                return task_queue_pb2.ServerAck(
                    acknowledged=False,
                    info=f"Unknown task {request.task_id}"
                )

            worker = self.all_workers.get(request.worker_id)

            # Update the task's lifecycle status
            task.lifecycle_status = request.status
            if request.status in (task_queue_pb2.COMPLETED, task_queue_pb2.FAILED):
                task.terminal_time = time.time()
                task.assigned_worker = ""  # Clear assignment for terminal states
            
            outcome = "Completed" if request.status == task_queue_pb2.COMPLETED else "Failed"
            print(f"[Server] Task {request.task_id}: {outcome} (by worker {request.worker_id})")

            # Free the worker so it can be assigned another task
            if worker is not None and worker.active_task_id == request.task_id:
                worker.active_task_id = ""
                worker.wakeup_signal.set()

        return task_queue_pb2.ServerAck(acknowledged=True, info=f"Status updated to {outcome}")

    # ---- Internal helper methods ----

    def _dequeue_matching_task(self, worker):
        """
        Find and remove the first task from the queues that matches
        this worker's capability.

        The assignment specifies:
          - CPU workers only get CPU tasks
          - IO workers only get IO tasks
          - BOTH workers can get either; when both queues have tasks,
            pick whichever task arrived first across both queues

        This ensures global FIFO ordering for BOTH-capable workers.
        """
        # Determine which queues this worker is allowed to pull from
        handles_cpu = worker.capability in (
            task_queue_pb2.CPU_WORKER, task_queue_pb2.BOTH_CAPABLE
        )
        handles_io = worker.capability in (
            task_queue_pb2.IO_WORKER, task_queue_pb2.BOTH_CAPABLE
        )

        # Peek at the front of each eligible queue (without removing yet)
        front_cpu = self.cpu_task_queue[0] if (handles_cpu and len(self.cpu_task_queue) > 0) else None
        front_io = self.io_task_queue[0] if (handles_io and len(self.io_task_queue) > 0) else None

        # Decide which task to dequeue
        if front_cpu is not None and front_io is not None:
            # Both queues have tasks — pick the one that arrived earlier
            # to maintain overall FIFO ordering across the system
            if front_cpu.sequence_num <= front_io.sequence_num:
                return self.cpu_task_queue.popleft()
            else:
                return self.io_task_queue.popleft()
        elif front_cpu is not None:
            return self.cpu_task_queue.popleft()
        elif front_io is not None:
            return self.io_task_queue.popleft()
        else:
            return None

    def _signal_idle_workers(self):
        """
        Wake up all idle workers so they can check if a newly
        submitted task matches their capability.
        """
        for worker in self.all_workers.values():
            if worker.is_alive and worker.active_task_id == "":
                worker.wakeup_signal.set()

    def _run_heartbeat_checker(self):
        """
        Background thread that runs for the lifetime of the server.

        Every 5 seconds, it scans all registered workers and checks
        if any have gone silent for more than 15 seconds (which means
        3 missed heartbeat intervals). If so, it:
          1. Marks the worker as dead
          2. Marks the worker's in-progress task as Failed
          3. Re-queues the failed task at the FRONT of its queue
             so it gets reassigned immediately to another worker

        The assignment requires re-queuing at the front because a
        failed task should be retried before any newer tasks.
        """
        while True:
            time.sleep(5)
            current_time = time.time()

            with self.state_lock:
                for worker in list(self.all_workers.values()):
                    if not worker.is_alive:
                        continue

                    seconds_since_heartbeat = current_time - worker.last_heartbeat_time

                    # 15 seconds = 3 missed heartbeat intervals (5 sec each)
                    if seconds_since_heartbeat > 15:
                        print(
                            f"[Server] Worker {worker.worker_id} timed out "
                            f"({seconds_since_heartbeat:.0f}s without heartbeat). "
                            f"Marking as DEAD."
                        )
                        worker.is_alive = False
                        # Wake up the worker's stream loop so it exits cleanly
                        worker.wakeup_signal.set()

                        # Handle the task that was being processed by this worker
                        if worker.active_task_id != "":
                            failed_task = self.all_tasks.get(worker.active_task_id)
                            if failed_task is not None:
                                # First mark as Failed so the job client sees the failure
                                print(f"[Server] Task {failed_task.task_id} failed due to worker death, re-queuing")
                                failed_task.lifecycle_status = task_queue_pb2.FAILED
                                failed_task.assigned_worker = ""
                                # Mark for requeue (actual visibility ~5s due to checker interval)
                                failed_task.requeue_after = current_time + 1.0
                                worker.active_task_id = ""

                # Second pass: check if any FAILED tasks are ready to be re-queued
                for task in list(self.all_tasks.values()):
                    if (task.lifecycle_status == task_queue_pb2.FAILED
                            and task.requeue_after is not None
                            and current_time >= task.requeue_after):
                        task.requeue_after = None
                        task.lifecycle_status = task_queue_pb2.QUEUED
                        if task.task_type == task_queue_pb2.CPU:
                            self.cpu_task_queue.appendleft(task)
                        else:
                            self.io_task_queue.appendleft(task)
                        print(f"[Server] Task {task.task_id} re-queued at front after failure")
                        self._signal_idle_workers()

                # Third pass: Cleanup tasks reaching terminal states older than 5 minutes
                cleanup_threshold = current_time - 300.0  # 5 minutes
                to_delete = [t_id for t_id, task in self.all_tasks.items() 
                             if task.terminal_time is not None and task.terminal_time < cleanup_threshold]
                for t_id in to_delete:
                    del self.all_tasks[t_id]
                if to_delete:
                    print(f"[Server] Cleaned up {len(to_delete)} old terminal tasks from memory.")

def start_server(address):
    """
    Initialize and run the task queue server.

    We start a background thread for heartbeat monitoring before
    the gRPC server begins accepting connections. The daemon flag
    ensures the heartbeat thread exits when the main program does.
    """
    handler = TaskQueueHandler()

    # Start the heartbeat monitoring in a separate daemon thread
    heartbeat_thread = threading.Thread(
        target=handler._run_heartbeat_checker,
        daemon=True
    )
    heartbeat_thread.start()

    # Create and start the gRPC server with enough threads
    # to handle multiple workers and clients concurrently
    # Each streaming RequestTask worker permanently holds 1 thread.
    # Budget: enough threads for N workers + N client connections + overhead.
    # Threads blocked on Event.wait() consume negligible CPU, so a large
    # pool is cheap. 50 workers + heartbeats + submits + status checks = safe.
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    task_queue_pb2_grpc.add_TaskQueueServiceServicer_to_server(handler, grpc_server)
    grpc_server.add_insecure_port(address)
    grpc_server.start()
    print(f"[Server] Listening on {address}")

    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[Server] Shutting down...")
        grpc_server.stop(grace=0)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python server.py <address>")
        print("Example: python server.py localhost:50051")
        sys.exit(1)

    start_server(sys.argv[1])
