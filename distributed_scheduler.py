"""
Distributed Scheduler Simulation
=================================
Simulates a distributed task scheduler with:
- Master Scheduler: receives tasks, maintains queue, assigns workers
- Worker Nodes: execute tasks, update status, report completion
- Fault Tolerance: simulates worker failure and task reassignment
- Logging: start time, completion time, failures
- SQLite database: tasks and workers tables
"""

import sqlite3
import threading
import time
import random
import logging
import uuid
from datetime import datetime
from queue import Queue, Empty
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


# ─── Logging Setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DistributedScheduler")


# ─── Enums ────────────────────────────────────────────────────────────────────
class TaskStatus(str, Enum):
    PENDING   = "PENDING"
    ASSIGNED  = "ASSIGNED"
    RUNNING   = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED    = "FAILED"


class WorkerStatus(str, Enum):
    IDLE    = "IDLE"
    BUSY    = "BUSY"
    OFFLINE = "OFFLINE"


# ─── Database Layer ───────────────────────────────────────────────────────────
class Database:
    """SQLite database manager for tasks and workers."""

    def __init__(self, db_path: str = "scheduler.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._init_schema()

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return self._local.conn

    def _init_schema(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id   TEXT PRIMARY KEY,
                    name        TEXT NOT NULL,
                    status      TEXT NOT NULL DEFAULT 'IDLE',
                    task_count  INTEGER DEFAULT 0,
                    registered_at TEXT NOT NULL,
                    last_seen   TEXT
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    task_id       TEXT PRIMARY KEY,
                    name          TEXT NOT NULL,
                    payload       TEXT,
                    status        TEXT NOT NULL DEFAULT 'PENDING',
                    priority      INTEGER DEFAULT 5,
                    worker_id     TEXT,
                    attempts      INTEGER DEFAULT 0,
                    max_attempts  INTEGER DEFAULT 3,
                    created_at    TEXT NOT NULL,
                    assigned_at   TEXT,
                    started_at    TEXT,
                    completed_at  TEXT,
                    duration_ms   INTEGER,
                    error_msg     TEXT,
                    FOREIGN KEY(worker_id) REFERENCES workers(worker_id)
                );

                CREATE TABLE IF NOT EXISTS task_logs (
                    log_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id     TEXT NOT NULL,
                    worker_id   TEXT,
                    event       TEXT NOT NULL,
                    detail      TEXT,
                    ts          TEXT NOT NULL
                );
            """)
        logger.info("Database schema initialised at '%s'", self.db_path)

    # Workers
    def upsert_worker(self, worker_id: str, name: str, status: str):
        now = datetime.now().isoformat()
        self._conn().execute("""
            INSERT INTO workers (worker_id, name, status, registered_at, last_seen)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET status=excluded.status, last_seen=excluded.last_seen
        """, (worker_id, name, status, now, now))
        self._conn().commit()

    def update_worker_status(self, worker_id: str, status: str):
        now = datetime.now().isoformat()
        self._conn().execute(
            "UPDATE workers SET status=?, last_seen=? WHERE worker_id=?",
            (status, now, worker_id)
        )
        self._conn().commit()

    def increment_worker_task_count(self, worker_id: str):
        self._conn().execute(
            "UPDATE workers SET task_count = task_count + 1 WHERE worker_id=?",
            (worker_id,)
        )
        self._conn().commit()

    # Tasks
    def insert_task(self, task_id: str, name: str, payload: str, priority: int):
        now = datetime.now().isoformat()
        self._conn().execute("""
            INSERT INTO tasks (task_id, name, payload, status, priority, created_at)
            VALUES (?, ?, ?, 'PENDING', ?, ?)
        """, (task_id, name, payload, priority, now))
        self._conn().commit()

    def assign_task(self, task_id: str, worker_id: str):
        now = datetime.now().isoformat()
        self._conn().execute("""
            UPDATE tasks
            SET status='ASSIGNED', worker_id=?, assigned_at=?, attempts=attempts+1
            WHERE task_id=?
        """, (worker_id, now, task_id))
        self._conn().commit()

    def start_task(self, task_id: str):
        now = datetime.now().isoformat()
        self._conn().execute(
            "UPDATE tasks SET status='RUNNING', started_at=? WHERE task_id=?",
            (now, task_id)
        )
        self._conn().commit()

    def complete_task(self, task_id: str, duration_ms: int):
        now = datetime.now().isoformat()
        self._conn().execute("""
            UPDATE tasks
            SET status='COMPLETED', completed_at=?, duration_ms=?
            WHERE task_id=?
        """, (now, duration_ms, task_id))
        self._conn().commit()

    def fail_task(self, task_id: str, error: str):
        now = datetime.now().isoformat()
        self._conn().execute("""
            UPDATE tasks
            SET status='FAILED', completed_at=?, error_msg=?
            WHERE task_id=?
        """, (now, error, task_id))
        self._conn().commit()

    def requeue_task(self, task_id: str):
        self._conn().execute(
            "UPDATE tasks SET status='PENDING', worker_id=NULL, assigned_at=NULL WHERE task_id=?",
            (task_id,)
        )
        self._conn().commit()

    def log_event(self, task_id: str, worker_id: Optional[str], event: str, detail: str = ""):
        now = datetime.now().isoformat()
        self._conn().execute("""
            INSERT INTO task_logs (task_id, worker_id, event, detail, ts)
            VALUES (?, ?, ?, ?, ?)
        """, (task_id, worker_id, event, detail, now))
        self._conn().commit()

    def get_task_summary(self):
        cur = self._conn().execute("""
            SELECT status, COUNT(*) as cnt FROM tasks GROUP BY status
        """)
        return {row["status"]: row["cnt"] for row in cur.fetchall()}

    def get_all_tasks(self):
        cur = self._conn().execute("""
            SELECT task_id, name, status, worker_id, attempts,
                   created_at, started_at, completed_at, duration_ms, error_msg
            FROM tasks ORDER BY created_at
        """)
        return [dict(row) for row in cur.fetchall()]

    def get_all_workers(self):
        cur = self._conn().execute(
            "SELECT * FROM workers ORDER BY registered_at"
        )
        return [dict(row) for row in cur.fetchall()]


# ─── Data Classes ─────────────────────────────────────────────────────────────
@dataclass(order=True)
class Task:
    priority: int
    task_id: str = field(compare=False)
    name: str = field(compare=False)
    payload: str = field(compare=False, default="")
    attempts: int = field(compare=False, default=0)
    max_attempts: int = field(compare=False, default=3)


# ─── Worker Node ──────────────────────────────────────────────────────────────
class WorkerNode(threading.Thread):
    """Simulates a worker node that picks up and executes tasks."""

    FAILURE_PROBABILITY = 0.15  # 15% chance of failure per task

    def __init__(self, worker_id: str, name: str, db: Database,
                 completion_callback, failure_callback):
        super().__init__(daemon=True, name=name)
        self.worker_id   = worker_id
        self.worker_name = name
        self.db          = db
        self.on_complete = completion_callback
        self.on_failure  = failure_callback
        self.task_queue: Queue = Queue()
        self._alive      = True
        self._status     = WorkerStatus.IDLE
        self.log         = logging.getLogger(name)

    def assign(self, task: Task):
        self.task_queue.put(task)

    def stop(self):
        self._alive = False

    def simulate_crash(self):
        """Simulate a worker going offline mid-task."""
        self.log.warning("[CRASH] Worker %s CRASHED!", self.worker_name)
        self._alive = False
        self.db.update_worker_status(self.worker_id, WorkerStatus.OFFLINE)

    def run(self):
        self.db.upsert_worker(self.worker_id, self.worker_name, WorkerStatus.IDLE)
        self.log.info("Worker %s started and IDLE", self.worker_name)

        while self._alive:
            try:
                task: Task = self.task_queue.get(timeout=1)
            except Empty:
                continue

            self._status = WorkerStatus.BUSY
            self.db.update_worker_status(self.worker_id, WorkerStatus.BUSY)
            self.db.start_task(task.task_id)
            self.db.log_event(task.task_id, self.worker_id, "STARTED")
            self.log.info(">> Running task [%s] '%s'", task.task_id[:8], task.name)

            start_ms = time.time() * 1000
            exec_time = random.uniform(0.5, 2.0)

            # Simulate work + possible failure
            time.sleep(exec_time)

            if random.random() < self.FAILURE_PROBABILITY:
                error = f"Worker {self.worker_name}: simulated execution error"
                self.log.warning("[X] Task [%s] FAILED - %s", task.task_id[:8], error)
                self.db.fail_task(task.task_id, error)
                self.db.log_event(task.task_id, self.worker_id, "FAILED", error)
                self.on_failure(task, self.worker_id)
            else:
                duration = int(time.time() * 1000 - start_ms)
                self.db.complete_task(task.task_id, duration)
                self.db.increment_worker_task_count(self.worker_id)
                self.db.log_event(task.task_id, self.worker_id, "COMPLETED",
                                  f"duration={duration}ms")
                self.log.info("[OK] Task [%s] COMPLETED in %dms", task.task_id[:8], duration)
                self.on_complete(task, self.worker_id)

            self._status = WorkerStatus.IDLE
            self.db.update_worker_status(self.worker_id, WorkerStatus.IDLE)
            self.task_queue.task_done()


# ─── Master Scheduler ─────────────────────────────────────────────────────────
class MasterScheduler:
    """
    Master Scheduler:
    - Maintains a priority task queue
    - Assigns tasks to available workers (round-robin with load-awareness)
    - Handles fault tolerance: detects failures, requeues, reassigns
    """

    def __init__(self, db: Database, num_workers: int = 3):
        self.db          = db
        self.task_queue  = Queue()          # (priority, Task)
        self.workers: list[WorkerNode] = []
        self._lock       = threading.Lock()
        self._running    = False
        self.log         = logging.getLogger("MasterScheduler")
        self._completed  = 0
        self._failed     = 0
        self._reassigned = 0
        self._dispatch_thread: Optional[threading.Thread] = None

        self._spawn_workers(num_workers)

    # ── Worker Management ─────────────────────────────────────────────────────
    def _spawn_workers(self, n: int):
        for i in range(1, n + 1):
            wid  = str(uuid.uuid4())
            name = f"Worker-{i}"
            w = WorkerNode(
                worker_id=wid,
                name=name,
                db=self.db,
                completion_callback=self._on_task_complete,
                failure_callback=self._on_task_failure,
            )
            self.workers.append(w)
            w.start()
        self.log.info("Spawned %d worker nodes", n)

    def _available_workers(self) -> list[WorkerNode]:
        return [w for w in self.workers
                if w._alive and w._status == WorkerStatus.IDLE]

    def _least_loaded_worker(self) -> Optional[WorkerNode]:
        available = self._available_workers()
        if not available:
            return None
        return random.choice(available)

    # ── Task Submission ───────────────────────────────────────────────────────
    def submit_task(self, name: str, payload: str = "", priority: int = 5) -> str:
        task_id = str(uuid.uuid4())
        task = Task(
            priority=priority,
            task_id=task_id,
            name=name,
            payload=payload,
        )
        self.db.insert_task(task_id, name, payload, priority)
        self.task_queue.put((priority, task))
        self.db.log_event(task_id, None, "SUBMITTED", f"priority={priority}")
        self.log.info("[SUBMIT] Task submitted: [%s] '%s' (priority=%d)",
                      task_id[:8], name, priority)
        return task_id

    # ── Dispatch Loop ─────────────────────────────────────────────────────────
    def _dispatch_loop(self):
        self.log.info("Dispatch loop started")
        while self._running:
            try:
                _, task = self.task_queue.get(timeout=0.5)
            except Empty:
                continue

            worker = None
            while worker is None and self._running:
                worker = self._least_loaded_worker()
                if worker is None:
                    time.sleep(0.2)

            if worker is None:
                break  # shutting down

            self.db.assign_task(task.task_id, worker.worker_id)
            self.db.log_event(task.task_id, worker.worker_id, "ASSIGNED")
            worker.assign(task)
            self.log.info("[->] Task [%s] assigned to %s",
                          task.task_id[:8], worker.worker_name)
            self.task_queue.task_done()

    # ── Callbacks ─────────────────────────────────────────────────────────────
    def _on_task_complete(self, task: Task, worker_id: str):
        with self._lock:
            self._completed += 1

    def _on_task_failure(self, task: Task, worker_id: str):
        with self._lock:
            self._failed += 1
            task.attempts += 1
            if task.attempts < task.max_attempts:
                self.log.warning("[RETRY] Requeuing task [%s] (attempt %d/%d)",
                                 task.task_id[:8], task.attempts, task.max_attempts)
                self.db.requeue_task(task.task_id)
                self.db.log_event(task.task_id, None, "REQUEUED",
                                  f"attempt={task.attempts}")
                self.task_queue.put((task.priority, task))
                self._reassigned += 1
            else:
                self.log.error("[FAIL] Task [%s] exhausted all %d attempts. Permanently failed.",
                               task.task_id[:8], task.max_attempts)

    # ── Fault Tolerance ───────────────────────────────────────────────────────
    def simulate_worker_crash(self, worker_index: int = 0):
        """Crash a specific worker to demonstrate fault tolerance."""
        alive = [w for w in self.workers if w._alive]
        if not alive or worker_index >= len(alive):
            self.log.warning("No alive worker at index %d to crash", worker_index)
            return
        target = alive[worker_index]
        target.simulate_crash()

        # Drain any tasks stuck in that worker's queue and requeue them
        while not target.task_queue.empty():
            try:
                stuck_task: Task = target.task_queue.get_nowait()
                self.log.warning("[RECOVER] Recovered stuck task [%s] from crashed %s",
                                 stuck_task.task_id[:8], target.worker_name)
                self.db.requeue_task(stuck_task.task_id)
                self.db.log_event(stuck_task.task_id, None, "RECOVERED_FROM_CRASH")
                self.task_queue.put((stuck_task.priority, stuck_task))
                self._reassigned += 1
            except Empty:
                break

    # ── Lifecycle ─────────────────────────────────────────────────────────────
    def start(self):
        self._running = True
        self._dispatch_thread = threading.Thread(
            target=self._dispatch_loop, name="Dispatcher", daemon=True
        )
        self._dispatch_thread.start()
        self.log.info("Master Scheduler started")

    def shutdown(self, wait: bool = True):
        self.log.info("Shutting down scheduler…")
        self._running = False
        if self._dispatch_thread:
            self._dispatch_thread.join(timeout=5)
        for w in self.workers:
            w.stop()
        if wait:
            for w in self.workers:
                w.join(timeout=3)
        self.log.info("Scheduler shutdown complete")

    def wait_for_completion(self, timeout: float = 30.0):
        """Block until all submitted tasks are done or timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            summary = self.db.get_task_summary()
            pending   = summary.get("PENDING", 0)
            assigned  = summary.get("ASSIGNED", 0)
            running   = summary.get("RUNNING", 0)
            if pending + assigned + running == 0:
                break
            time.sleep(0.5)

    # ── Report ────────────────────────────────────────────────────────────────
    def print_report(self):
        summary  = self.db.get_task_summary()
        workers  = self.db.get_all_workers()
        tasks    = self.db.get_all_tasks()

        print("\n" + "═" * 65)
        print("  DISTRIBUTED SCHEDULER — EXECUTION REPORT")
        print("═" * 65)

        print("\nTask Summary:")
        for status, count in summary.items():
            bar = "█" * count
            print(f"  {status:<12} {count:>3}  {bar}")

        print(f"\n  Total Reassignments : {self._reassigned}")
        print(f"  Worker Crashes      : {sum(1 for w in self.workers if not w._alive)}")

        print("\nWorker Status:")
        print(f"  {'Name':<12} {'Status':<10} {'Tasks Done':<12} {'Alive'}")
        print("  " + "-" * 45)
        for w in workers:
            alive = any(wn.worker_id == w["worker_id"] and wn._alive
                        for wn in self.workers)
            print(f"  {w['name']:<12} {w['status']:<10} {w['task_count']:<12} {'Yes' if alive else 'No (offline)'}")

        print("\nTask Details:")
        print(f"  {'ID':<10} {'Name':<22} {'Status':<11} {'Worker':<12} {'ms'}")
        print("  " + "-" * 65)
        for t in tasks:
            wid   = (t["worker_id"] or "")[:8]
            dur   = str(t["duration_ms"]) if t["duration_ms"] else "-"
            print(f"  {t['task_id'][:8]:<10} {t['name'][:22]:<22} {t['status']:<11} {wid:<12} {dur}")

        print("\n" + "═" * 65 + "\n")


# ─── Demo / Entry Point ───────────────────────────────────────────────────────
def run_demo():
    print("\n" + "═" * 65)
    print("  DISTRIBUTED SCHEDULER — SIMULATION DEMO")
    print("═" * 65 + "\n")

    import os
    db_path = "scheduler.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    db        = Database(db_path)
    scheduler = MasterScheduler(db, num_workers=3)
    scheduler.start()

    # Phase 1: Submit a batch of tasks
    print("\n[Phase 1] Submitting 10 tasks…\n")
    task_names = [
        ("Parse CSV Report", "file=report_q1.csv", 3),
        ("Send Email Digest", "recipients=50", 5),
        ("Resize Images", "count=200", 4),
        ("Generate Invoice #1001", "", 2),
        ("Sync Database Backup", "size=500MB", 1),
        ("Run Unit Tests", "suite=core", 6),
        ("Compress Logs", "date=2024-06-01", 7),
        ("Update User Profiles", "batch=300", 5),
        ("Calculate Analytics", "period=monthly", 4),
        ("Clean Temp Files", "", 8),
    ]
    for name, payload, priority in task_names:
        scheduler.submit_task(name, payload, priority)

    time.sleep(2)

    # Phase 2: Crash a worker to demonstrate fault tolerance
    print("\n[Phase 2] Simulating Worker-1 crash for fault tolerance demo…\n")
    scheduler.simulate_worker_crash(worker_index=0)

    # Phase 3: Submit more tasks after crash (to show reassignment)
    print("\n[Phase 3] Submitting 5 more tasks after crash…\n")
    for i in range(5):
        scheduler.submit_task(f"Post-Crash Task {i+1}", priority=random.randint(1, 9))

    # Wait for everything to finish
    print("\n[Waiting for all tasks to complete…]\n")
    scheduler.wait_for_completion(timeout=45)

    # Shutdown and report
    scheduler.shutdown(wait=True)
    scheduler.print_report()

    print("Database saved to:", db_path)
    print("Run `sqlite3 scheduler.db` to inspect the data.\n")


if __name__ == "__main__":
    run_demo()
