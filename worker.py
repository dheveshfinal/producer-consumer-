import json
import time
import redis
from kafka import KafkaConsumer
from worker_utils import update_task_status, acquire_lock, release_lock


class TaskWorker:
    def _init_(self, worker_id, kafka_topics, lock_path=None):
        self.worker_id = worker_id
        self.kafka_topics = kafka_topics
        self.lock_path = lock_path
        self.redis_client = redis.Redis(host="localhost", port=6379)
        self.consumer = KafkaConsumer(
            *kafka_topics,
            bootstrap_servers="localhost:9092",
            group_id="task_workers",
            value_deserializer=lambda message: json.loads(message.decode("utf-8")),
        )

    def handle_task(self, task_data):
        task_id = task_data["id"]
        operation = task_data.get("type", task_id.split("_")[0])
        operand1 = task_data["operand1"]
        operand2 = task_data["operand2"]

        print(f"[{self.worker_id}] Received task: {task_data}")

        # Attempt to acquire a lock if lock_path is specified
        if self.lock_path and not acquire_lock(self.lock_path):
            print(f"[{self.worker_id}] Worker is busy, task skipped.")
            return

        try:
            # Mark worker's state in Redis
            self.redis_client.set(f"worker:{self.worker_id}", "busy")

            # Update task status in the system
            update_task_status(task_id, "processing")

            # Simulate time-consuming processing
            time.sleep(30)

            # Perform operations
            operation_map = {
                "add": lambda x, y: x + y,
                "sub": lambda x, y: x - y,
                "mul": lambda x, y: x * y,
            }

            if operation not in operation_map:
                raise ValueError(f"Unsupported task type: {operation}")

            result = operation_map[operation](operand1, operand2)

            # Task successfully completed
            update_task_status(task_id, "success", result=result)
            print(f"[{self.worker_id}] Completed task {task_id}: Result = {result}")

        except Exception as error:
            update_task_status(task_id, "failed", error=str(error))
            print(f"[{self.worker_id}] Failed task {task_id}: Error = {error}")

        finally:
            # Set worker to idle and release lock
            self.redis_client.set(f"worker:{self.worker_id}", "idle")
            if self.lock_path:
                release_lock(self.lock_path)

    def listen_and_process(self):
        print(f"[{self.worker_id}] Listening for tasks...")
        for task_message in self.consumer:
            self.handle_task(task_message.value)


# Functions to initialize specific worker instances
def addition_worker():
    worker = TaskWorker(
        worker_id="addition_worker",
        kafka_topics=["addition"],
        lock_path="./worker_locks/addition.lock",
    )
    worker.listen_and_process()


def subtraction_worker():
    worker = TaskWorker(
        worker_id="subtraction_worker",
        kafka_topics=["subtraction"],
        lock_path="./worker_locks/subtraction.lock",
    )
    worker.listen_and_process()


def multiplication_worker():
    worker = TaskWorker(
        worker_id="multiplication_worker",
        kafka_topics=["multiplication"],
        lock_path="./worker_locks/multiplication.lock",
    )
    worker.listen_and_process()


def general_worker():
    worker = TaskWorker(
        worker_id="generic_worker",
        kafka_topics=["worker1", "worker2", "worker3"],
    )
    worker.listen_and_process()


if _name_ == "_main_":
    # Uncomment the required worker instance to run
    # addition_worker()
    # subtraction_worker()
    # multiplication_worker()
    general_worker()