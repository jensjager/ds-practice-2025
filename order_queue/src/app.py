import logging
import os
import threading
from collections import deque
from concurrent import futures

import grpc

from utils.other.runtime_utils import add_grpc_path, env_int, setup_logging

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
add_grpc_path(FILE, '../../../utils/pb/order_queue')
import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc

setup_logging()
logger = logging.getLogger(__name__)


class OrderQueueService(order_queue_grpc.OrderQueueServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = deque()

    def Enqueue(self, request, context):
        order_id = request.order_id.strip()
        if not order_id:
            return order_queue.EnqueueResponse(
                success=False,
                reason="order_id is required.",
                queue_size=0,
            )

        with self._lock:
            self._queue.append({
                "order_id": order_id,
                "order_json": request.order_json,
            })
            queue_size = len(self._queue)

        logger.info("Enqueued order_id=%s queue_size=%s", order_id, queue_size)
        return order_queue.EnqueueResponse(
            success=True,
            reason="Order enqueued.",
            queue_size=queue_size,
        )

    def Dequeue(self, request, context):
        executor_id = request.executor_id.strip() or "unknown"

        with self._lock:
            if not self._queue:
                return order_queue.DequeueResponse(
                    success=True,
                    reason="Queue is empty.",
                    has_order=False,
                    queue_size=0,
                )

            item = self._queue.popleft()
            queue_size = len(self._queue)

        order_id = item["order_id"]
        logger.info(
            "Dequeued order_id=%s by_executor=%s queue_size=%s",
            order_id,
            executor_id,
            queue_size,
        )

        return order_queue.DequeueResponse(
            success=True,
            reason="Order dequeued.",
            has_order=True,
            order_id=order_id,
            order_json=item.get("order_json", ""),
            queue_size=queue_size,
        )

    def GetQueueStatus(self, request, context):
        with self._lock:
            queue_size = len(self._queue)

        return order_queue.QueueStatusResponse(queue_size=queue_size)


def serve():
    port = str(env_int("ORDER_QUEUE_PORT", 50054))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_queue_grpc.add_OrderQueueServicer_to_server(OrderQueueService(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info("Order queue started on port %s", port)
    server.wait_for_termination()


if __name__ == '__main__':
    serve()