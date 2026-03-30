import logging
import os
import threading
import time
from concurrent import futures

import grpc

from utils.other.runtime_utils import add_grpc_path, env_float, env_int, setup_logging

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
add_grpc_path(FILE, '../../../utils/pb/order_executor')
add_grpc_path(FILE, '../../../utils/pb/order_queue')
import order_executor_pb2 as order_executor
import order_executor_pb2_grpc as order_executor_grpc
import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc

setup_logging()
logger = logging.getLogger(__name__)


def parse_peer_config(raw_peers):
    peers = {}
    if not raw_peers:
        return peers

    for part in raw_peers.split(","):
        item = part.strip()
        if not item:
            continue
        if ":" not in item:
            continue

        peer_id_text, target = item.split(":", 1)
        try:
            peer_id = int(peer_id_text.strip())
        except ValueError:
            continue

        cleaned_target = target.strip()
        if not cleaned_target:
            continue

        peers[peer_id] = cleaned_target

    return peers


class ExecutorNode(order_executor_grpc.OrderExecutorServicer):
    def __init__(self):
        self.executor_id = env_int("EXECUTOR_ID", 1)
        self.executor_port = env_int("ORDER_EXECUTOR_PORT", 50055)
        self.executor_target = os.getenv("EXECUTOR_TARGET", f"localhost:{self.executor_port}")
        self.order_queue_target = os.getenv("ORDER_QUEUE_TARGET", "order_queue:50054")
        self.peers = parse_peer_config(os.getenv("EXECUTOR_PEERS", ""))

        self.rpc_timeout_seconds = env_float("RPC_TIMEOUT_SECONDS", 2.0)
        self.election_wait_seconds = env_float("ELECTION_WAIT_SECONDS", 3.0)
        self.leader_check_interval_seconds = env_float("LEADER_CHECK_INTERVAL_SECONDS", 2.0)
        self.dequeue_interval_seconds = env_float("DEQUEUE_INTERVAL_SECONDS", 1.0)

        self._state_lock = threading.Lock()
        self._leader_id = None
        self._election_in_progress = False
        self._running = True

    def stop(self):
        self._running = False

    def is_leader(self):
        with self._state_lock:
            return self._leader_id == self.executor_id

    def _set_leader(self, leader_id):
        with self._state_lock:
            self._leader_id = leader_id
            self._election_in_progress = False

    def _peer_stub(self, target):
        channel = grpc.insecure_channel(target)
        stub = order_executor_grpc.OrderExecutorStub(channel)
        return channel, stub

    def _queue_stub(self):
        channel = grpc.insecure_channel(self.order_queue_target)
        stub = order_queue_grpc.OrderQueueStub(channel)
        return channel, stub

    def _is_peer_alive(self, peer_id):
        target = self.peers.get(peer_id)
        if not target:
            return False

        channel, stub = self._peer_stub(target)
        try:
            response = stub.Ping(
                order_executor.PingRequest(from_executor_id=self.executor_id),
                timeout=self.rpc_timeout_seconds,
            )
            return bool(response.alive)
        except grpc.RpcError:
            return False
        finally:
            channel.close()

    def _announce_leader(self, leader_id):
        for peer_id, target in self.peers.items():
            if peer_id == self.executor_id:
                continue

            channel, stub = self._peer_stub(target)
            try:
                stub.AnnounceLeader(
                    order_executor.CoordinatorRequest(leader_id=leader_id),
                    timeout=self.rpc_timeout_seconds,
                )
            except grpc.RpcError:
                logger.warning("Failed to notify peer_id=%s about leader_id=%s", peer_id, leader_id)
            finally:
                channel.close()

    def _become_leader(self):
        self._set_leader(self.executor_id)
        logger.info("Executor %s became leader", self.executor_id)
        self._announce_leader(self.executor_id)

    def _wait_for_coordinator(self):
        deadline = time.time() + self.election_wait_seconds
        while self._running and time.time() < deadline:
            with self._state_lock:
                if self._leader_id is not None and self._leader_id != self.executor_id:
                    self._election_in_progress = False
                    return True
            time.sleep(0.2)
        return False

    def start_election(self):
        with self._state_lock:
            if self._election_in_progress:
                return
            self._election_in_progress = True
            self._leader_id = None

        logger.info("Executor %s starts Bully election", self.executor_id)

        higher_alive = False
        for peer_id in sorted(self.peers.keys()):
            if peer_id <= self.executor_id:
                continue

            target = self.peers[peer_id]
            channel, stub = self._peer_stub(target)
            try:
                response = stub.Election(
                    order_executor.ElectionRequest(candidate_id=self.executor_id),
                    timeout=self.rpc_timeout_seconds,
                )
                if response.alive:
                    higher_alive = True
            except grpc.RpcError:
                pass
            finally:
                channel.close()

        if higher_alive:
            if self._wait_for_coordinator():
                return

        self._become_leader()

    def _start_election_async(self):
        thread = threading.Thread(target=self.start_election, daemon=True)
        thread.start()

    def _leader_monitor_loop(self):
        time.sleep(1.0)
        self._start_election_async()

        while self._running:
            with self._state_lock:
                leader_id = self._leader_id

            if leader_id is None:
                self._start_election_async()
                time.sleep(self.leader_check_interval_seconds)
                continue

            if leader_id == self.executor_id:
                time.sleep(self.leader_check_interval_seconds)
                continue

            if not self._is_peer_alive(leader_id):
                logger.warning(
                    "Current leader_id=%s appears down, triggering election from executor_id=%s",
                    leader_id,
                    self.executor_id,
                )
                self._start_election_async()

            time.sleep(self.leader_check_interval_seconds)

    def _execute_orders_loop(self):
        while self._running:
            if not self.is_leader():
                time.sleep(self.dequeue_interval_seconds)
                continue

            channel, stub = self._queue_stub()
            try:
                response = stub.Dequeue(
                    order_queue.DequeueRequest(executor_id=str(self.executor_id)),
                    timeout=self.rpc_timeout_seconds,
                )
            except grpc.RpcError as exc:
                logger.warning("Leader dequeue failed: %s", exc.code().name)
                time.sleep(self.dequeue_interval_seconds)
                continue
            finally:
                channel.close()

            if not response.success:
                logger.warning("Queue rejected dequeue: %s", response.reason)
                time.sleep(self.dequeue_interval_seconds)
                continue

            if not response.has_order:
                time.sleep(self.dequeue_interval_seconds)
                continue

            logger.info(
                "Order is being executed... order_id=%s executor_id=%s queue_size=%s",
                response.order_id,
                self.executor_id,
                response.queue_size,
            )

    def Ping(self, request, context):
        with self._state_lock:
            leader_id = self._leader_id or 0

        return order_executor.PingResponse(
            alive=True,
            executor_id=self.executor_id,
            leader_id=leader_id,
        )

    def Election(self, request, context):
        candidate_id = request.candidate_id
        if self.executor_id > candidate_id:
            self._start_election_async()
            reason = "Higher ID executor is alive and will participate."
            alive = True
        else:
            reason = "Candidate has higher priority."
            alive = True

        return order_executor.ElectionResponse(
            alive=alive,
            responder_id=self.executor_id,
            reason=reason,
        )

    def AnnounceLeader(self, request, context):
        self._set_leader(request.leader_id)
        logger.info("Executor %s accepted leader_id=%s", self.executor_id, request.leader_id)
        return order_executor.CoordinatorResponse(
            acknowledged=True,
            executor_id=self.executor_id,
        )

    def GetStatus(self, request, context):
        with self._state_lock:
            leader_id = self._leader_id or 0
            election_in_progress = self._election_in_progress

        return order_executor.StatusResponse(
            executor_id=self.executor_id,
            leader_id=leader_id,
            is_leader=leader_id == self.executor_id,
            election_in_progress=election_in_progress,
            known_peer_count=len(self.peers),
        )


def serve():
    executor = ExecutorNode()

    monitor_thread = threading.Thread(target=executor._leader_monitor_loop, daemon=True)
    monitor_thread.start()

    worker_thread = threading.Thread(target=executor._execute_orders_loop, daemon=True)
    worker_thread.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    order_executor_grpc.add_OrderExecutorServicer_to_server(executor, server)
    port = str(executor.executor_port)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(
        "Order executor started executor_id=%s target=%s queue_target=%s peers=%s",
        executor.executor_id,
        executor.executor_target,
        executor.order_queue_target,
        sorted(executor.peers.keys()),
    )

    try:
        server.wait_for_termination()
    finally:
        executor.stop()


if __name__ == '__main__':
    serve()