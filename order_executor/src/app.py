import logging
import os
import threading
import time
import json
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

add_grpc_path(FILE, '../../../utils/pb/books_database')
import books_database_pb2 as books_database
import books_database_pb2_grpc as books_database_grpc

add_grpc_path(FILE, '../../../utils/pb/payment')
import payment_pb2 as payment
import payment_pb2_grpc as payment_grpc

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
        self.books_database_target = os.getenv("BOOKS_DATABASE_TARGET", "books_database_1:50056")
        self.payment_target = os.getenv("PAYMENT_TARGET", "payment:50057")
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

    def _db_stub(self):
        channel = grpc.insecure_channel(self.books_database_target)
        stub = books_database_grpc.BooksDatabaseStub(channel)
        return channel, stub

    def _payment_stub(self):
        channel = grpc.insecure_channel(self.payment_target)
        stub = payment_grpc.PaymentServiceStub(channel)
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

            try:
                order_data = json.loads(response.order_json)
                items = order_data.get("items", [])
                
                db_channel, db_stub = self._db_stub()
                payment_channel, payment_stub = self._payment_stub()
                
                try:
                    # Pre-Phase 1: Determine updates for database
                    db_items = []
                    can_fulfill = True
                    for item in items:
                        title = item.get("name")
                        quantity = item.get("quantity", 1)
                        
                        read_resp = db_stub.Read(
                            books_database.ReadRequest(title=title),
                            timeout=self.rpc_timeout_seconds
                        )
                        current_stock = read_resp.stock
                        
                        if current_stock >= quantity:
                            db_items.append(books_database.WriteRequest(
                                title=title,
                                new_stock=current_stock - quantity,
                                expected_current_stock=current_stock,
                                is_replica_update=False
                            ))
                        else:
                            can_fulfill = False
                            logger.warning("Not enough stock for %s. Required: %d, Available: %d", title, quantity, current_stock)
                            break
                            
                    if not can_fulfill:
                        logger.warning("Aborting order %s before 2PC due to insufficient stock", response.order_id)
                        continue
                        
                    # Phase 1: Prepare
                    logger.info("Starting Phase 1 (Prepare) for order %s", response.order_id)
                    db_ready = False
                    payment_ready = False
                    
                    try:
                        db_prep_resp = db_stub.Prepare(
                            books_database.PrepareRequest(order_id=response.order_id, items=db_items),
                            timeout=self.rpc_timeout_seconds
                        )
                        db_ready = db_prep_resp.ready
                    except grpc.RpcError as e:
                        logger.error("Database Prepare failed: %s", e)
                        
                    try:
                        pay_prep_resp = payment_stub.Prepare(
                            payment.PrepareRequest(order_id=response.order_id),
                            timeout=self.rpc_timeout_seconds
                        )
                        payment_ready = pay_prep_resp.ready
                    except grpc.RpcError as e:
                        logger.error("Payment Prepare failed: %s", e)
                        
                    # Phase 2: Commit or Abort
                    if db_ready and payment_ready:
                        logger.info("All participants ready. Starting Phase 2 (Commit) for order %s", response.order_id)
                        
                        try:
                            db_stub.Commit(books_database.CommitRequest(order_id=response.order_id), timeout=self.rpc_timeout_seconds)
                        except grpc.RpcError as e:
                            logger.error("Database Commit failed (needs recovery): %s", e)
                            
                        try:
                            payment_stub.Commit(payment.CommitRequest(order_id=response.order_id), timeout=self.rpc_timeout_seconds)
                        except grpc.RpcError as e:
                            logger.error("Payment Commit failed (needs recovery): %s", e)
                            
                        logger.info("Successfully committed order %s", response.order_id)
                    else:
                        logger.warning("Not all participants ready. Starting Phase 2 (Abort) for order %s", response.order_id)
                        
                        try:
                            db_stub.Abort(books_database.AbortRequest(order_id=response.order_id), timeout=self.rpc_timeout_seconds)
                        except grpc.RpcError as e:
                            logger.error("Database Abort failed: %s", e)
                            
                        try:
                            payment_stub.Abort(payment.AbortRequest(order_id=response.order_id), timeout=self.rpc_timeout_seconds)
                        except grpc.RpcError as e:
                            logger.error("Payment Abort failed: %s", e)
                            
                        logger.warning("Successfully aborted order %s", response.order_id)

                finally:
                    db_channel.close()
                    payment_channel.close()
            except Exception as e:
                logger.error("Failed to process order %s: %s", response.order_id, e)

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