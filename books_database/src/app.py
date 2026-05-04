import logging
import os
import threading
from concurrent import futures

import grpc

from utils.other.runtime_utils import add_grpc_path, env_int, setup_logging

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
add_grpc_path(FILE, '../../../utils/pb/books_database')

import books_database_pb2 as books_database
import books_database_pb2_grpc as books_database_grpc

setup_logging()
logger = logging.getLogger(__name__)


class BooksDatabaseServicer(books_database_grpc.BooksDatabaseServicer):
    def __init__(self):
        self.port = env_int("BOOKS_DATABASE_PORT", 50056)
        self.is_primary = os.getenv("IS_PRIMARY", "false").lower() == "true"
        self.primary_target = os.getenv("PRIMARY_TARGET", "")
        self.backup_targets = [t.strip() for t in os.getenv("BACKUP_TARGETS", "").split(",") if t.strip()]
        
        self.store = {}
        stock_file = os.path.join(os.path.dirname(__file__), "initial_stock.json")
        if os.path.exists(stock_file):
            import json
            try:
                with open(stock_file, "r") as f:
                    self.store = json.load(f)
                logger.info(f"Loaded initial stock from {stock_file}")
            except Exception as e:
                logger.error(f"Failed to load initial stock: {e}")
                
        self.lock = threading.Lock()
        
        logger.info(f"Initialized database. Primary: {self.is_primary}")

    def _get_stub(self, target):
        channel = grpc.insecure_channel(target)
        return books_database_grpc.BooksDatabaseStub(channel), channel

    def Read(self, request, context):
        with self.lock:
            stock = self.store.get(request.title, 0)
        logger.info(f"Read title='{request.title}', stock={stock}")
        return books_database.ReadResponse(stock=stock)

    def Write(self, request, context):
        if request.is_replica_update:
            # Replicating write to this backup node
            if self.is_primary:
                logger.warning("Primary received a replica update! Ignoring.")
                return books_database.WriteResponse(success=False)
            
            with self.lock:
                self.store[request.title] = request.new_stock
            logger.info(f"Replica updated title='{request.title}' to {request.new_stock}")
            return books_database.WriteResponse(success=True)

        if not self.is_primary:
            # Forward to primary
            logger.info(f"Forwarding write for title='{request.title}' to primary {self.primary_target}")
            stub, channel = self._get_stub(self.primary_target)
            try:
                # We forward as a normal client request
                forward_req = books_database.WriteRequest(
                    title=request.title,
                    new_stock=request.new_stock,
                    expected_current_stock=request.expected_current_stock,
                    is_replica_update=False
                )
                response = stub.Write(forward_req, timeout=3.0)
                return response
            except grpc.RpcError as e:
                logger.error(f"Failed to forward write to primary: {e.code().name}")
                return books_database.WriteResponse(success=False)
            finally:
                channel.close()

        # Primary processing
        with self.lock:
            current_stock = self.store.get(request.title, 0)
            if current_stock != request.expected_current_stock:
                logger.warning(f"Concurrent write detected! Expected {request.expected_current_stock}, got {current_stock}")
                return books_database.WriteResponse(success=False)
            
            # Apply locally
            self.store[request.title] = request.new_stock
            
            # Replicate to backups
            replication_success = True
            for target in self.backup_targets:
                stub, channel = self._get_stub(target)
                try:
                    rep_req = books_database.WriteRequest(
                        title=request.title,
                        new_stock=request.new_stock,
                        expected_current_stock=request.expected_current_stock,
                        is_replica_update=True
                    )
                    stub.Write(rep_req, timeout=3.0)
                except grpc.RpcError as e:
                    logger.error(f"Failed to replicate to {target}: {e.code().name}")
                    replication_success = False
                finally:
                    channel.close()

        logger.info(f"Primary updated title='{request.title}' to {request.new_stock} and replicated: {replication_success}")
        return books_database.WriteResponse(success=replication_success)


def serve():
    servicer = BooksDatabaseServicer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    books_database_grpc.add_BooksDatabaseServicer_to_server(servicer, server)
    port = str(servicer.port)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"Books Database started on port {port}")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
