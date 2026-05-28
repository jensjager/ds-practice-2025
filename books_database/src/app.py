import logging
import os
import threading
from concurrent import futures

import grpc

from utils.other.runtime_utils import add_grpc_path, env_int, setup_logging

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
add_grpc_path(FILE, "../../../utils/pb/books_database")

import books_database_pb2 as books_database
import books_database_pb2_grpc as books_database_grpc

# OpenTelemetry instrumentation
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.grpc import (
    GrpcInstrumentorClient,
    GrpcInstrumentorServer,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import View
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode

setup_logging()
logger = logging.getLogger(__name__)

# Setup OpenTelemetry
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://observability:4317")

# Configure trace provider and exporter
trace_provider = TracerProvider()
span_exporter = OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)
trace_provider.add_span_processor(BatchSpanProcessor(span_exporter))
trace.set_tracer_provider(trace_provider)

# Configure metric provider and exporter
metric_exporter = OTLPMetricExporter(endpoint=OTEL_ENDPOINT, insecure=True)
metric_reader = PeriodicExportingMetricReader(
    metric_exporter, export_interval_millis=1000
)
metric_provider = MeterProvider(
    resource=Resource.create({"service.name": "books_database"}),
    metric_readers=[metric_reader],
)
metrics.set_meter_provider(metric_provider)

# Initialize automatic instrumentation
GrpcInstrumentorServer().instrument()
GrpcInstrumentorClient().instrument()

# Get tracer and meter for manual instrumentation
tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)

# Create metrics
db_read_counter = meter.create_counter(
    "books_database.reads", description="Number of read operations"
)
db_write_counter = meter.create_counter(
    "books_database.writes", description="Number of write operations"
)
db_prepare_counter = meter.create_counter(
    "books_database.prepares", description="Number of prepare operations"
)
db_commit_counter = meter.create_counter(
    "books_database.commits", description="Number of commit operations"
)
db_abort_counter = meter.create_counter(
    "books_database.aborts", description="Number of abort operations"
)

# Create UpDownCounter for stock levels (acts as Asynchronous Gauge)
stock_gauge = meter.create_up_down_counter(
    "books_database.stock.level",
    description="Current stock level for each book",
    unit="1",
)


class BooksDatabaseServicer(books_database_grpc.BooksDatabaseServicer):
    def __init__(self):
        self.port = env_int("BOOKS_DATABASE_PORT", 50056)
        self.is_primary = os.getenv("IS_PRIMARY", "false").lower() == "true"
        self.primary_target = os.getenv("PRIMARY_TARGET", "")
        self.backup_targets = [
            t.strip() for t in os.getenv("BACKUP_TARGETS", "").split(",") if t.strip()
        ]

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
        self.temp_updates = {}

        # Initialize stock gauge with current values from store
        with self.lock:
            for title, stock in self.store.items():
                stock_gauge.add(stock, {"book": title})

        logger.info(f"Initialized database. Primary: {self.is_primary}")

    def _get_stub(self, target):
        channel = grpc.insecure_channel(target)
        return books_database_grpc.BooksDatabaseStub(channel), channel

    def Read(self, request, context):
        with tracer.start_as_current_span("Read") as span:
            span.set_attribute("db.operation", "read")
            span.set_attribute("book.title", request.title)

            with self.lock:
                stock = self.store.get(request.title, 0)

            logger.info(f"Read title='{request.title}', stock={stock}")
            db_read_counter.add(1)
            span.set_attribute("book.stock", stock)
            return books_database.ReadResponse(stock=stock)

    def Write(self, request, context):
        with tracer.start_as_current_span("Write") as span:
            span.set_attribute("db.operation", "write")
            span.set_attribute("book.title", request.title)
            span.set_attribute("new_stock", request.new_stock)
            span.set_attribute("is_replica_update", request.is_replica_update)

            if request.is_replica_update:
                # Replicating write to this backup node
                if self.is_primary:
                    logger.warning("Primary received a replica update! Ignoring.")
                    db_write_counter.add(1, {"type": "replica_rejected"})
                    span.set_status(
                        Status(StatusCode.ERROR, "Replica update on primary")
                    )
                    return books_database.WriteResponse(success=False)

                with self.lock:
                    old_stock = self.store.get(request.title, 0)
                    self.store[request.title] = request.new_stock
                    # Update stock gauge using add with delta
                    stock_gauge.add(
                        request.new_stock - old_stock, {"book": request.title}
                    )

                logger.info(
                    f"Replica updated title='{request.title}' to {request.new_stock}"
                )
                db_write_counter.add(1, {"type": "replica"})
                return books_database.WriteResponse(success=True)

            if not self.is_primary:
                # Forward to primary
                logger.info(
                    f"Forwarding write for title='{request.title}' to primary {self.primary_target}"
                )
                span.add_event("forwarding_to_primary")
                stub, channel = self._get_stub(self.primary_target)
                try:
                    # We forward as a normal client request
                    forward_req = books_database.WriteRequest(
                        title=request.title,
                        new_stock=request.new_stock,
                        expected_current_stock=request.expected_current_stock,
                        is_replica_update=False,
                    )
                    response = stub.Write(forward_req, timeout=3.0)
                    db_write_counter.add(1, {"type": "forwarded"})
                    return response
                except grpc.RpcError as e:
                    logger.error(f"Failed to forward write to primary: {e.code().name}")
                    span.record_exception(e)
                    db_write_counter.add(1, {"type": "forward_failed"})
                    return books_database.WriteResponse(success=False)
                finally:
                    channel.close()

            # Primary processing
            with self.lock:
                current_stock = self.store.get(request.title, 0)
                if current_stock != request.expected_current_stock:
                    logger.warning(
                        f"Concurrent write detected! Expected {request.expected_current_stock}, got {current_stock}"
                    )
                    span.set_status(
                        Status(StatusCode.ERROR, "Concurrent write detected")
                    )
                    db_write_counter.add(1, {"type": "concurrent_conflict"})
                    return books_database.WriteResponse(success=False)

                # Apply locally
                old_stock = self.store.get(request.title, 0)
                self.store[request.title] = request.new_stock
                # Update stock gauge using add with delta
                stock_gauge.add(request.new_stock - old_stock, {"book": request.title})

                # Replicate to backups
                replication_success = True
                for target in self.backup_targets:
                    stub, channel = self._get_stub(target)
                    try:
                        rep_req = books_database.WriteRequest(
                            title=request.title,
                            new_stock=request.new_stock,
                            expected_current_stock=request.expected_current_stock,
                            is_replica_update=True,
                        )
                        stub.Write(rep_req, timeout=3.0)
                    except grpc.RpcError as e:
                        logger.error(
                            f"Failed to replicate to {target}: {e.code().name}"
                        )
                        replication_success = False
                        span.record_exception(e)
                    finally:
                        channel.close()

            db_write_counter.add(1, {"type": "primary"})
            logger.info(
                f"Primary updated title='{request.title}' to {request.new_stock} and replicated: {replication_success}"
            )
            span.set_attribute("replication_success", replication_success)
            return books_database.WriteResponse(success=replication_success)

    def Prepare(self, request, context):
        with tracer.start_as_current_span("Prepare") as span:
            span.set_attribute("order.id", request.order_id)
            span.set_attribute("num_items", len(request.items))

            with self.lock:
                # Check if we can fulfill all updates in the transaction
                for item in request.items:
                    current_stock = self.store.get(item.title, 0)
                    # Stock checks and consistency will be validated against expected_current_stock
                    if current_stock != item.expected_current_stock:
                        logger.warning(
                            f"Prepare failed: Concurrent write or insufficient stock for {item.title}. Expected {item.expected_current_stock}, got {current_stock}"
                        )
                        span.set_status(Status(StatusCode.ERROR, "Stock mismatch"))
                        span.record_exception(Exception("Stock mismatch"))
                        db_prepare_counter.add(1, {"status": "failed"})
                        return books_database.PrepareResponse(ready=False)
                    # Validate new stock is non-negative
                    if item.new_stock < 0:
                        logger.warning(
                            f"Prepare failed: Invalid stock value {item.new_stock} for {item.title}"
                        )
                        span.set_status(Status(StatusCode.ERROR, "Invalid stock"))
                        span.record_exception(Exception("Invalid stock value"))
                        db_prepare_counter.add(1, {"status": "failed"})
                        return books_database.PrepareResponse(ready=False)

                # Store tentatively
                self.temp_updates[request.order_id] = request.items

            logger.info(f"Prepared transaction for order {request.order_id}")
            db_prepare_counter.add(1, {"status": "success"})
            span.set_attribute("ready", True)
            return books_database.PrepareResponse(ready=True)

    def Commit(self, request, context):
        with tracer.start_as_current_span("Commit") as span:
            span.set_attribute("order.id", request.order_id)

            with self.lock:
                items = self.temp_updates.pop(request.order_id, None)
                if not items:
                    logger.warning(
                        f"Commit failed: transaction not prepared for order {request.order_id}"
                    )
                    span.set_status(
                        Status(StatusCode.ERROR, "Transaction not prepared")
                    )
                    db_commit_counter.add(1, {"status": "failed"})
                    return books_database.CommitResponse(success=False)

                # Re-validate: check current stock matches expected for all items
                for item in items:
                    current_stock = self.store.get(item.title, 0)
                    if current_stock != item.expected_current_stock:
                        logger.warning(
                            f"Commit failed: Concurrent write for {item.title}. Expected {item.expected_current_stock}, got {current_stock}"
                        )
                        span.set_status(
                            Status(StatusCode.ERROR, "Concurrent write during commit")
                        )
                        span.record_exception(Exception("Stock mismatch on commit"))
                        db_commit_counter.add(1, {"status": "failed"})
                        # Put items back for potential retry
                        self.temp_updates[request.order_id] = items
                        return books_database.CommitResponse(success=False)

                # Apply updates
                for item in items:
                    old_stock = self.store.get(item.title, 0)
                    self.store[item.title] = item.new_stock
                    # Update stock gauge after commit using add with delta
                    stock_gauge.add(item.new_stock - old_stock, {"book": item.title})

            # Replicate to backups atomically
            replication_success = True
            if self.backup_targets:
                for target in self.backup_targets:
                    stub, channel = self._get_stub(target)
                    try:
                        for item in items:
                            rep_req = books_database.WriteRequest(
                                title=item.title,
                                new_stock=item.new_stock,
                                expected_current_stock=item.expected_current_stock,
                                is_replica_update=True,
                            )
                            stub.Write(rep_req, timeout=3.0)
                    except grpc.RpcError as e:
                        logger.error(
                            f"Failed to replicate commit to {target}: {e.code().name}"
                        )
                        span.record_exception(e)
                        replication_success = False
                        # Rollback local changes if replication fails
                        with self.lock:
                            for item in items:
                                self.store[item.title] = item.expected_current_stock
                                stock_gauge.add(
                                    item.expected_current_stock - item.new_stock,
                                    {"book": item.title},
                                )
                        break
                    finally:
                        channel.close()

            if replication_success:
                logger.info(f"Committed transaction for order {request.order_id}")
                db_commit_counter.add(1, {"status": "success"})
                span.set_attribute("success", True)
                return books_database.CommitResponse(success=True)
            else:
                logger.warning(
                    f"Commit rolled back for order {request.order_id} due to replication failure"
                )
                db_commit_counter.add(1, {"status": "failed"})
                span.set_status(Status(StatusCode.ERROR, "Replication failed"))
                return books_database.CommitResponse(success=False)

    def Abort(self, request, context):
        with tracer.start_as_current_span("Abort") as span:
            span.set_attribute("order.id", request.order_id)

            with self.lock:
                self.temp_updates.pop(request.order_id, None)

            logger.info(f"Aborted transaction for order {request.order_id}")
            db_abort_counter.add(1, {"status": "success"})
            span.set_attribute("aborted", True)
            return books_database.AbortResponse(aborted=True)


def serve():
    servicer = BooksDatabaseServicer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    books_database_grpc.add_BooksDatabaseServicer_to_server(servicer, server)
    port = str(servicer.port)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"Books Database started on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
