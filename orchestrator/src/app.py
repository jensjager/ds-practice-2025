import json
import logging
import os
import uuid
from concurrent.futures import FIRST_COMPLETED, CancelledError, ThreadPoolExecutor, wait

import grpc
from flask import Flask, jsonify, request
from flask_cors import CORS

# OpenTelemetry instrumentation
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.grpc import GrpcInstrumentorClient
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Span, Status, StatusCode

from utils.other.clock_utils import (
    clock_from_proto,
    clock_to_log,
    empty_clock,
    merge_clocks,
)
from utils.other.runtime_utils import add_grpc_path, env_float, setup_logging

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
add_grpc_path(FILE, "../../../utils/pb/fraud_detection")
add_grpc_path(FILE, "../../../utils/pb/transaction_verification")
add_grpc_path(FILE, "../../../utils/pb/suggestions")
add_grpc_path(FILE, "../../../utils/pb/order_queue")

import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc
import order_queue_pb2 as order_queue
import order_queue_pb2_grpc as order_queue_grpc
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

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
metric_provider = MeterProvider(metric_readers=[metric_reader])
metrics.set_meter_provider(metric_provider)

# Get tracer and meter for manual instrumentation
tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)

# Create metrics
order_counter = meter.create_counter(
    "orchestrator.orders.total", description="Total number of orders processed"
)
order_approved_counter = meter.create_counter(
    "orchestrator.orders.approved", description="Number of approved orders"
)
order_rejected_counter = meter.create_counter(
    "orchestrator.orders.rejected", description="Number of rejected orders"
)
order_execution_time = meter.create_histogram(
    "orchestrator.order.execution.time",
    description="Time taken to process an order (ms)",
    unit="ms",
)
pending_orders_gauge = meter.create_up_down_counter(
    "orchestrator.orders.pending",
    description="Current number of pending orders",
    unit="1",
)

RPC_TIMEOUT_SECONDS = env_float("RPC_TIMEOUT_SECONDS", 5.0)


def build_service_config():
    return {
        "transaction_verification": {
            "target": os.getenv(
                "TRANSACTION_VERIFICATION_TARGET", "transaction_verification:50052"
            ),
            "stub_class": transaction_verification_grpc.TransactionVerificationStub,
            "module": transaction_verification,
        },
        "fraud_detection": {
            "target": os.getenv("FRAUD_DETECTION_TARGET", "fraud_detection:50051"),
            "stub_class": fraud_detection_grpc.FraudDetectionStub,
            "module": fraud_detection,
        },
        "suggestions": {
            "target": os.getenv("SUGGESTIONS_TARGET", "suggestions:50053"),
            "stub_class": suggestions_grpc.SuggestionsStub,
            "module": suggestions,
        },
    }


SERVICE_CONFIG = build_service_config()
ORDER_QUEUE_TARGET = os.getenv("ORDER_QUEUE_TARGET", "order_queue:50054")
EVENT_FLOW = [
    {
        "name": "validate_items",
        "deps": [],
        "label": "Validate items",
        "service": "transaction_verification",
        "method": "ValidateItems",
    },
    {
        "name": "validate_user_data",
        "deps": [],
        "label": "Validate user data",
        "service": "transaction_verification",
        "method": "ValidateUserData",
    },
    {
        "name": "check_stock",
        "deps": ["validate_items"],
        "label": "Check stock",
        "service": "transaction_verification",
        "method": "CheckStock",
    },
    {
        "name": "validate_card_format",
        "deps": ["check_stock"],
        "label": "Validate card format",
        "service": "transaction_verification",
        "method": "ValidateCardFormat",
    },
    {
        "name": "prepare_suggestions_context",
        "deps": ["validate_items"],
        "label": "Prepare suggestions context",
        "service": "suggestions",
        "method": "PrepareSuggestionsContext",
    },
    {
        "name": "check_user_fraud",
        "deps": ["validate_user_data"],
        "label": "Check user fraud",
        "service": "fraud_detection",
        "method": "CheckUserFraud",
    },
    {
        "name": "check_card_fraud",
        "deps": ["validate_card_format", "check_user_fraud"],
        "label": "Check card fraud",
        "service": "fraud_detection",
        "method": "CheckCardFraud",
    },
    {
        "name": "generate_suggestions",
        "deps": ["prepare_suggestions_context", "check_card_fraud"],
        "label": "Generate suggestions",
        "service": "suggestions",
        "method": "GenerateSuggestions",
    },
]


# For compatibility with response message conversions
def clock_from_message(clock_message):
    """Alias for clock_from_proto from shared utils."""
    return clock_from_proto(clock_message)


def make_vector_clock(service_name, clock):
    service_module = SERVICE_CONFIG[service_name]["module"]
    return service_module.VectorClock(**clock)


def call_service_rpc(service_name, method_name, request):
    service = SERVICE_CONFIG[service_name]
    with grpc.insecure_channel(service["target"]) as channel:
        stub = service["stub_class"](channel)
        method = getattr(stub, method_name)
        return method(request, timeout=RPC_TIMEOUT_SECONDS)


def enqueue_order(order_id, order_json):
    request = order_queue.EnqueueRequest(order_id=order_id, order_json=order_json)
    with grpc.insecure_channel(ORDER_QUEUE_TARGET) as channel:
        stub = order_queue_grpc.OrderQueueStub(channel)
        response = stub.Enqueue(request, timeout=RPC_TIMEOUT_SECONDS)
    return response


def init_order(service_name, order_id, order_json, clock):
    service_module = SERVICE_CONFIG[service_name]["module"]
    request = service_module.OrderInitRequest(
        order_id=order_id,
        order_json=order_json,
        vector_clock=make_vector_clock(service_name, clock),
    )
    return call_service_rpc(service_name, "InitOrder", request)


def execute_event(service_name, method_name, order_id, clock):
    service_module = SERVICE_CONFIG[service_name]["module"]
    request = service_module.EventRequest(
        order_id=order_id,
        vector_clock=make_vector_clock(service_name, clock),
    )
    return call_service_rpc(service_name, method_name, request)


def clear_order(service_name, order_id, clock):
    service_module = SERVICE_CONFIG[service_name]["module"]
    request = service_module.ClearOrderRequest(
        order_id=order_id,
        final_vector_clock=make_vector_clock(service_name, clock),
    )
    return call_service_rpc(service_name, "ClearOrder", request)


def initialize_order(order_id, order_json):
    initial_clock = empty_clock()
    init_calls = {
        service_name: (
            lambda service_name=service_name: init_order(
                service_name, order_id, order_json, initial_clock
            )
        )
        for service_name in SERVICE_CONFIG
    }

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_map = {
            executor.submit(call): service_name
            for service_name, call in init_calls.items()
        }
        for future, service_name in list(future_map.items()):
            try:
                response = future.result()
            except grpc.RpcError as exc:
                raise RuntimeError(
                    f"{service_name} init failed: {exc.code().name}"
                ) from exc

            if not response.success:
                raise RuntimeError(f"{service_name} init failed: {response.reason}")

    logger.info("Initialized backend state for order_id=%s", order_id)


def broadcast_cleanup(order_id, final_clock):
    cleanup_calls = {
        service_name: (
            lambda service_name=service_name: clear_order(
                service_name, order_id, final_clock
            )
        )
        for service_name in SERVICE_CONFIG
    }

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_map = {
            executor.submit(call): service_name
            for service_name, call in cleanup_calls.items()
        }
        for future, service_name in list(future_map.items()):
            try:
                response = future.result()
                logger.info(
                    "Cleanup service=%s order_id=%s success=%s reason=%s",
                    service_name,
                    order_id,
                    response.success,
                    response.reason,
                )
            except grpc.RpcError as exc:
                logger.error(
                    "Cleanup gRPC failure service=%s order_id=%s code=%s",
                    service_name,
                    order_id,
                    exc.code().name,
                )


def run_event_flow(order_id, order_json):
    initialize_order(order_id, order_json)

    completed_events = set()
    scheduled_events = set()
    pending_futures = {}
    event_clocks = {}
    suggested_books = []
    failure_message = None
    transport_error = None

    with ThreadPoolExecutor(max_workers=4) as executor:

        def schedule_ready_events():
            for event in EVENT_FLOW:
                name = event["name"]
                deps = event["deps"]
                if name in scheduled_events or failure_message or transport_error:
                    continue
                if all(dep in completed_events for dep in deps):
                    dependency_clock = merge_clocks(
                        *(event_clocks.get(dep, empty_clock()) for dep in deps)
                    )
                    future = executor.submit(
                        execute_event,
                        event["service"],
                        event["method"],
                        order_id,
                        dependency_clock,
                    )
                    pending_futures[future] = name
                    scheduled_events.add(name)
                    logger.info(
                        "Scheduled event=%s order_id=%s deps=%s vc=%s",
                        name,
                        order_id,
                        deps,
                        clock_to_log(dependency_clock),
                    )

        schedule_ready_events()

        while pending_futures:
            done, _ = wait(list(pending_futures.keys()), return_when=FIRST_COMPLETED)
            for future in done:
                event_name = pending_futures.pop(future)
                try:
                    response = future.result()
                except CancelledError:
                    logger.info("Cancelled event=%s order_id=%s", event_name, order_id)
                    continue
                except grpc.RpcError as exc:
                    transport_error = (
                        f"Event {event_name} failed with gRPC error {exc.code().name}"
                    )
                    logger.error("%s for order_id=%s", transport_error, order_id)
                    for pending_future in list(pending_futures.keys()):
                        pending_future.cancel()
                    continue
                except Exception as exc:
                    transport_error = (
                        f"Event {event_name} failed unexpectedly: {str(exc)}"
                    )
                    logger.exception("%s for order_id=%s", transport_error, order_id)
                    for pending_future in list(pending_futures.keys()):
                        pending_future.cancel()
                    continue

                response_clock = clock_from_message(
                    getattr(response, "vector_clock", None)
                )
                event_clocks[event_name] = response_clock
                completed_events.add(event_name)

                if not response.success:
                    failure_message = f"{response.event_name}: {response.reason}"
                    logger.warning(
                        "Event failure for order_id=%s: %s", order_id, failure_message
                    )
                    for pending_future in list(pending_futures.keys()):
                        pending_future.cancel()
                else:
                    logger.info(
                        "Completed event=%s order_id=%s vc=%s",
                        event_name,
                        order_id,
                        clock_to_log(response_clock),
                    )
                    if event_name == "generate_suggestions":
                        suggested_books = [
                            {
                                "bookId": book.book_id,
                                "title": book.title,
                                "author": book.author,
                            }
                            for book in response.books
                        ]

            if not failure_message and not transport_error:
                schedule_ready_events()

    final_clock = merge_clocks(*(event_clocks.values()))
    broadcast_cleanup(order_id, final_clock)

    if transport_error:
        raise RuntimeError(transport_error)

    if failure_message:
        return {
            "approved": False,
            "status": "Order Rejected",
            "suggested_books": [],
            "reason": failure_message,
            "vector_clock": final_clock,
        }

    return {
        "approved": True,
        "status": "Order Approved",
        "suggested_books": suggested_books,
        "reason": "Order flow completed successfully.",
        "vector_clock": final_clock,
    }


app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Initialize automatic instrumentation
FlaskInstrumentor().instrument_app(app)
RequestsInstrumentor().instrument()
GrpcInstrumentorClient().instrument()


@app.route("/", methods=["GET"])
def index():
    return "Orchestrator is running."


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


def error_response(code, message):
    return jsonify({"error": {"code": str(code), "message": message}}), code


def validate_request(data):
    if not isinstance(data, dict):
        return "Invalid JSON payload."
    return None


@app.route("/checkout", methods=["POST"])
def checkout():
    """
    Responds with a JSON object containing the order ID, status, and suggested books.
    """
    import time as time_module

    start_time = time_module.time()

    # Update pending orders gauge
    pending_orders_gauge.add(1, {"status": "pending"})

    with tracer.start_as_current_span("checkout_order") as span:
        span.set_attribute("http.method", "POST")
        span.set_attribute("http.route", "/checkout")

        try:
            request_data = request.get_json(force=True, silent=False)
        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, "Invalid JSON"))
            pending_orders_gauge.add(-1, {"status": "pending"})
            return error_response(400, "Request body must be valid JSON.")

        validation_error = validate_request(request_data)
        if validation_error:
            span.record_exception(Exception(validation_error))
            span.set_status(Status(StatusCode.ERROR, validation_error))
            pending_orders_gauge.add(-1, {"status": "pending"})
            return error_response(400, validation_error)

        order_id = str(uuid.uuid4())
        request_data["orderId"] = order_id
        logger.info("Accepted checkout request for order_id=%s", order_id)
        order_json = json.dumps(request_data)

        span.set_attribute("order.id", order_id)
        order_counter.add(1)

        try:
            flow_result = run_event_flow(order_id, order_json)
        except RuntimeError as exc:
            logger.error(
                "Downstream workflow failure for order_id=%s: %s", order_id, str(exc)
            )
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            pending_orders_gauge.add(-1, {"status": "pending"})
            order_rejected_counter.add(1)
            return error_response(500, str(exc))
        except Exception as exc:
            logger.exception(
                "Unexpected failure for order_id=%s: %s", order_id, str(exc)
            )
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            pending_orders_gauge.add(-1, {"status": "pending"})
            order_rejected_counter.add(1)
            return error_response(500, f"Unexpected error: {str(exc)}")

        response = {
            "orderId": order_id,
            "status": flow_result["status"],
            "suggestedBooks": flow_result["suggested_books"],
        }

        if flow_result["approved"]:
            order_approved_counter.add(1)
            span.set_attribute("order.status", "approved")

            try:
                enqueue_response = enqueue_order(order_id, order_json)
            except grpc.RpcError as exc:
                logger.error(
                    "Failed to enqueue approved order_id=%s because queue call failed: %s",
                    order_id,
                    exc.code().name,
                )
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, "Enqueue failed"))
                pending_orders_gauge.add(-1, {"status": "pending"})
                return error_response(
                    500, "Order approval succeeded, but enqueue failed."
                )

            if not enqueue_response.success:
                logger.error(
                    "Queue rejected approved order_id=%s reason=%s",
                    order_id,
                    enqueue_response.reason,
                )
                span.set_status(Status(StatusCode.ERROR, enqueue_response.reason))
                pending_orders_gauge.add(-1, {"status": "pending"})
                return error_response(
                    500,
                    f"Order approval succeeded, but enqueue failed: {enqueue_response.reason}",
                )

            logger.info(
                "Enqueued approved order_id=%s queue_size=%s",
                order_id,
                enqueue_response.queue_size,
            )
            span.set_attribute("queue.size", enqueue_response.queue_size)
        else:
            order_rejected_counter.add(1)
            span.set_attribute("order.status", "rejected")
            span.set_attribute("order.reason", flow_result["reason"])

        # Record execution time
        execution_time_ms = (time_module.time() - start_time) * 1000
        order_execution_time.record(execution_time_ms)
        span.set_attribute("execution.time.ms", execution_time_ms)

        pending_orders_gauge.add(-1, {"status": "completed"})

        logger.info(
            "Returning response for order_id=%s status=%s suggested_books=%s vc=%s",
            order_id,
            flow_result["status"],
            len(flow_result["suggested_books"]),
            clock_to_log(flow_result["vector_clock"]),
        )

        return jsonify(response)


if __name__ == "__main__":
    app.run(host="0.0.0.0")
