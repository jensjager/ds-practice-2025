import json
import os
import sys
import uuid
from concurrent.futures import CancelledError, FIRST_COMPLETED, ThreadPoolExecutor, wait

import grpc
from flask import Flask, jsonify, request
from flask_cors import CORS

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")


def add_grpc_path(relative_path):
    sys.path.insert(0, os.path.abspath(os.path.join(FILE, relative_path)))


add_grpc_path('../../../utils/pb/fraud_detection')
add_grpc_path('../../../utils/pb/transaction_verification')
add_grpc_path('../../../utils/pb/suggestions')

import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

CLOCK_KEYS = ("transaction_verification", "fraud_detection", "suggestions")
EVENT_FLOW = [
    ("validate_items", [], "Validate items", lambda order_id, clock: validate_items(order_id, clock)),
    ("validate_user_data", [], "Validate user data", lambda order_id, clock: validate_user_data(order_id, clock)),
    (
        "validate_card_format",
        ["validate_items"],
        "Validate card format",
        lambda order_id, clock: validate_card_format(order_id, clock),
    ),
    (
        "prepare_suggestions_context",
        ["validate_items"],
        "Prepare suggestions context",
        lambda order_id, clock: prepare_suggestions_context(order_id, clock),
    ),
    (
        "check_user_fraud",
        ["validate_user_data"],
        "Check user fraud",
        lambda order_id, clock: check_user_fraud(order_id, clock),
    ),
    (
        "check_card_fraud",
        ["validate_card_format", "check_user_fraud"],
        "Check card fraud",
        lambda order_id, clock: check_card_fraud(order_id, clock),
    ),
    (
        "generate_suggestions",
        ["prepare_suggestions_context", "check_card_fraud"],
        "Generate suggestions",
        lambda order_id, clock: generate_suggestions(order_id, clock),
    ),
]


def empty_clock():
    return {key: 0 for key in CLOCK_KEYS}


def merge_clocks(*clocks):
    merged = empty_clock()
    for clock in clocks:
        if not clock:
            continue
        for key in CLOCK_KEYS:
            merged[key] = max(merged[key], int(clock.get(key, 0)))
    return merged


def clock_from_message(clock_message):
    if clock_message is None:
        return empty_clock()
    return {key: int(getattr(clock_message, key, 0)) for key in CLOCK_KEYS}


def clock_to_log(clock):
    return json.dumps(clock, sort_keys=True)


def tx_clock(clock):
    return transaction_verification.VectorClock(**clock)


def fraud_clock(clock):
    return fraud_detection.VectorClock(**clock)


def suggestions_clock(clock):
    return suggestions.VectorClock(**clock)


def init_transaction_order(order_id, order_json, clock):
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        stub = transaction_verification_grpc.TransactionVerificationStub(channel)
        return stub.InitOrder(
            transaction_verification.OrderInitRequest(
                order_id=order_id,
                order_json=order_json,
                vector_clock=tx_clock(clock),
            ),
            timeout=5.0,
        )


def init_fraud_order(order_id, order_json, clock):
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudDetectionStub(channel)
        return stub.InitOrder(
            fraud_detection.OrderInitRequest(
                order_id=order_id,
                order_json=order_json,
                vector_clock=fraud_clock(clock),
            ),
            timeout=5.0,
        )


def init_suggestions_order(order_id, order_json, clock):
    with grpc.insecure_channel('suggestions:50053') as channel:
        stub = suggestions_grpc.SuggestionsStub(channel)
        return stub.InitOrder(
            suggestions.OrderInitRequest(
                order_id=order_id,
                order_json=order_json,
                vector_clock=suggestions_clock(clock),
            ),
            timeout=5.0,
        )


def validate_items(order_id, clock):
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        stub = transaction_verification_grpc.TransactionVerificationStub(channel)
        return stub.ValidateItems(
            transaction_verification.EventRequest(order_id=order_id, vector_clock=tx_clock(clock)),
            timeout=5.0,
        )


def validate_user_data(order_id, clock):
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        stub = transaction_verification_grpc.TransactionVerificationStub(channel)
        return stub.ValidateUserData(
            transaction_verification.EventRequest(order_id=order_id, vector_clock=tx_clock(clock)),
            timeout=5.0,
        )


def validate_card_format(order_id, clock):
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        stub = transaction_verification_grpc.TransactionVerificationStub(channel)
        return stub.ValidateCardFormat(
            transaction_verification.EventRequest(order_id=order_id, vector_clock=tx_clock(clock)),
            timeout=5.0,
        )


def check_user_fraud(order_id, clock):
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudDetectionStub(channel)
        return stub.CheckUserFraud(
            fraud_detection.EventRequest(order_id=order_id, vector_clock=fraud_clock(clock)),
            timeout=5.0,
        )


def check_card_fraud(order_id, clock):
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudDetectionStub(channel)
        return stub.CheckCardFraud(
            fraud_detection.EventRequest(order_id=order_id, vector_clock=fraud_clock(clock)),
            timeout=5.0,
        )


def prepare_suggestions_context(order_id, clock):
    with grpc.insecure_channel('suggestions:50053') as channel:
        stub = suggestions_grpc.SuggestionsStub(channel)
        return stub.PrepareSuggestionsContext(
            suggestions.EventRequest(order_id=order_id, vector_clock=suggestions_clock(clock)),
            timeout=5.0,
        )


def generate_suggestions(order_id, clock):
    with grpc.insecure_channel('suggestions:50053') as channel:
        stub = suggestions_grpc.SuggestionsStub(channel)
        return stub.GenerateSuggestions(
            suggestions.EventRequest(order_id=order_id, vector_clock=suggestions_clock(clock)),
            timeout=5.0,
        )


def clear_transaction_order(order_id, clock):
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        stub = transaction_verification_grpc.TransactionVerificationStub(channel)
        return stub.ClearOrder(
            transaction_verification.ClearOrderRequest(
                order_id=order_id,
                final_vector_clock=tx_clock(clock),
            ),
            timeout=5.0,
        )


def clear_fraud_order(order_id, clock):
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudDetectionStub(channel)
        return stub.ClearOrder(
            fraud_detection.ClearOrderRequest(
                order_id=order_id,
                final_vector_clock=fraud_clock(clock),
            ),
            timeout=5.0,
        )


def clear_suggestions_order(order_id, clock):
    with grpc.insecure_channel('suggestions:50053') as channel:
        stub = suggestions_grpc.SuggestionsStub(channel)
        return stub.ClearOrder(
            suggestions.ClearOrderRequest(
                order_id=order_id,
                final_vector_clock=suggestions_clock(clock),
            ),
            timeout=5.0,
        )


def initialize_order(order_id, order_json):
    initial_clock = empty_clock()
    init_calls = {
        "transaction_verification": lambda: init_transaction_order(order_id, order_json, initial_clock),
        "fraud_detection": lambda: init_fraud_order(order_id, order_json, initial_clock),
        "suggestions": lambda: init_suggestions_order(order_id, order_json, initial_clock),
    }

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_map = {executor.submit(call): service_name for service_name, call in init_calls.items()}
        for future, service_name in list(future_map.items()):
            try:
                response = future.result()
            except grpc.RpcError as exc:
                raise RuntimeError(f"{service_name} init failed: {exc.code().name}") from exc

            if not response.success:
                raise RuntimeError(f"{service_name} init failed: {response.reason}")

    print(f"[orchestrator] Initialized backend state for order_id={order_id}")


def broadcast_cleanup(order_id, final_clock):
    cleanup_calls = {
        "transaction_verification": lambda: clear_transaction_order(order_id, final_clock),
        "fraud_detection": lambda: clear_fraud_order(order_id, final_clock),
        "suggestions": lambda: clear_suggestions_order(order_id, final_clock),
    }

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_map = {executor.submit(call): service_name for service_name, call in cleanup_calls.items()}
        for future, service_name in list(future_map.items()):
            try:
                response = future.result()
                print(
                    f"[orchestrator] Cleanup service={service_name} order_id={order_id} success={response.success} reason={response.reason}"
                )
            except grpc.RpcError as exc:
                print(
                    f"[orchestrator] Cleanup gRPC failure service={service_name} order_id={order_id} code={exc.code().name}"
                )


def run_event_flow(order_id, order_json):
    initialize_order(order_id, order_json)

    event_definitions = {
        name: {"deps": deps, "label": label, "call": call}
        for name, deps, label, call in EVENT_FLOW
    }
    completed_events = set()
    scheduled_events = set()
    pending_futures = {}
    event_clocks = {}
    suggested_books = []
    failure_message = None
    transport_error = None

    with ThreadPoolExecutor(max_workers=4) as executor:
        def schedule_ready_events():
            for name, deps, label, call in EVENT_FLOW:
                if name in scheduled_events or failure_message or transport_error:
                    continue
                if all(dep in completed_events for dep in deps):
                    dependency_clock = merge_clocks(*(event_clocks.get(dep, empty_clock()) for dep in deps))
                    future = executor.submit(call, order_id, dependency_clock)
                    pending_futures[future] = name
                    scheduled_events.add(name)
                    print(
                        f"[orchestrator] Scheduled event={name} order_id={order_id} deps={deps} vc={clock_to_log(dependency_clock)}"
                    )

        schedule_ready_events()

        while pending_futures:
            done, _ = wait(list(pending_futures.keys()), return_when=FIRST_COMPLETED)
            for future in done:
                event_name = pending_futures.pop(future)
                try:
                    response = future.result()
                except CancelledError:
                    print(f"[orchestrator] Cancelled event={event_name} order_id={order_id}")
                    continue
                except grpc.RpcError as exc:
                    transport_error = f"Event {event_name} failed with gRPC error {exc.code().name}"
                    print(f"[orchestrator] {transport_error} for order_id={order_id}")
                    for pending_future in list(pending_futures.keys()):
                        pending_future.cancel()
                    continue
                except Exception as exc:
                    transport_error = f"Event {event_name} failed unexpectedly: {str(exc)}"
                    print(f"[orchestrator] {transport_error} for order_id={order_id}")
                    for pending_future in list(pending_futures.keys()):
                        pending_future.cancel()
                    continue

                response_clock = clock_from_message(getattr(response, "vector_clock", None))
                event_clocks[event_name] = response_clock
                completed_events.add(event_name)

                if not response.success:
                    failure_message = f"{response.event_name}: {response.reason}"
                    print(f"[orchestrator] Event failure for order_id={order_id}: {failure_message}")
                    for pending_future in list(pending_futures.keys()):
                        pending_future.cancel()
                else:
                    print(
                        f"[orchestrator] Completed event={event_name} order_id={order_id} vc={clock_to_log(response_clock)}"
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
CORS(app, resources={r'/*': {'origins': '*'}})


@app.route('/', methods=['GET'])
def index():
    return "Orchestrator is running."


def error_response(code, message):
    return jsonify({"error": {"code": str(code), "message": message}}), code


def validate_request(data):
    if not isinstance(data, dict):
        return "Invalid JSON payload."
    return None


@app.route('/checkout', methods=['POST'])
def checkout():
    """
    Responds with a JSON object containing the order ID, status, and suggested books.
    """
    try:
        request_data = request.get_json(force=True, silent=False)
    except Exception:
        return error_response(400, "Request body must be valid JSON.")

    validation_error = validate_request(request_data)
    if validation_error:
        return error_response(400, validation_error)

    order_id = str(uuid.uuid4())
    request_data["orderId"] = order_id
    print(f"[orchestrator] Accepted checkout request for order_id={order_id}")
    order_json = json.dumps(request_data)

    try:
        flow_result = run_event_flow(order_id, order_json)
    except RuntimeError as exc:
        print(f"[orchestrator] Downstream workflow failure for order_id={order_id}: {str(exc)}")
        return error_response(500, str(exc))
    except Exception as exc:
        print(f"[orchestrator] Unexpected failure for order_id={order_id}: {str(exc)}")
        return error_response(500, f"Unexpected error: {str(exc)}")

    response = {
        "orderId": order_id,
        "status": flow_result["status"],
        "suggestedBooks": flow_result["suggested_books"],
    }

    print(
        f"[orchestrator] Returning response for order_id={order_id}: status={flow_result['status']}, suggested_books={len(flow_result['suggested_books'])}, vc={clock_to_log(flow_result['vector_clock'])}"
    )

    return jsonify(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0')
