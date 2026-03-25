import json
import os
import re
import sys
import threading
from concurrent import futures

import grpc

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
transaction_grpc_path = os.path.abspath(
    os.path.join(FILE, '../../../utils/pb/transaction_verification')
)
sys.path.insert(0, transaction_grpc_path)
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

CLOCK_KEYS = ("transaction_verification", "fraud_detection", "suggestions")
SERVICE_KEY = "transaction_verification"
ORDER_STATES = {}
STATE_LOCK = threading.Lock()


def empty_clock():
    return {key: 0 for key in CLOCK_KEYS}


def clock_from_proto(clock):
    if clock is None:
        return empty_clock()
    return {key: int(getattr(clock, key, 0)) for key in CLOCK_KEYS}


def clock_to_proto(clock):
    return transaction_verification.VectorClock(**clock)


def merge_clocks(*clocks):
    merged = empty_clock()
    for clock in clocks:
        if not clock:
            continue
        for key in CLOCK_KEYS:
            merged[key] = max(merged[key], int(clock.get(key, 0)))
    return merged


def increment_clock(clock):
    clock[SERVICE_KEY] += 1


def clock_to_log(clock):
    return json.dumps(clock, sort_keys=True)


def event_response(event_name, success, reason, clock):
    return transaction_verification.EventResponse(
        success=success,
        reason=reason,
        event_name=event_name,
        vector_clock=clock_to_proto(clock),
    )


class TransactionVerificationService(
    transaction_verification_grpc.TransactionVerificationServicer
):
    def InitOrder(self, request, context):
        try:
            order = json.loads(request.order_json) if request.order_json else {}
        except json.JSONDecodeError:
            print("[transaction_verification] Failed to cache invalid order payload")
            return transaction_verification.OrderInitResponse(
                success=False,
                reason="Invalid order payload.",
                vector_clock=clock_to_proto(empty_clock()),
            )

        order_id = request.order_id.strip() or str(order.get("orderId", "unknown")).strip()
        initial_clock = merge_clocks(clock_from_proto(request.vector_clock))

        with STATE_LOCK:
            ORDER_STATES[order_id] = {
                "order": order,
                "vector_clock": initial_clock,
                "events": {},
                "failed": False,
            }

        print(
            f"[transaction_verification] Cached order_id={order_id} vc={clock_to_log(initial_clock)}"
        )
        return transaction_verification.OrderInitResponse(
            success=True,
            reason="Order cached.",
            vector_clock=clock_to_proto(initial_clock),
        )

    def _execute_event(self, order_id, incoming_clock, event_name, handler):
        with STATE_LOCK:
            state = ORDER_STATES.get(order_id)
            if state is None:
                return event_response(event_name, False, "Order state not found.", empty_clock())

            state["vector_clock"] = merge_clocks(state["vector_clock"], incoming_clock)
            increment_clock(state["vector_clock"])
            current_clock = dict(state["vector_clock"])
            order = state["order"]

        print(
            f"[transaction_verification] order_id={order_id} event={event_name} vc={clock_to_log(current_clock)}"
        )
        success, reason = handler(order)

        with STATE_LOCK:
            state = ORDER_STATES.get(order_id)
            if state is not None:
                state["events"][event_name] = {"success": success, "reason": reason}
                if not success:
                    state["failed"] = True
                current_clock = dict(state["vector_clock"])

        print(
            f"[transaction_verification] order_id={order_id} event={event_name} success={success} reason={reason}"
        )
        return event_response(event_name, success, reason, current_clock)

    def ValidateItems(self, request, context):
        return self._execute_event(
            request.order_id,
            clock_from_proto(request.vector_clock),
            "validate_items",
            self._validate_items,
        )

    def ValidateUserData(self, request, context):
        return self._execute_event(
            request.order_id,
            clock_from_proto(request.vector_clock),
            "validate_user_data",
            self._validate_user_data,
        )

    def ValidateCardFormat(self, request, context):
        return self._execute_event(
            request.order_id,
            clock_from_proto(request.vector_clock),
            "validate_card_format",
            self._validate_card_format,
        )

    def ClearOrder(self, request, context):
        order_id = request.order_id
        final_clock = clock_from_proto(request.final_vector_clock)

        with STATE_LOCK:
            state = ORDER_STATES.get(order_id)
            if state is None:
                print(f"[transaction_verification] Cleanup skipped for order_id={order_id}: already cleared")
                return transaction_verification.ClearOrderResponse(
                    success=True,
                    reason="Order state already cleared.",
                    vector_clock=clock_to_proto(empty_clock()),
                )

            local_clock = dict(state["vector_clock"])
            is_safe_to_clear = all(local_clock[key] <= final_clock[key] for key in CLOCK_KEYS)
            if is_safe_to_clear:
                del ORDER_STATES[order_id]

        if is_safe_to_clear:
            print(
                f"[transaction_verification] Cleared order_id={order_id} final_vc={clock_to_log(final_clock)}"
            )
            return transaction_verification.ClearOrderResponse(
                success=True,
                reason="Order state cleared.",
                vector_clock=clock_to_proto(local_clock),
            )

        print(
            f"[transaction_verification] Cleanup rejected for order_id={order_id}: local_vc={clock_to_log(local_clock)} final_vc={clock_to_log(final_clock)}"
        )
        return transaction_verification.ClearOrderResponse(
            success=False,
            reason="Local vector clock is ahead of the final vector clock.",
            vector_clock=clock_to_proto(local_clock),
        )

    @staticmethod
    def _validate_items(order):
        items = order.get("items", []) or []
        if not isinstance(items, list) or not items:
            return False, "Items list is required."
        if any(int(item.get("quantity", 0)) <= 0 for item in items):
            return False, "Item quantities must be positive."
        return True, "Items validated."

    @staticmethod
    def _validate_user_data(order):
        user = order.get("user", {}) or {}
        billing_address = order.get("billingAddress", {}) or {}

        missing_fields = []
        if not str(user.get("name", "")).strip():
            missing_fields.append("user.name")
        if not str(user.get("contact", "")).strip():
            missing_fields.append("user.contact")
        for field in ("street", "city", "country"):
            if not str(billing_address.get(field, "")).strip():
                missing_fields.append(f"billingAddress.{field}")

        if missing_fields:
            return False, f"Missing mandatory fields: {', '.join(missing_fields)}"
        return True, "User data validated."

    @staticmethod
    def _validate_card_format(order):
        card = order.get("creditCard", {}) or {}
        card_number = str(card.get("number", "")).replace(" ", "")
        expiration_date = str(card.get("expirationDate", "")).strip()
        cvv = str(card.get("cvv", "")).strip()

        if not card_number.isdigit() or not 13 <= len(card_number) <= 19:
            return False, "Invalid credit card number."
        if not re.match(r"^(0[1-9]|1[0-2])\/\d{2}$", expiration_date):
            return False, "Invalid expiration date."
        if not cvv.isdigit() or len(cvv) not in (3, 4):
            return False, "Invalid CVV."
        return True, "Credit card format validated."


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    transaction_verification_grpc.add_TransactionVerificationServicer_to_server(
        TransactionVerificationService(),
        server,
    )
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started. Listening on port 50052.")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
