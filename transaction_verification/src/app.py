import json
import logging
import os
import re
import threading
from concurrent import futures

import grpc

from utils.other.clock_utils import empty_clock, merge_clocks, clock_from_proto, clock_to_log
from utils.other.order_state import (
    clear_order_if_safe,
    record_event_result,
    set_state,
    start_cleanup_thread,
    update_for_event,
)
from utils.other.runtime_utils import add_grpc_path, env_float, setup_logging

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
add_grpc_path(FILE, '../../../utils/pb/transaction_verification')
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

add_grpc_path(FILE, '../../../utils/pb/books_database')
import books_database_pb2 as books_database
import books_database_pb2_grpc as books_database_grpc

setup_logging()
logger = logging.getLogger(__name__)

SERVICE_KEY = "transaction_verification"
ORDER_STATES = {}
STATE_LOCK = threading.Lock()
STATE_TTL_SECONDS = env_float("ORDER_STATE_TTL_SECONDS", 900.0)
STATE_CLEANUP_INTERVAL_SECONDS = env_float("ORDER_STATE_CLEANUP_INTERVAL_SECONDS", 30.0)


def clock_to_proto(clock):
    return transaction_verification.VectorClock(**clock)


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
    def __init__(self):
        self.books_database_target = os.getenv("BOOKS_DATABASE_TARGET", "books_database_1:50056")

    def InitOrder(self, request, context):
        try:
            order = json.loads(request.order_json) if request.order_json else {}
        except json.JSONDecodeError:
            logger.warning("Failed to cache invalid order payload")
            return transaction_verification.OrderInitResponse(
                success=False,
                reason="Invalid order payload.",
                vector_clock=clock_to_proto(empty_clock()),
            )

        order_id = request.order_id.strip() or str(order.get("orderId", "unknown")).strip()
        initial_clock = merge_clocks(clock_from_proto(request.vector_clock))

        set_state(
            ORDER_STATES,
            STATE_LOCK,
            order_id,
            {
                "order": order,
                "vector_clock": initial_clock,
                "events": {},
            },
        )

        logger.info("Cached order_id=%s vc=%s", order_id, clock_to_log(initial_clock))
        return transaction_verification.OrderInitResponse(
            success=True,
            reason="Order cached.",
            vector_clock=clock_to_proto(initial_clock),
        )

    def _execute_event(self, order_id, incoming_clock, event_name, handler):
        event_state = update_for_event(ORDER_STATES, STATE_LOCK, order_id, incoming_clock, SERVICE_KEY)
        if event_state is None:
            return event_response(event_name, False, "Order state not found.", empty_clock())

        current_clock = event_state["vector_clock"]
        order = event_state["order"]

        logger.info("order_id=%s event=%s vc=%s", order_id, event_name, clock_to_log(current_clock))
        success, reason = handler(order)

        persisted_clock = record_event_result(
            ORDER_STATES,
            STATE_LOCK,
            order_id,
            event_name,
            success,
            reason,
        )
        if persisted_clock is not None:
            current_clock = persisted_clock

        logger.info(
            "order_id=%s event=%s success=%s reason=%s",
            order_id,
            event_name,
            success,
            reason,
        )
        return event_response(event_name, success, reason, current_clock)

    def ValidateItems(self, request, context):
        return self._execute_event(
            request.order_id,
            clock_from_proto(request.vector_clock),
            "validate_items",
            self._validate_items,
        )

    def CheckStock(self, request, context):
        return self._execute_event(
            request.order_id,
            clock_from_proto(request.vector_clock),
            "check_stock",
            self._check_stock,
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

        is_safe_to_clear, reason, local_clock = clear_order_if_safe(
            ORDER_STATES,
            STATE_LOCK,
            order_id,
            final_clock,
        )

        if local_clock is None:
            logger.info("Cleanup skipped for order_id=%s: already cleared", order_id)
            return transaction_verification.ClearOrderResponse(
                success=True,
                reason=reason,
                vector_clock=clock_to_proto(empty_clock()),
            )

        if is_safe_to_clear:
            logger.info("Cleared order_id=%s final_vc=%s", order_id, clock_to_log(final_clock))
            return transaction_verification.ClearOrderResponse(
                success=True,
                reason=reason,
                vector_clock=clock_to_proto(local_clock),
            )

        logger.warning(
            "Cleanup rejected for order_id=%s local_vc=%s final_vc=%s",
            order_id,
            clock_to_log(local_clock),
            clock_to_log(final_clock),
        )
        return transaction_verification.ClearOrderResponse(
            success=False,
            reason=reason,
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

    def _check_stock(self, order):
        items = order.get("items", []) or []
        if not items:
            return True, "No items to check."

        channel = grpc.insecure_channel(self.books_database_target)
        stub = books_database_grpc.BooksDatabaseStub(channel)
        
        try:
            for item in items:
                title = item.get("name")
                quantity = int(item.get("quantity", 0))
                
                try:
                    read_resp = stub.Read(
                        books_database.ReadRequest(title=title),
                        timeout=5.0
                    )
                    current_stock = read_resp.stock
                    if current_stock < quantity:
                        return False, f"Insufficient stock for {title}. Required: {quantity}, Available: {current_stock}"
                except grpc.RpcError as e:
                    logger.error(f"Failed to read stock for {title}: {e.code().name}")
                    return False, f"Failed to check stock for {title}."
        finally:
            channel.close()
            
        return True, "Stock validated."

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
    start_cleanup_thread(
        ORDER_STATES,
        STATE_LOCK,
        STATE_TTL_SECONDS,
        STATE_CLEANUP_INTERVAL_SECONDS,
        logger,
        SERVICE_KEY,
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    transaction_verification_grpc.add_TransactionVerificationServicer_to_server(
        TransactionVerificationService(),
        server,
    )
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info("Server started. Listening on port 50052.")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
