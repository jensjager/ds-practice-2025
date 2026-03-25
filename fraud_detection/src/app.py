import json
import logging
import os
import sys
import threading
from concurrent import futures

import grpc

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

CLOCK_KEYS = ("transaction_verification", "fraud_detection", "suggestions")
SERVICE_KEY = "fraud_detection"
ORDER_STATES = {}
STATE_LOCK = threading.Lock()


def empty_clock():
    return {key: 0 for key in CLOCK_KEYS}


def clock_from_proto(clock):
    if clock is None:
        return empty_clock()
    return {key: int(getattr(clock, key, 0)) for key in CLOCK_KEYS}


def clock_to_proto(clock):
    return fraud_detection.VectorClock(**clock)


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
    return fraud_detection.EventResponse(
        success=success,
        reason=reason,
        event_name=event_name,
        vector_clock=clock_to_proto(clock),
    )


class FraudDetectionService(fraud_detection_grpc.FraudDetectionServicer):
    def InitOrder(self, request, context):
        try:
            order = json.loads(request.order_json) if request.order_json else {}
        except json.JSONDecodeError:
            logger.warning("Failed to cache invalid order payload")
            return fraud_detection.OrderInitResponse(
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

        logger.info("Cached order_id=%s vc=%s", order_id, clock_to_log(initial_clock))
        return fraud_detection.OrderInitResponse(
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

        logger.info("order_id=%s event=%s vc=%s", order_id, event_name, clock_to_log(current_clock))
        success, reason = handler(order)

        with STATE_LOCK:
            state = ORDER_STATES.get(order_id)
            if state is not None:
                state["events"][event_name] = {"success": success, "reason": reason}
                if not success:
                    state["failed"] = True
                current_clock = dict(state["vector_clock"])

        logger.info(
            "order_id=%s event=%s success=%s reason=%s",
            order_id,
            event_name,
            success,
            reason,
        )
        return event_response(event_name, success, reason, current_clock)

    def CheckUserFraud(self, request, context):
        return self._execute_event(
            request.order_id,
            clock_from_proto(request.vector_clock),
            "check_user_fraud",
            self._check_user_fraud,
        )

    def CheckCardFraud(self, request, context):
        return self._execute_event(
            request.order_id,
            clock_from_proto(request.vector_clock),
            "check_card_fraud",
            self._check_card_fraud,
        )

    def ClearOrder(self, request, context):
        order_id = request.order_id
        final_clock = clock_from_proto(request.final_vector_clock)

        with STATE_LOCK:
            state = ORDER_STATES.get(order_id)
            if state is None:
                logger.info("Cleanup skipped for order_id=%s: already cleared", order_id)
                return fraud_detection.ClearOrderResponse(
                    success=True,
                    reason="Order state already cleared.",
                    vector_clock=clock_to_proto(empty_clock()),
                )

            local_clock = dict(state["vector_clock"])
            is_safe_to_clear = all(local_clock[key] <= final_clock[key] for key in CLOCK_KEYS)
            if is_safe_to_clear:
                del ORDER_STATES[order_id]

        if is_safe_to_clear:
            logger.info("Cleared order_id=%s final_vc=%s", order_id, clock_to_log(final_clock))
            return fraud_detection.ClearOrderResponse(
                success=True,
                reason="Order state cleared.",
                vector_clock=clock_to_proto(local_clock),
            )

        logger.warning(
            "Cleanup rejected for order_id=%s local_vc=%s final_vc=%s",
            order_id,
            clock_to_log(local_clock),
            clock_to_log(final_clock),
        )
        return fraud_detection.ClearOrderResponse(
            success=False,
            reason="Local vector clock is ahead of the final vector clock.",
            vector_clock=clock_to_proto(local_clock),
        )

    @staticmethod
    def _check_user_fraud(order):
        user = order.get("user", {}) or {}
        name = str(user.get("name", "")).strip().lower()
        contact = str(user.get("contact", "")).strip().lower()
        comment = str(order.get("userComment", "")).strip().lower()

        if "fraud" in name or "fraud" in contact or "chargeback" in comment:
            return False, "User data flagged as suspicious."
        if contact.endswith("@blocked.example"):
            return False, "Blocked contact domain detected."
        return True, "User data passed fraud screening."

    @staticmethod
    def _check_card_fraud(order):
        card_number = str(order.get("creditCard", {}).get("number", "")).replace(" ", "")
        items = order.get("items", []) or []
        total_quantity = sum(int(item.get("quantity", 0)) for item in items)

        if card_number.endswith("0000") or card_number.endswith("9999"):
            return False, "Suspicious card number pattern detected."
        if total_quantity > 100:
            return False, "Unusually large order volume detected."
        return True, "Card data passed fraud screening."


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    fraud_detection_grpc.add_FraudDetectionServicer_to_server(FraudDetectionService(), server)
    port = "50051"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info("Server started. Listening on port 50051.")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()