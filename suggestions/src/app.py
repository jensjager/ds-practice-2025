import json
import logging
import os
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
add_grpc_path(FILE, '../../../utils/pb/suggestions')
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc

setup_logging()
logger = logging.getLogger(__name__)

SERVICE_KEY = "suggestions"
ORDER_STATES = {}
STATE_LOCK = threading.Lock()
STATE_TTL_SECONDS = env_float("ORDER_STATE_TTL_SECONDS", 900.0)
STATE_CLEANUP_INTERVAL_SECONDS = env_float("ORDER_STATE_CLEANUP_INTERVAL_SECONDS", 30.0)
CATALOG = [
    {"book_id": "101", "title": "Distributed Systems 101", "author": "A. Tanenbaum"},
    {"book_id": "102", "title": "Clean Architecture", "author": "R. Martin"},
    {"book_id": "103", "title": "Designing Data-Intensive Apps", "author": "M. Kleppmann"},
    {"book_id": "104", "title": "Site Reliability Engineering", "author": "B. Beyer"},
    {"book_id": "105", "title": "The Pragmatic Programmer", "author": "A. Hunt"},
    {"book_id": "106", "title": "Refactoring", "author": "M. Fowler"},
]


def clock_to_proto(clock):
    return suggestions.VectorClock(**clock)


def event_response(event_name, success, reason, clock):
    return suggestions.EventResponse(
        success=success,
        reason=reason,
        event_name=event_name,
        vector_clock=clock_to_proto(clock),
    )


class SuggestionsService(suggestions_grpc.SuggestionsServicer):
    def InitOrder(self, request, context):
        try:
            order = json.loads(request.order_json) if request.order_json else {}
        except json.JSONDecodeError:
            logger.warning("Failed to cache invalid order payload")
            return suggestions.OrderInitResponse(
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
                "prepared_books": [],
                "generated_books": [],
            },
        )

        logger.info("Cached order_id=%s vc=%s", order_id, clock_to_log(initial_clock))
        return suggestions.OrderInitResponse(
            success=True,
            reason="Order cached.",
            vector_clock=clock_to_proto(initial_clock),
        )

    def _run_event(self, order_id, incoming_clock, event_name, handler):
        event_state = update_for_event(ORDER_STATES, STATE_LOCK, order_id, incoming_clock, SERVICE_KEY)
        if event_state is None:
            return False, "Order state not found.", empty_clock(), {}

        current_clock = event_state["vector_clock"]
        order = event_state["order"]
        prepared_books = event_state["prepared_books"]

        logger.info("order_id=%s event=%s vc=%s", order_id, event_name, clock_to_log(current_clock))
        success, reason, updates = handler(order, prepared_books)

        persisted_clock = record_event_result(
            ORDER_STATES,
            STATE_LOCK,
            order_id,
            event_name,
            success,
            reason,
            updates=updates,
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
        return success, reason, current_clock, updates

    def PrepareSuggestionsContext(self, request, context):
        success, reason, current_clock, _ = self._run_event(
            request.order_id,
            clock_from_proto(request.vector_clock),
            "prepare_suggestions_context",
            self._prepare_suggestions_context,
        )
        return event_response("prepare_suggestions_context", success, reason, current_clock)

    def GenerateSuggestions(self, request, context):
        success, reason, current_clock, updates = self._run_event(
            request.order_id,
            clock_from_proto(request.vector_clock),
            "generate_suggestions",
            self._generate_suggestions,
        )
        response = suggestions.SuggestionsEventResponse(
            success=success,
            reason=reason,
            event_name="generate_suggestions",
            vector_clock=clock_to_proto(current_clock),
        )
        for book in updates.get("generated_books", []):
            response.books.add(
                book_id=book["book_id"],
                title=book["title"],
                author=book["author"],
            )
        return response

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
            return suggestions.ClearOrderResponse(
                success=True,
                reason=reason,
                vector_clock=clock_to_proto(empty_clock()),
            )

        if is_safe_to_clear:
            logger.info("Cleared order_id=%s final_vc=%s", order_id, clock_to_log(final_clock))
            return suggestions.ClearOrderResponse(
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
        return suggestions.ClearOrderResponse(
            success=False,
            reason=reason,
            vector_clock=clock_to_proto(local_clock),
        )

    @staticmethod
    def _prepare_suggestions_context(order, prepared_books):
        items = order.get("items", []) or []
        ordered_titles = {str(item.get("name", "")).lower() for item in items}
        seed = sum(ord(char) for char in str(order.get("user", {}).get("name", "")))
        start_index = seed % len(CATALOG) if CATALOG else 0

        candidates = []
        for offset in range(len(CATALOG)):
            book = CATALOG[(start_index + offset) % len(CATALOG)]
            if book["title"].lower() not in ordered_titles:
                candidates.append(book)

        return True, "Suggestion context prepared.", {"prepared_books": candidates}

    @staticmethod
    def _generate_suggestions(order, prepared_books):
        books = list(prepared_books)
        if not books:
            _, _, updates = SuggestionsService._prepare_suggestions_context(order, prepared_books)
            books = updates.get("prepared_books", [])

        generated_books = books[:3]
        return True, f"Generated {len(generated_books)} suggestions.", {"generated_books": generated_books}


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
    suggestions_grpc.add_SuggestionsServicer_to_server(SuggestionsService(), server)
    port = "50053"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info("Server started. Listening on port 50053.")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
