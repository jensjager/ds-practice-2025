import threading
import time

from utils.other.clock_utils import CLOCK_KEYS, merge_clocks, increment_clock


def _now():
    return time.time()


def set_state(store, lock, order_id, state):
    with lock:
        state["updated_at"] = _now()
        store[order_id] = state


def update_for_event(store, lock, order_id, incoming_clock, service_key):
    with lock:
        state = store.get(order_id)
        if state is None:
            return None

        state["vector_clock"] = merge_clocks(state["vector_clock"], incoming_clock)
        increment_clock(state["vector_clock"], service_key)
        state["updated_at"] = _now()
        return {
            "order": state["order"],
            "vector_clock": dict(state["vector_clock"]),
            "prepared_books": list(state.get("prepared_books", [])),
        }


def record_event_result(store, lock, order_id, event_name, success, reason, updates=None):
    with lock:
        state = store.get(order_id)
        if state is None:
            return None

        state.setdefault("events", {})[event_name] = {"success": success, "reason": reason}
        if updates:
            state.update(updates)
        state["updated_at"] = _now()
        return dict(state["vector_clock"])


def clear_order_if_safe(store, lock, order_id, final_clock):
    with lock:
        state = store.get(order_id)
        if state is None:
            return True, "Order state already cleared.", None

        local_clock = dict(state["vector_clock"])
        is_safe_to_clear = all(local_clock[key] <= final_clock[key] for key in CLOCK_KEYS)
        if is_safe_to_clear:
            del store[order_id]
            return True, "Order state cleared.", local_clock

        return False, "Local vector clock is ahead of the final vector clock.", local_clock


def prune_expired_orders(store, lock, ttl_seconds):
    now = _now()
    removed = 0
    with lock:
        stale_order_ids = [
            order_id
            for order_id, state in store.items()
            if (now - float(state.get("updated_at", now))) > ttl_seconds
        ]
        for order_id in stale_order_ids:
            del store[order_id]
            removed += 1
    return removed


def start_cleanup_thread(store, lock, ttl_seconds, interval_seconds, logger, service_key):
    if ttl_seconds <= 0 or interval_seconds <= 0:
        return

    def _cleanup_loop():
        while True:
            time.sleep(interval_seconds)
            removed = prune_expired_orders(store, lock, ttl_seconds)
            if removed:
                logger.info(
                    "Removed stale order states service=%s count=%s ttl_seconds=%s",
                    service_key,
                    removed,
                    ttl_seconds,
                )

    thread = threading.Thread(target=_cleanup_loop, daemon=True)
    thread.start()
