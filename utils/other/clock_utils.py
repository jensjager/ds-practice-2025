"""Shared vector clock utilities for distributed system orchestration."""
import json

CLOCK_KEYS = ("transaction_verification", "fraud_detection", "suggestions")


def empty_clock():
    """Create a new empty vector clock."""
    return {key: 0 for key in CLOCK_KEYS}


def merge_clocks(*clocks):
    """Merge multiple vector clocks by taking the maximum for each key."""
    merged = empty_clock()
    for clock in clocks:
        if not clock:
            continue
        for key in CLOCK_KEYS:
            merged[key] = max(merged[key], int(clock.get(key, 0)))
    return merged


def clock_from_proto(clock_message):
    """Convert protobuf vector clock message to dict."""
    if clock_message is None:
        return empty_clock()
    return {key: int(getattr(clock_message, key, 0)) for key in CLOCK_KEYS}


def clock_to_log(clock):
    """Convert vector clock to JSON string for logging."""
    return json.dumps(clock, sort_keys=True)


def increment_clock(clock, service_key):
    """Increment the service's clock value in place."""
    clock[service_key] += 1
