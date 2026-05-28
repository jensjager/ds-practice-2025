"""
Automated test scenarios for the distributed book order system.

This module provides comprehensive testing for:
1. Single non-fraudulent order
2. Multiple non-fraudulent non-conflicting orders
3. Multiple mixed orders (fraudulent and non-fraudulent)
4. Conflicting orders (same book simultaneously)

Usage:
    python -m pytest tests/test_order_flow.py -v
"""

import json
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# Configuration
ORCHESTRATOR_URL = "http://localhost:8081/checkout"
TEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 1  # second


def build_order_data(book_name="Book A", quantity=1, is_fraud=False):
    """Build a test order payload."""
    credit_card = "5555555555554444" if is_fraud else "4111111111111111"
    cvv = "999" if is_fraud else "123"

    return {
        "user": {
            "name": "Test User",
            "contact": "test@example.com",
        },
        "creditCard": {
            "number": credit_card,
            "expirationDate": "12/25",
            "cvv": cvv,
        },
        "userComment": "Test order",
        "items": [{"name": book_name, "quantity": quantity}],
        "billingAddress": {
            "street": "123 Test St",
            "city": "Testville",
            "state": "TS",
            "zip": "12345",
            "country": "Testland",
        },
        "shippingMethod": "Standard",
        "giftWrapping": False,
        "termsAccepted": True,
    }


def send_order(order_data, retries=MAX_RETRIES):
    """Send an order to the orchestrator with retry logic."""
    for attempt in range(retries):
        try:
            response = requests.post(
                ORCHESTRATOR_URL,
                json=order_data,
                timeout=TEST_TIMEOUT,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            return response.json(), response.status_code, None
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                return None, None, str(e)
    return None, None, "Max retries exceeded"


def wait_for_health_check(url="http://localhost:8081/health", timeout=60):
    """Wait for a service to be healthy."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "ok":
                    return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)
    return False


# ============================================================================
# Test Case 1: Single Non-Fraudulent Order
# ============================================================================


def test_single_non_fraudulent_order():
    """
    Test scenario: Single non-fraudulent order

    Demonstrates the complete flow of a single valid order:
    1. Submit order with valid credit card
    2. Verify order is processed successfully
    3. Check that order is approved
    """
    print("\n" + "=" * 80)
    print("TEST 1: Single Non-Fraudulent Order")
    print("=" * 80)

    # Wait for orchestrator to be ready
    if not wait_for_health_check():
        raise RuntimeError("Orchestrator not ready after timeout")

    # Build and send order
    order_data = build_order_data(book_name="Book A", quantity=1, is_fraud=False)

    print(f"Submitting order for Book A (quantity: 1)...")
    result, status_code, error = send_order(order_data)

    assert error is None, f"Failed to send order: {error}"
    assert status_code == 200, f"Expected status 200, got {status_code}"
    assert result is not None, "No response received"

    print(f"Response: {json.dumps(result, indent=2)}")

    # Verify order was approved
    assert result.get("status") == "Order Approved", (
        f"Expected 'Order Approved', got '{result.get('status')}'"
    )
    assert "orderId" in result, "Missing orderId in response"
    assert result.get("orderId") is not None, "orderId is None"

    print(f"✓ Order {result['orderId']} was approved successfully")
    print("✓ Test 1 PASSED\n")
    return result


# ============================================================================
# Test Case 2: Multiple Non-Fraudulent Non-Conflicting Orders
# ============================================================================


def test_multiple_non_conflicting_orders():
    """
    Test scenario: Multiple simultaneous non-fraudulent orders for different books

    Non-conflicting means orders attempt to purchase different books.
    Tests concurrent processing of multiple valid orders.
    """
    print("\n" + "=" * 80)
    print("TEST 2: Multiple Non-Fraudulent Non-Conflicting Orders")
    print("=" * 80)

    # Different books for non-conflicting orders
    books = ["Book A", "Book B", "Book C", "Book D", "Book E"]
    num_orders = len(books)

    print(f"Submitting {num_orders} concurrent orders for different books...")

    results = []
    with ThreadPoolExecutor(max_workers=num_orders) as executor:
        futures = []
        for i, book in enumerate(books):
            order_data = build_order_data(book_name=book, quantity=1, is_fraud=False)
            # Add small delay to stagger requests
            time.sleep(0.01 * i)
            future = executor.submit(send_order, order_data)
            futures.append(future)

        for future in as_completed(futures):
            result, status_code, error = future.result()
            if error:
                print(f"✗ Order failed: {error}")
                results.append((None, None, error))
            else:
                results.append((result, status_code, None))

    # Verify all orders were processed
    approved_count = 0
    for result, status_code, error in results:
        assert error is None, f"Order failed: {error}"
        assert status_code == 200, f"Expected status 200, got {status_code}"
        assert result is not None, "No response received"

        if result.get("status") == "Order Approved":
            approved_count += 1
            print(
                f"✓ Order {result['orderId']} approved for {result.get('suggestedBooks', [{}])[0].get('title', 'unknown') if result.get('suggestedBooks') else 'unknown'}"
            )

    print(f"\n✓ All {num_orders} orders processed successfully")
    print(f"✓ {approved_count}/{num_orders} orders were approved")
    assert approved_count == num_orders, (
        f"Expected all {num_orders} orders to be approved"
    )
    print("✓ Test 2 PASSED\n")
    return results


# ============================================================================
# Test Case 3: Multiple Mixed Orders (Fraudulent and Non-Fraudulent)
# ============================================================================


def test_mixed_orders():
    """
    Test scenario: Mixture of fraudulent and non-fraudulent orders

    Tests that:
    1. Valid orders are approved
    2. Fraudulent orders are rejected
    3. System handles both types simultaneously
    """
    print("\n" + "=" * 80)
    print("TEST 3: Multiple Mixed Orders (Fraudulent and Non-Fraudulent)")
    print("=" * 80)

    # Define test cases: (book_name, is_fraud, expected_status)
    test_cases = [
        ("Book A", False, "Order Approved"),
        ("Book B", True, "Order Rejected"),  # Fraud card
        ("Book C", False, "Order Approved"),
        ("Book D", True, "Order Rejected"),  # Fraud card
        ("Book E", False, "Order Approved"),
    ]

    print(f"Submitting {len(test_cases)} orders (mix of valid and fraudulent)...")

    results = []
    with ThreadPoolExecutor(max_workers=len(test_cases)) as executor:
        futures = []
        for i, (book_name, is_fraud, expected_status) in enumerate(test_cases):
            order_data = build_order_data(
                book_name=book_name, quantity=1, is_fraud=is_fraud
            )
            time.sleep(0.01 * i)  # Small delay to stagger
            future = executor.submit(send_order, order_data)
            futures.append((future, expected_status, book_name))

        for future, expected_status, book_name in futures:
            result, status_code, error = future.result()
            results.append((result, status_code, error, expected_status, book_name))

    approved_count = 0
    rejected_count = 0

    for result, status_code, error, expected_status, book_name in results:
        assert error is None, f"Order for {book_name} failed: {error}"
        assert status_code == 200, (
            f"Expected status 200 for {book_name}, got {status_code}"
        )

        actual_status = result.get("status")
        if actual_status == "Order Approved":
            approved_count += 1
            print(f"✓ {book_name}: Approved (expected: {expected_status})")
        elif actual_status == "Order Rejected":
            rejected_count += 1
            print(f"✓ {book_name}: Rejected (expected: {expected_status})")

        assert actual_status == expected_status, (
            f"Expected {expected_status} for {book_name}, got {actual_status}"
        )

    print(f"\n✓ All {len(test_cases)} orders processed correctly")
    print(f"✓ {approved_count} approved, {rejected_count} rejected")
    assert approved_count == 3, f"Expected 3 approved orders, got {approved_count}"
    assert rejected_count == 2, f"Expected 2 rejected orders, got {rejected_count}"
    print("✓ Test 3 PASSED\n")
    return results


# ============================================================================
# Test Case 4: Conflicting Orders (Same Book Simultaneously)
# ============================================================================


def test_conflicting_orders():
    """
    Test scenario: Multiple orders attempting to purchase the same book simultaneously

    Conflicting orders test the system's ability to handle concurrent requests
    for the same resource (book stock). The system should:
    1. Process orders in some order
    2. Approve orders that can be fulfilled based on available stock
    3. Reject orders when stock is exhausted

    Note: This depends on the initial stock of "Book A" in the books_database.
    We assume Book A starts with stock of 2.
    """
    print("\n" + "=" * 80)
    print("TEST 4: Conflicting Orders (Same Book Simultaneously)")
    print("=" * 80)

    # We'll try to order more than the available stock
    # Assuming Book A has initial stock of 2
    num_orders = 5  # More than available stock
    book_name = "Book A"

    print(f"Submitting {num_orders} concurrent orders for {book_name}...")
    print("(Assuming initial stock of 2, only 2 should succeed)")

    results = []
    with ThreadPoolExecutor(max_workers=num_orders) as executor:
        futures = []
        for i in range(num_orders):
            order_data = build_order_data(
                book_name=book_name, quantity=1, is_fraud=False
            )
            # All submitted at nearly the same time
            future = executor.submit(send_order, order_data)
            futures.append(future)

        for future in as_completed(futures):
            result, status_code, error = future.result()
            results.append((result, status_code, error))

    approved_count = 0
    rejected_count = 0

    for result, status_code, error in results:
        if error:
            print(f"✗ Order failed with error: {error}")
            rejected_count += 1
            continue

        if status_code != 200:
            print(f"✗ Order failed with status: {status_code}")
            rejected_count += 1
            continue

        if result is None:
            print(f"✗ Order returned no result")
            rejected_count += 1
            continue

        if result.get("status") == "Order Approved":
            approved_count += 1
            print(f"✓ Order {result['orderId']} approved")
        else:
            rejected_count += 1
            print(
                f"✗ Order {result.get('orderId', 'unknown')} rejected: {result.get('reason', 'no reason')}"
            )

    print(f"\n✓ Processed {num_orders} conflicting orders")
    print(f"✓ {approved_count} approved, {rejected_count} rejected")

    # At most 2 should be approved (based on initial stock)
    # The rest should be rejected or failed
    assert approved_count <= 2, f"Expected at most 2 approved, got {approved_count}"
    print("✓ Test 4 PASSED\n")
    return results


# ============================================================================
# Load Testing Functions
# ============================================================================


def run_load_test(num_orders=20, duration=10, book_pool=["Book A", "Book B", "Book C"]):
    """
    Run a load test to measure system performance.

    Args:
        num_orders: Total number of orders to submit
        duration: Time window (seconds) to submit orders
        book_pool: List of books to randomly select from

    Returns:
        Dict with performance metrics
    """
    print("\n" + "=" * 80)
    print(f"LOAD TEST: {num_orders} orders over {duration} seconds")
    print("=" * 80)

    start_time = time.time()
    end_time = start_time + duration

    results = []
    errors = []
    order_times = []

    def submit_order_with_timing():
        order_data = build_order_data(
            book_name=random.choice(book_pool), quantity=1, is_fraud=False
        )
        submit_start = time.time()
        result, status_code, error = send_order(order_data)
        submit_end = time.time()

        order_times.append(submit_end - submit_start)

        if error:
            errors.append(error)
            return None
        return result

    with ThreadPoolExecutor(max_workers=min(num_orders, 10)) as executor:
        futures = []
        submitted = 0

        while time.time() < end_time and submitted < num_orders:
            future = executor.submit(submit_order_with_timing)
            futures.append(future)
            submitted += 1
            time.sleep(0.01)  # Small delay between submissions

        # Wait for remaining futures
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    # Calculate metrics
    total_time = max(order_times) if order_times else 0
    avg_time = sum(order_times) / len(order_times) if order_times else 0
    min_time = min(order_times) if order_times else 0
    max_time = max(order_times) if order_times else 0

    approved = sum(1 for r in results if r.get("status") == "Order Approved")
    rejected = sum(1 for r in results if r.get("status") == "Order Rejected")

    print(f"\nResults:")
    print(f"  Total orders submitted: {submitted}")
    print(f"  Successful responses: {len(results)}")
    print(f"  Errors: {len(errors)}")
    print(f"  Approved: {approved}")
    print(f"  Rejected: {rejected}")
    print(f"\nTiming (seconds):")
    print(f"  Average: {avg_time:.3f}")
    print(f"  Minimum: {min_time:.3f}")
    print(f"  Maximum: {max_time:.3f}")
    print(f"  Total test time: {total_time:.3f}")

    if len(results) > 0:
        throughput = len(results) / total_time if total_time > 0 else 0
        print(f"  Throughput: {throughput:.2f} orders/second")

    return {
        "total_submitted": submitted,
        "successful": len(results),
        "errors": len(errors),
        "approved": approved,
        "rejected": rejected,
        "avg_time": avg_time,
        "min_time": min_time,
        "max_time": max_time,
        "throughput": len(results) / total_time if total_time > 0 else 0,
    }


def run_stress_test():
    """
    Run a stress test with increasing load to find breaking point.
    """
    print("\n" + "=" * 80)
    print("STRESS TEST: Finding system limits")
    print("=" * 80)

    loads = [5, 10, 20, 30, 50]
    results = []

    for load in loads:
        print(f"\nTesting with {load} concurrent orders...")
        result = run_load_test(num_orders=load, duration=5)
        results.append((load, result))
        time.sleep(2)  # Cool down period

    print("\n" + "=" * 80)
    print("STRESS TEST SUMMARY")
    print("=" * 80)
    for load, result in results:
        print(f"\nLoad: {load} orders")
        print(
            f"  Success rate: {result['successful']}/{result['total_submitted']} ({100 * result['successful'] / result['total_submitted'] if result['total_submitted'] > 0 else 0:.1f}%)"
        )
        print(f"  Throughput: {result['throughput']:.2f} orders/sec")
        print(f"  Avg response time: {result['avg_time']:.3f}s")

    return results


# ============================================================================
# Main Test Runner
# ============================================================================


def run_all_tests():
    """Run all test scenarios."""
    print("\n" + "=" * 80)
    print("DISTRIBUTED BOOK ORDER SYSTEM - AUTOMATED TEST SUITE")
    print("=" * 80)

    # Test 1: Single order
    try:
        test_single_non_fraudulent_order()
    except Exception as e:
        print(f"\n✗ Test 1 FAILED: {e}\n")
        return False

    # Test 2: Multiple non-conflicting
    try:
        test_multiple_non_conflicting_orders()
    except Exception as e:
        print(f"\n✗ Test 2 FAILED: {e}\n")
        return False

    # Test 3: Mixed orders
    try:
        test_mixed_orders()
    except Exception as e:
        print(f"\n✗ Test 3 FAILED: {e}\n")
        return False

    # Test 4: Conflicting orders
    try:
        test_conflicting_orders()
    except Exception as e:
        print(f"\n✗ Test 4 FAILED: {e}\n")
        return False

    # Optional: Load test
    # Uncomment to run
    # try:
    #     run_load_test(num_orders=10, duration=5)
    # except Exception as e:
    #     print(f"\n✗ Load test FAILED: {e}\n")

    print("\n" + "=" * 80)
    print("ALL TESTS PASSED!")
    print("=" * 80)
    return True


if __name__ == "__main__":
    # Run all tests
    success = run_all_tests()
    exit(0 if success else 1)
