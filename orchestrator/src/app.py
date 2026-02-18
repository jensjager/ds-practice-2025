import sys
import os
import json
import uuid
from concurrent import futures

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
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc

import grpc

def check_fraud(order_json):
    print("[orchestrator] Sending fraud check")
    with grpc.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudDetectionStub(channel)
        response = stub.CheckFraud(
            fraud_detection.FraudCheckRequest(order_json=order_json),
            timeout=3.0,
        )
    print(f"[orchestrator] Fraud check result: is_fraud={response.is_fraud}")
    return response

def verify_transaction(order_json):
    print("[orchestrator] Sending transaction verification")
    with grpc.insecure_channel('transaction_verification:50052') as channel:
        stub = transaction_verification_grpc.TransactionVerificationStub(channel)
        response = stub.VerifyTransaction(
            transaction_verification.TransactionVerificationRequest(order_json=order_json),
            timeout=3.0,
        )
    print(f"[orchestrator] Transaction verification result: is_valid={response.is_valid}")
    return response

def get_suggestions(order_json):
    print("[orchestrator] Requesting suggestions")
    with grpc.insecure_channel('suggestions:50053') as channel:
        stub = suggestions_grpc.SuggestionsStub(channel)
        response = stub.GetSuggestions(
            suggestions.SuggestionsRequest(order_json=order_json),
            timeout=3.0,
        )
    print(f"[orchestrator] Suggestions received: count={len(response.books)}")
    return response

# Import Flask.
# Flask is a web framework for Python.
# It allows you to build a web application quickly.
# For more information, see https://flask.palletsprojects.com/en/latest/
from flask import Flask, request, jsonify
from flask_cors import CORS
import json

# Create a simple Flask app.
app = Flask(__name__)
# Enable CORS for the app.
CORS(app, resources={r'/*': {'origins': '*'}})

# Define a GET endpoint.
@app.route('/', methods=['GET'])
def index():
    return "Orchestrator is running."

def error_response(code, message):
    return jsonify({"error": {"code": str(code), "message": message}}), code

def validate_request(data):
    if not isinstance(data, dict):
        return "Invalid JSON payload."
    if not isinstance(data.get("items"), list) or len(data.get("items")) == 0:
        return "Items list is required."
    user = data.get("user", {})
    card = data.get("creditCard", {})
    if not user.get("name") or not user.get("contact"):
        return "User name and contact are required."
    if not card.get("number") or not card.get("expirationDate") or not card.get("cvv"):
        return "Credit card details are required."
    return None

@app.route('/checkout', methods=['POST'])
def checkout():
    """
    Responds with a JSON object containing the order ID, status, and suggested books.
    """
    print("[orchestrator] /checkout request received")
    try:
        request_data = request.get_json(force=True, silent=False)
    except Exception:
        return error_response(400, "Request body must be valid JSON.")

    validation_error = validate_request(request_data)
    if validation_error:
        print(f"[orchestrator] Request validation failed: {validation_error}")
        return error_response(400, validation_error)

    order_id = str(uuid.uuid4())
    request_data["orderId"] = order_id
    order_json = json.dumps(request_data)

    try:
        with futures.ThreadPoolExecutor(max_workers=3) as executor:
            print("[orchestrator] Spawning worker threads")
            fraud_future = executor.submit(check_fraud, order_json)
            transaction_future = executor.submit(verify_transaction, order_json)
            suggestions_future = executor.submit(get_suggestions, order_json)

            fraud_result = fraud_future.result()
            transaction_result = transaction_future.result()
            suggestions_result = suggestions_future.result()
    except grpc.RpcError as exc:
        return error_response(500, f"gRPC error: {exc.code().name}")
    except Exception as exc:
        return error_response(500, f"Unexpected error: {str(exc)}")

    approved = (not fraud_result.is_fraud) and transaction_result.is_valid
    status = "Order Approved" if approved else "Order Rejected"
    print(f"[orchestrator] Decision computed: {status}")

    suggested_books = []
    if approved:
        for book in suggestions_result.books:
            suggested_books.append(
                {"bookId": book.book_id, "title": book.title, "author": book.author}
            )

    response = {
        "orderId": order_id,
        "status": status,
        "suggestedBooks": suggested_books,
    }

    print("[orchestrator] Response ready for client")

    return jsonify(response)


if __name__ == '__main__':
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host='0.0.0.0')
