import sys
import os
import json
import re
from concurrent import futures

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

import grpc

class TransactionVerificationService(
    transaction_verification_grpc.TransactionVerificationServicer
):
    def VerifyTransaction(self, request, context):
        print("[transaction_verification] Request received")
        response = transaction_verification.TransactionVerificationResponse()

        try:
            order = json.loads(request.order_json) if request.order_json else {}
        except json.JSONDecodeError:
            response.is_valid = False
            response.reason = "Invalid order payload."
            return response

        errors = []
        items = order.get("items", []) or []
        user = order.get("user", {}) or {}
        card = order.get("creditCard", {}) or {}

        if not items:
            errors.append("No items provided")
        if not user.get("name"):
            errors.append("Missing user name")
        if not user.get("contact"):
            errors.append("Missing user contact")

        card_number = str(card.get("number", "")).replace(" ", "")
        exp_date = str(card.get("expirationDate", "")).strip()
        cvv = str(card.get("cvv", "")).strip()

        if not card_number.isdigit() or not 13 <= len(card_number) <= 19:
            errors.append("Invalid credit card number")
        if not re.match(r"^(0[1-9]|1[0-2])\/\d{2}$", exp_date):
            errors.append("Invalid expiration date")
        if not cvv.isdigit() or len(cvv) not in (3, 4):
            errors.append("Invalid CVV")

        response.is_valid = len(errors) == 0
        response.reason = "Transaction verified" if response.is_valid else "; ".join(errors)
        print(
            f"[transaction_verification] Result: is_valid={response.is_valid}, reason={response.reason}"
        )
        return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
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
