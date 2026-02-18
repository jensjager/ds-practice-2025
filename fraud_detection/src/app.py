import sys
import os
import json

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

import grpc
from concurrent import futures

class FraudDetectionService(fraud_detection_grpc.FraudDetectionServicer):
    def CheckFraud(self, request, context):
        print("[fraud_detection] Request received")
        response = fraud_detection.FraudCheckResponse()

        try:
            order = json.loads(request.order_json) if request.order_json else {}
        except json.JSONDecodeError:
            response.is_fraud = True
            response.reason = "Invalid order payload."
            return response

        card_number = str(order.get("creditCard", {}).get("number", "")).replace(" ", "")
        contact = str(order.get("user", {}).get("contact", "")).strip()
        items = order.get("items", []) or []

        reasons = []
        if not contact:
            reasons.append("Missing contact information")
        if card_number.endswith("0000") or card_number.endswith("9999"):
            reasons.append("Suspicious card number pattern")
        if sum(item.get("quantity", 0) for item in items) > 100:
            reasons.append("Unusually large order")

        response.is_fraud = len(reasons) > 0
        response.reason = "; ".join(reasons) if reasons else "No fraud detected"
        print(
            f"[fraud_detection] Result: is_fraud={response.is_fraud}, reason={response.reason}"
        )
        return response

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    fraud_detection_grpc.add_FraudDetectionServicer_to_server(FraudDetectionService(), server)
    # Listen on port 50051
    port = "50051"
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    print("Server started. Listening on port 50051.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == '__main__':
    serve()