import logging
import os
import time
from concurrent import futures
import grpc

from utils.other.runtime_utils import add_grpc_path, env_float, env_int, setup_logging

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
add_grpc_path(FILE, '../../../utils/pb/payment')
import payment_pb2 as payment
import payment_pb2_grpc as payment_grpc

setup_logging()
logger = logging.getLogger(__name__)

class PaymentService(payment_grpc.PaymentServiceServicer):
    def __init__(self):
        self.prepared = {}

    def Prepare(self, request, context):
        # Dummy validation logic
        logger.info(f"Preparing payment for order {request.order_id}")
        self.prepared[request.order_id] = True
        return payment.PrepareResponse(ready=True)

    def Commit(self, request, context):
        is_prepared = self.prepared.pop(request.order_id, False)
        if is_prepared:
            logger.info(f"Payment committed for order {request.order_id}")
            return payment.CommitResponse(success=True)
        else:
            logger.warning(f"Payment commit failed: not prepared for order {request.order_id}")
            return payment.CommitResponse(success=False)

    def Abort(self, request, context):
        self.prepared.pop(request.order_id, None)
        logger.info(f"Payment aborted for order {request.order_id}")
        return payment.AbortResponse(aborted=True)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    payment_grpc.add_PaymentServiceServicer_to_server(PaymentService(), server)
    port = env_int("PAYMENT_PORT", 50057)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"Payment Service started on port {port}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Payment service stopped.")

if __name__ == '__main__':
    serve()
