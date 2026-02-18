import sys
import os
import json
from concurrent import futures

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
suggestions_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/suggestions'))
sys.path.insert(0, suggestions_grpc_path)
import suggestions_pb2 as suggestions
import suggestions_pb2_grpc as suggestions_grpc

import grpc

CATALOG = [
    {"book_id": "101", "title": "Distributed Systems 101", "author": "A. Tanenbaum"},
    {"book_id": "102", "title": "Clean Architecture", "author": "R. Martin"},
    {"book_id": "103", "title": "Designing Data-Intensive Apps", "author": "M. Kleppmann"},
    {"book_id": "104", "title": "Site Reliability Engineering", "author": "B. Beyer"},
    {"book_id": "105", "title": "The Pragmatic Programmer", "author": "A. Hunt"},
    {"book_id": "106", "title": "Refactoring", "author": "M. Fowler"},
]

class SuggestionsService(suggestions_grpc.SuggestionsServicer):
    def GetSuggestions(self, request, context):
        print("[suggestions] Request received")
        response = suggestions.SuggestionsResponse()

        try:
            order = json.loads(request.order_json) if request.order_json else {}
        except json.JSONDecodeError:
            order = {}

        items = order.get("items", []) or []
        ordered_titles = {str(item.get("name", "")).lower() for item in items}

        seed = sum(ord(c) for c in str(order.get("user", {}).get("name", "")))
        start_index = seed % len(CATALOG) if CATALOG else 0

        candidates = []
        for offset in range(len(CATALOG)):
            book = CATALOG[(start_index + offset) % len(CATALOG)]
            if book["title"].lower() not in ordered_titles:
                candidates.append(book)
            if len(candidates) == 3:
                break

        for book in candidates:
            response.books.add(
                book_id=book["book_id"],
                title=book["title"],
                author=book["author"],
            )

        print(f"[suggestions] Returning {len(response.books)} suggestions")

        return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    suggestions_grpc.add_SuggestionsServicer_to_server(SuggestionsService(), server)
    port = "50053"
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started. Listening on port 50053.")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
