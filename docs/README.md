# Documentation

## REST Implementation
The REST API handles requests originating from the Frontend, ensuring communication between the user interface and backend services. The initial code is located under the `orchestrator` folder, and the API specification can be found in the `utils/api` folder. Refer to the `bookstore.yaml` file for the API specification.

## Orchestrator Service
Upon receiving a user request, the orchestrator service deploys worker threads for parallel processing. These threads dispatch order data to the designated backend microservices and wait for the respective results.

## Backend Microservices
Three backend microservices have been developed:

1. **Fraud Detection**
   - Listens on port `50051`.
   - Implements dummy logic to determine if an order is fraudulent.
   - Located in the `fraud_detection` folder with its own Dockerfile.

2. **Transaction Verification**
   - Listens on port `50052`.
   - Implements simple logic to verify transactions (e.g., checking if the list of items is not empty, user data is filled-in, and credit card format is correct).
   - Located in the `transaction_verification` folder with its own Dockerfile.

3. **Suggestions**
   - Listens on port `50053`.
   - Implements logic to calculate and send back a list of new book suggestions.
   - Located in the `suggestions` folder with its own Dockerfile.

The `docker-compose` file at the top level of the repository lists and orchestrates these services with the necessary configurations (name, port, volumes, etc.).

## gRPC Communication Setup
Communication channels to the backend microservices (fraud detection, transaction verification, and suggestions) are established using gRPC. The orchestrator worker threads send requests and expect responses via these channels. The gRPC proto file specification can be found in the `utils/pb` folder.

## Results Consolidation
The orchestrator service combines results received from the backend microservices. It waits for the threads to finish and then:
- If fraud is detected or transaction verification fails, it sends "Order Rejected" to the user.
- Otherwise, it sends "Order Approved" along with the list of book suggestions.

## System Logging
Relevant logs are added in all services to track key actions such as receiving requests, spawning new threads, and returning responses.

## Setup and Running the Services
1. Ensure Docker and Docker Compose are installed.
2. Build and start the services using the `docker-compose` file:
   ```sh
   docker compose up
    ```

## Architecure Diagram

<img src="architecture.png">

## System Diagram

<img src="system.png">
