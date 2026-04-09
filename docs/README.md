# Documentation

## System Model

The system follows a microservices architecture with a centralized orchestrator coordinating workflows across backend services. Key components:

- **Frontend**: User interface that sends requests to the orchestrator
- **Orchestrator**: Central coordinator that manages event workflows, vector clocks, and service communication
- **Backend Microservices**: Five specialized services (Fraud Detection, Transaction Verification, Suggestions, Order Queue, Order Executor)
- **gRPC Communication**: All backend services communicate via gRPC protocols

The orchestrator maintains causal ordering through vector clocks and handles failure propagation. Services cache order state with TTL-based cleanup. The Order Executor service uses Bully algorithm for leader election among replicas.

## REST Implementation
The REST API handles requests originating from the Frontend, ensuring communication between the user interface and backend services. The initial code is located under the `orchestrator` folder, and the API specification can be found in the `utils/api` folder. Refer to the `bookstore.yaml` file for the API specification.

## Orchestrator Service
Upon receiving a user request, the orchestrator service now coordinates an ordered multi-event workflow. It generates a unique `orderId`, initializes backend order state in all services, schedules causally dependent events, propagates vector clocks, and returns early when any intermediate event fails.

### OrderID Generation and Dispatch
The orchestrator generates a unique `OrderID` for each order request using UUID. This `OrderID` is dispatched to all backend services during the initialization phase. The `OrderID` is used to track the order across all services and ensure consistency in the event flow.

### Event Flow and Vector Clock Initialization
The orchestrator initializes the order state in all backend services by sending the order data along with an initial vector clock. Each service caches the order data and initializes a vector clock for the order. The vector clock is used to track the causal relationships between events and ensure correct event ordering.

### Event Ordering and Concurrency
The orchestrator schedules events based on their dependencies, allowing some events to run in parallel. For example, `validate_items` and `validate_user_data` can run concurrently, while `validate_card_format` depends on the completion of `validate_items`. This concurrency is captured by the vector clocks, which are updated and propagated with each event execution.

### Response Handling and Failure Propagation
If any intermediate event fails, the orchestrator immediately propagates the failure back to the user and cancels all pending events. If all events succeed, the orchestrator returns the list of suggested books to the user. In both cases, the orchestrator broadcasts a cleanup message to all services to clear the cached order data.

### Broadcast Mechanism for Cleanup
The orchestrator broadcasts a `ClearOrder` message to all backend services with the final vector clock `VCf`. Each service checks if its local vector clock is less than or equal to `VCf` before clearing the cached order data. This ensures that all services have processed all events up to the final state before cleanup.

## Backend Microservices
Five backend microservices are available:

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

4. **Order Queue**
   - Listens on port `50054`.
   - Implements `Enqueue` and `Dequeue` RPCs with in-memory FIFO order queueing.
   - Located in the `order_queue` folder with its own Dockerfile.

5. **Order Executor (replicated)**
   - Listens on port `50055` inside each executor container.
   - Replicas run the same code and coordinate with a Bully leader-election algorithm.
   - Only the current leader dequeues and executes orders (logs `Order is being executed...`).
   - Located in the `order_executor` folder with its own Dockerfile.

The `docker-compose` file at the top level of the repository lists and orchestrates these services with the necessary configurations (name, port, volumes, etc.).

### Order Queue and Order Executor Services

#### Order Queue
The Order Queue service is responsible for queuing orders after they have been approved by the orchestrator. It implements the following gRPC functions:

- `Enqueue`: Enqueues an order into the queue.
- `Dequeue`: Dequeues an order from the queue.
- `GetQueueStatus`: Returns the current status of the queue.

The Order Queue service listens on port `50054` and is located in the `order_queue` folder with its own Dockerfile.

#### Order Executor
The Order Executor service is responsible for dequeuing orders from the Order Queue and executing them. It is replicated to ensure high availability and fault tolerance. The Order Executor service implements the following gRPC functions:

- `Ping`: Checks if the executor is alive.
- `Election`: Participates in the leader election process.
- `AnnounceLeader`: Announces the elected leader to other executors.
- `GetStatus`: Returns the current status of the executor.

The Order Executor service listens on port `50055` and is located in the `order_executor` folder with its own Dockerfile.

#### Leader Election Mechanism
The Order Executor service uses the Bully algorithm for leader election. The Bully algorithm ensures that the highest reachable executor ID is elected as the leader. The leader is responsible for dequeuing and executing orders from the Order Queue. If the leader fails, a new leader is elected from the remaining executors.

The Bully algorithm works as follows:
1. When an executor detects that the current leader is down, it initiates an election.
2. The executor sends election messages to all higher-priority executors.
3. If a higher-priority executor responds, it takes over the election process.
4. If no higher-priority executor responds, the initiator becomes the new leader and announces its leadership to all other executors.

This mechanism ensures that only one leader is active at any given time, preventing concurrent access to the Order Queue and ensuring mutual exclusion.

## gRPC Communication Setup
Communication channels to backend microservices are established using gRPC.

### gRPC Functions for Events
The orchestrator invokes explicit event RPCs in transaction verification, fraud detection, and suggestions, and propagates vector clocks on every event call. The following gRPC functions are implemented for each event:

- `ValidateItems`: Validates if the order items are not empty.
- `ValidateUserData`: Validates if all mandatory user data is filled in.
- `ValidateCardFormat`: Validates if the credit card information is in the correct format.
- `PrepareSuggestionsContext`: Prepares the context for generating book suggestions.
- `CheckUserFraud`: Checks the user data for fraud.
- `CheckCardFraud`: Checks the credit card data for fraud.
- `GenerateSuggestions`: Generates book suggestions.

### Order Queue and Executor Communication
- After a successful flow (`Order Approved`), the orchestrator enqueues the approved order in the order queue and waits for enqueue confirmation.
- Executors coordinate among themselves via gRPC (`Ping`, `Election`, `AnnounceLeader`) and the elected leader performs dequeue/execute.

The gRPC proto file specifications are in the `utils/pb` folder.

## Event Ordering and Vector Clocks
Seminar 5 is implemented with backend-only vector clocks (components: `transaction_verification`, `fraud_detection`, `suggestions`) and explicit event ordering.

### Event Flow
The orchestrator defines an event flow with dependencies to ensure correct execution order. The event flow includes the following events:

1. `validate_items` (transaction verification): Validates if the order items are not empty.
2. `validate_user_data` (transaction verification): Validates if all mandatory user data is filled in.
3. `validate_card_format` (transaction verification): Validates if the credit card information is in the correct format. Depends on `validate_items`.
4. `prepare_suggestions_context` (suggestions): Prepares the context for generating book suggestions. Depends on `validate_items`.
5. `check_user_fraud` (fraud detection): Checks the user data for fraud. Depends on `validate_user_data`.
6. `check_card_fraud` (fraud detection): Checks the credit card data for fraud. Depends on both `validate_card_format` and `check_user_fraud`.
7. `generate_suggestions` (suggestions): Generates book suggestions. Depends on both `prepare_suggestions_context` and `check_card_fraud`.

### Concurrency and Vector Clocks
Concurrency is present by design. For example, `validate_items` and `validate_user_data` can run in parallel. Each event logs the current vector clock for the given `orderId`, capturing the causal relationships between events. The vector clocks are updated and propagated with each event execution to ensure correct event ordering and consistency.

### Vector Clock Implementation
Each backend service initializes a vector clock for each order during the `InitOrder` phase. The vector clock is updated with each event execution, and the updated clock is returned to the orchestrator. The orchestrator merges the vector clocks from all events to compute the final vector clock `VCf`, which is used during the cleanup phase to ensure all services have processed all events up to the final state.

## Backend State Caching
Each backend service caches per-order data in memory, keyed by `orderId`. On `InitOrder`, a service stores the order payload and initializes local state including the vector clock. Event RPC handlers merge incoming clocks, increment their local component, execute dummy logic, and return the updated clock.
Cached order state is periodically pruned with TTL settings (`ORDER_STATE_TTL_SECONDS`, `ORDER_STATE_CLEANUP_INTERVAL_SECONDS`) to avoid unbounded growth.

### Response Handling and Failure Propagation
If an event fails (business validation or fraud check), the orchestrator immediately marks the checkout as failed, cancels pending work, and returns `Order Rejected`. If all events succeed, the orchestrator returns `Order Approved` with suggested books. In both cases, the orchestrator broadcasts a cleanup message to all services to clear the cached order data.

### Broadcast Mechanism for Cleanup
The orchestrator broadcasts a `ClearOrder` message to all backend services with the final vector clock `VCf`. Each service checks if its local vector clock is less than or equal to `VCf` before clearing the cached order data. This ensures that all services have processed all events up to the final state before cleanup.

## Failure Propagation and Cleanup
- If an event fails (business validation or fraud check), the orchestrator immediately marks the checkout as failed, cancels pending work, and returns `Order Rejected`.
- If all events succeed, the orchestrator returns `Order Approved` with suggested books.
- As the final step (success or failure), the orchestrator broadcasts `ClearOrder` with final vector clock `VCf`.
- Each backend clears local cached state only when local clock is less than or equal to `VCf`; otherwise it reports a cleanup error.

## Results Consolidation
The orchestrator combines intermediate event responses and produces a final checkout result:
- It returns "Order Rejected" immediately when a required intermediate event fails.
- Otherwise, once all required events finish successfully, it returns "Order Approved" with book suggestions.

## System Logging
Relevant logs are added in all services to track initialization, event execution, vector clock updates, event outcomes, and final cleanup decisions.

## Setup and Running the Services
1. Ensure Docker and Docker Compose are installed.
2. Build and start the services using the `docker-compose` file:
   ```sh
   docker compose up
   ```

Default startup runs two order-executor replicas (`order_executor_1`, `order_executor_2`).

To run additional replicas:
- 3 replicas total:
   ```sh
   docker compose --profile n3 up
   ```
- 4 replicas total:
   ```sh
   docker compose --profile n4 up
   ```

The Bully algorithm will elect the highest reachable executor ID as leader.

The orchestrator includes `GET /health`, and Docker Compose uses healthchecks for service startup ordering.

## Architecture Diagram

<img src="architecture.png">

## System Diagram

<img src="system.png">
