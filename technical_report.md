# MPCS 52040 Final Project (RAFT)

## Introduction

The Raft Message Queue implementation provides a distributed message queue system based on the Raft consensus algorithm. This report details the development process, design decisions, and challenges encountered while building a system that maintains consistency across multiple nodes while ensuring fault tolerance.

## Part 1: Single Node Message Queue Implementation

### Implementation Approach

The single-node message queue serves as the foundation for the distributed system. I opted for a straightforward approach using Python's built-in data structures. Topics are stored in a dictionary with topic names as keys and lists of messages as values, enabling efficient operations.

For the REST API, I implemented the required endpoints using Flask:
- `/topic` (PUT/GET) for topic creation and listing
- `/message` (PUT) for adding messages to a topic
- `/message/<topic>` (GET) for retrieving messages from a topic

Each endpoint validates input parameters and returns appropriate HTTP status codes along with JSON responses conforming to the specified format. 

### Design Challenges and Decisions

The primary challenge in the single-node implementation was balancing simplicity with robustness. I chose to use Python's built-in data structures for their simplicity and readability, rather than implementing more complex queue mechanisms.

For request validation, I implemented basic checks for parameter presence and topic existence. While more sophisticated validation could have been added, I prioritized meeting the core requirements while maintaining code clarity.

### Shortcomings

While the single-node implementation meets the project requirements, it has several limitations:

While the API implementation meets the requirements, there's no mechanism to limit queue size, making it vulnerable to memory exhaustion under heavy load. Additionally, the basic error handling would need enhancement for a production environment.

## Part 2: Raft Leader Election Algorithm

### Implementation Approach

Implementing the Raft leader election algorithm presented significant challenges in managing distributed state and concurrent operations. I initially experimented with different approaches for managing election timeouts and heartbeats before settling on an effective solution.

The implementation follows the Raft paper closely, with each node maintaining one of three states: Follower, Candidate, or Leader. The key components include:

**Timers:** I created a custom `RPCTimer` class that implements randomized election timeouts, crucial for preventing split votes. Each node starts with an election timer that triggers a new election when it expires. 

**Election Process:** When a node's election timer expires, it transitions to the Candidate state, increments its term, votes for itself, and requests votes from other nodes. If it receives votes from a majority, it becomes the Leader.

**Term-Based Protocol:** Each node tracks its current term, and all communications include term information. Higher terms take precedence, causing nodes to update their terms and revert to Follower state when they encounter higher terms.

**Concurrency Management:** I used a ThreadPoolExecutor to handle concurrent RPC processing, which allowed operations like vote requests and heartbeats to run asynchronously without blocking the main thread. To ensure thread safety, I implemented a re-entrant lock (`threading.RLock()`) that protects critical sections during concurrent operations. I specifically chose re-entrant locks after encountering several deadlock issues with regular locks, as re-entrant locks allow a thread to acquire the same lock multiple times without blocking itself.


### Design Challenges and Decisions

One of the most challenging aspects was implementing the asynchronous nature of the leader election process. Multiple nodes can start elections simultaneously, and messages may be delayed or lost. I faced several race conditions and deadlocks during early testing, particularly when multiple nodes were transitioning between states. These issues often occurred because multiple threads were trying to update the same state variables at the same time.

After several iterations, I settled on a design that:
1. Uses separate endpoints for RequestVote and AppendEntries RPCs
2. Implements careful state checking with proper locking to prevent inconsistencies
3. Uses a ThreadPoolExecutor to make RPC calls asynchronous
4. Employs re-entrant locks to prevent deadlocks when the same thread needs to acquire the lock multiple times
5. Uses a heartbeat mechanism for leaders to maintain authority
6. Handles term updates properly to ensure safety properties

I initially struggled with timer management - ensuring that election timeouts were properly reset upon receiving heartbeats and that leaders maintained a faster heartbeat rate. After experimenting with several designs, including simple timestamp comparisons and thread-based delay loops, I found that a dedicated timer class provided the most reliable behavior. The `RPCTimer` class evolved to handle these requirements effectively, with the ability to reset timeouts upon receiving heartbeats and manage the different timing requirements for election and heartbeat intervals.

### Shortcomings

The leader election implementation has a few limitations:

- The fixed timeout ranges may not be optimal for all network environments. A more adaptive approach could improve performance across varying network conditions.

- The current implementation has basic handling of network failures but lacks sophisticated retry mechanisms. This could potentially lead to unnecessary elections in unstable network conditions.

- During leadership transitions, there's a brief period where no leader is available to process client requests, which could impact availability under frequent leader changes.

## Part 3: Log Replication and Fault Tolerance

### Implementation Approach

For log replication, I extended the leader election framework to manage a replicated log across all nodes. The log consists of entries representing commands to be executed on the state machine (the message queue).

The implementation centers around a few key mechanisms:

**Command Logging:** Every client operation (creating topics, adding/retrieving messages) is first added to the leader's log before being replicated to followers. Log entries include the term, command type, and relevant parameters.

**AppendEntries Protocol:** The AppendEntries RPC serves dual purposes - as a heartbeat mechanism and for replicating log entries. When the leader has new entries, it sends them to followers along with information needed to verify log consistency.

**Consistency Checking:** Followers verify that entries being appended match their existing logs by checking the previous log entry's index and term. If inconsistencies are found, the follower rejects the entries, prompting the leader to back up and try with earlier entries.

**Log Consistency Resolution:** When a follower rejects an append entries request, the leader decrements the next index for that follower and will try again on the next heartbeat cycle, rather than implementing immediate retries. This approach simplifies the implementation while still ensuring eventual consistency, though it may take longer to resolve log inconsistencies.

**Commit Process:** The leader determines when an entry is committed (when it's replicated to a majority of nodes) and notifies followers of the new commit index during AppendEntries RPCs. Nodes only apply committed entries to their state machines.

**State Persistence and Cleanup:** Critical state (current term, voted for, and log) is persisted to disk, allowing nodes to recover from crashes without losing their state. I implemented a SIGTERM handler to ensure clean removal of persistent state when nodes are properly shut down, while preserving state for recovery after abnormal terminations (like SIGKILL). This distinction is important for testing fault tolerance - properly terminated nodes clean up after themselves, while crashed nodes leave their state for recovery.

**State Rebuilding:** When a node starts up, it rebuilds its state machine by replaying all committed log entries, ensuring consistency after restarts.

**Read Operation Safety:** For read operations (like getting a list of topics), I opted for a simplified approach compared to the Raft paper's recommendation. Instead of implementing the full linearizability guarantees with no-op entries, I have the leader send heartbeats before processing read requests. This helps verify the leader hasn't been superseded without its knowledge, providing a practical balance between correctness and implementation complexity. However, this approach doesn't guarantee linearizability in all scenarios as described in the Raft paper.

### Design Challenges and Decisions

Implementing log replication presented several complex challenges:

Handling log inconsistencies was particularly difficult. I needed to ensure that the leader could bring followers with outdated or inconsistent logs back into sync without corrupting the log. The AppendEntries consistency check was crucial here, allowing the leader to identify where a follower's log diverged and send the appropriate entries. I implemented a backtracking mechanism where the leader decrements the next index for a follower when replication fails, gradually finding a point of consistency.

Another challenge was determining when entries were committed and ensuring all nodes applied them in the same order. I implemented a wait mechanism (`wait_for_log_commit`) that allows the leader to wait until an entry is replicated to a majority before responding to clients.

Persisting state without degrading performance required careful consideration. I chose to write state to disk whenever the log, current term, or voted for fields changed, ensuring durability while minimizing unnecessary writes.

Managing persistence across node restarts presented an interesting challenge. I needed to distinguish between normal shutdowns and crash scenarios to properly test fault tolerance. I implemented a signal handler for SIGTERM that cleans up persistent state on graceful shutdowns, while allowing state to remain for recovery after crashes (SIGKILL). This approach ensured that tests could verify both proper cleanup and crash recovery without interference from previous test runs.

### Shortcomings

The log replication implementation has several limitations:

- There's no log compaction mechanism, which means logs will grow unbounded over time. This could lead to excessive disk usage and slow recovery times for nodes that have been down for extended periods.

- The implementation lacks support for dynamic cluster membership changes, making it difficult to add or remove nodes without restarting the entire cluster.

- The synchronous wait for commit confirmation introduces latency in client operations. While I used ThreadPoolExecutor to make many operations asynchronous, client requests still wait for confirmation of log replication. A more sophisticated approach could allow for asynchronous confirmation in certain scenarios.

- The error recovery mechanisms are limited. While basic crash recovery is supported, more complex failure scenarios might require manual intervention.

- The simplified approach for read operations (sending heartbeats before reads) may not provide the full linearizability guarantees described in the Raft paper, potentially allowing stale reads in certain edge cases with network partitions.

- When a follower rejects an append entries request due to log inconsistency, the leader waits until the next heartbeat cycle to retry with a decremented index rather than immediately retrying. This can increase the time to resolve log inconsistencies.



## Sources Used

1. Original Raft paper: "In Search of an Understandable Consensus Algorithm" by Diego Ongaro and John Ousterhout: https://raft.github.io/raft.pdf 
2. Raft Website and interactive visualization: https://raft.github.io/
3. "The Secret Lives of Data" Raft visualization: https://thesecretlivesofdata.com/raft/
4. Raft in Python: https://chelseatroy.com/wp-content/uploads/2020/09/Chelsea_Troy_Raft_In_Python.pdf

