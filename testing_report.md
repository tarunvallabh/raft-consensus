# Testing Report

## Part 1 (Message Queue)

### Testing Approach
To validate the single-node message queue implementation, I employed both automated and manual testing strategies:

- Started with the provided test cases for basic queue functionality
- Created additional tests in **additional_tests.py** to thoroughly verify queue operations:
  - `test_put_message_to_nonexistent_topic`: Verifying proper handling of invalid operations
  - `test_put_empty_message`: Ensuring rejection of invalid message content
  - `test_order_with_mult_messages`: Testing ordering with multiple messages
  - `test_multiple_topics_independence`: Confirming operations on one topic don't affect others
- Manually tested edge cases by directly interacting with the server
- Verified proper ordering by confirming messages were consumed in the same order they were produced

### Limitations
- No stress testing was performed to evaluate how the queue would handle a very large number of topics or messages

## Part 2 (Leader Election)

### Testing Approach
For the Raft leader election component, I implemented the following strategy:

- Built upon the provided tests which verified basic leader election functionality
- Added these specific tests in **additional_tests.py**:
  - `test_term_increments_on_new_election`: Verifying term increases with new elections
  - `test_leader_step_down`: Testing leader behavior when rejoining with higher term
  - `test_majority_requirement`: Ensuring a majority is required for election
  - `test_election_resilience_to_single_node_failure`: Testing recovery from leader failure
- Scaled testing to verify the algorithm works with larger clusters by running tests with up to 15 nodes
- The system maintained stability even with heartbeat timers as low as 25-100ms
- Manually verified election behavior by inspecting logs during multiple election cycles

### Limitations
- Testing cannot fully simulate real-world network conditions like varying latencies
- Not all possible combinations of node failures and restarts were tested


## Part 3 (Log Replication)

### Testing Approach
For testing log replication and fault tolerance, I implemented:

- Started with provided tests that verified basic replication functionality
- Created additional tests in **additional_tests.py** focusing on key aspects:
  - `test_follower_catchup`: Verifying nodes can catch up after missing operations
  - `test_multiple_operations_ordering`: Ensuring consistent operation ordering
  - `test_message_order_after_restart`: Testing message persistence across restarts
  - `test_conflicting_log_resolution`: Verifying resolution of divergent logs
- Manually verified log consistency by checking state across nodes after operations (`node` subdirectory)
- Tested leader failover during active operation to validate seamless transition

### Limitations
- Limited testing of node recovery with significantly divergent logs
- The current `wait_for_log_commit` function uses a fixed timeout, which might be insufficient for large clusters
- Hard-to-reproduce race conditions that might occur between commit operations and leader changes weren't systematically tested.
- When clients contact non-leader nodes, they simply receive error responses without information about which node is the current leader. There is no redirection. 

## Additional Testing Notes
### Test Execution
Tests were run using pytest with the command pytest test/additional_tests.py. Please set the `PROGRAM_FILE_PATH = "../src/node.py"` in the each of the test files in order to ensure that the tests run and call the correct file. 

### Shutdown Handling
I implemented SIGTERM handlers to gracefully clean up persistent state during node shutdown. However, if tests are abruptly terminated, users may need to manually delete the node folder to ensure a clean state for subsequent test runs. This is particularly important when testing log persistence and recovery behaviors, as stale state files can affect test outcomes.




