from test_utils import Swarm, Node, LEADER, FOLLOWER, CANDIDATE
import pytest
import time
import requests

TEST_TOPIC = "test_topic"
TEST_MESSAGE = "Test Message"
PROGRAM_FILE_PATH = "../src/node.py"
ELECTION_TIMEOUT = 2.0

# array of numbr of nodes spawned on tests, an example could be [3,5,7,11,...]
# default is only 5 for faster tests
NUM_NODES_ARRAY = [5]

NUMBER_OF_LOOP_FOR_SEARCHING_LEADER = 3


# Test Methods
@pytest.fixture
def node_with_test_topic():
    node = Swarm(PROGRAM_FILE_PATH, 1)[0]
    node.start()
    time.sleep(ELECTION_TIMEOUT)
    assert node.create_topic(TEST_TOPIC).json() == {"success": True}
    yield node
    node.clean()


@pytest.fixture
def node():
    node = Swarm(PROGRAM_FILE_PATH, 1)[0]
    node.start()
    time.sleep(ELECTION_TIMEOUT)
    yield node
    node.clean()


@pytest.fixture
def swarm(num_nodes):
    swarm = Swarm(PROGRAM_FILE_PATH, num_nodes)
    swarm.remove_persistent_state()
    swarm.start(ELECTION_TIMEOUT)
    yield swarm
    swarm.clean()


def collect_leaders_in_buckets(leader_each_terms: dict, new_statuses: list):
    for i, status in new_statuses.items():
        assert "term" in status.keys()
        term = status["term"]
        assert "role" in status.keys()
        role = status["role"]
        if role == LEADER:
            leader_each_terms[term] = leader_each_terms.get(term, set())
            leader_each_terms[term].add(i)


def assert_leader_uniqueness_each_term(leader_each_terms):
    for leader_set in leader_each_terms.values():
        assert len(leader_set) <= 1


def wait_for_commit(seconds=1):
    time.sleep(seconds)


# MESSAGE TESTS
def test_put_message_to_nonexistent_topic(node):
    """Test trying to put a message to a topic that doesn't exist"""
    response = node.put_message("nonexistent_topic", TEST_MESSAGE)
    assert response.json() == {"success": False}


def test_put_empty_message(node_with_test_topic):
    """Test putting an empty message to a topic"""
    response = node_with_test_topic.put_message(TEST_TOPIC, "")
    # reject empty messages
    assert response.json() == {"success": False}


def test_order_with_mult_messages(node_with_test_topic):
    """Test ordering  with multiple messages"""
    messages = [f"Message {i}" for i in range(10)]
    for msg in messages:
        response = node_with_test_topic.put_message(TEST_TOPIC, msg)
        assert response.json() == {"success": True}

    # verify they come out in the same order
    for i, expected_msg in enumerate(messages):
        response = node_with_test_topic.get_message(TEST_TOPIC)
        assert response.json() == {
            "success": True,
            "message": expected_msg,
        }, f"Failed at message {i}"


def test_multiple_topics_independence(node):
    """Test that messages in different topics don't interfere with each other"""

    topic1 = "topic1"
    topic2 = "topic2"
    node.create_topic(topic1)
    node.create_topic(topic2)

    for i in range(3):
        node.put_message(topic1, f"Topic1-Message{i}")
        node.put_message(topic2, f"Topic2-Message{i}")

    # verify messages
    for i in range(3):
        response = node.get_message(topic1)
        assert response.json() == {"success": True, "message": f"Topic1-Message{i}"}

    for i in range(3):
        response = node.get_message(topic2)
        assert response.json() == {"success": True, "message": f"Topic2-Message{i}"}


# ELECTION TESTS


@pytest.mark.parametrize("num_nodes", NUM_NODES_ARRAY)
def test_term_increments_on_new_election(swarm: Swarm, num_nodes: int):
    """Test that terms increase when leaders change"""
    leader1 = swarm.get_leader_loop(3)
    assert leader1 != None
    initial_term = leader1.get_status().json()["term"]

    # Kill leader and wait for new election
    leader1.clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(3)
    assert leader2 != None
    new_term = leader2.get_status().json()["term"]

    # New term should be higher than previous term
    assert new_term > initial_term


@pytest.mark.parametrize("num_nodes", NUM_NODES_ARRAY)
def test_leader_step_down(swarm: Swarm, num_nodes: int):
    """Test that a former leader steps down when it rejoins the cluster with a higher term"""
    if num_nodes < 3:
        pytest.skip("This test requires at least 3 nodes")

    # Get initial leader and term
    leader1 = swarm.get_leader_loop(3)
    assert leader1 != None
    initial_term = leader1.get_status().json()["term"]
    initial_leader_id = leader1.i

    # Shut down the leader temporarily
    leader1.clean()

    # Wait for a new leader to be elected
    time.sleep(ELECTION_TIMEOUT * 2)
    leader2 = swarm.get_leader_loop(3)
    assert leader2 != None
    assert leader2.i != initial_leader_id

    # Start the old leader again - it should come back as a follower
    # since there's now a leader with a higher term
    swarm.nodes[initial_leader_id].start()
    time.sleep(ELECTION_TIMEOUT)

    # Verify the old leader is now a follower and has updated its term
    status = swarm.nodes[initial_leader_id].get_status().json()
    assert status["role"] == FOLLOWER
    assert status["term"] >= initial_term + 1


@pytest.mark.parametrize("num_nodes", NUM_NODES_ARRAY)
def test_majority_requirement(swarm: Swarm, num_nodes: int):
    """Test that a majority of nodes is required for leader election"""
    if num_nodes < 3:
        pytest.skip("This test requires at least 3 nodes")

    # Get initial leader and term
    leader = swarm.get_leader_loop(3)
    assert leader != None

    # Kill enough nodes to prevent a majority (more than half)
    kill_count = (num_nodes // 2) + 1
    for i in range(kill_count):
        if i == 0 and swarm.nodes[i].i == leader.i:
            # Make sure we don't skip the leader
            continue
        swarm.nodes[i].clean()

    # Kill the leader last if it wasn't already killed
    if leader.get_status().ok:
        leader.clean()

    # Wait some time to see if new election happens
    time.sleep(ELECTION_TIMEOUT * 2)

    # Check if any node became leader (none should)
    new_leader = swarm.get_leader()
    assert new_leader is None


@pytest.mark.parametrize("num_nodes", [3])
def test_election_resilience_to_single_node_failure(swarm: Swarm, num_nodes: int):
    """Test that the cluster can elect a leader despite a single node failure"""

    leader = swarm.get_leader_loop(3)
    assert leader is not None

    leader.clean()

    time.sleep(ELECTION_TIMEOUT)
    new_leader = swarm.get_leader_loop(3)
    assert new_leader is not None
    assert new_leader.i != leader.i


# LOG REPLICATION TESTS


@pytest.mark.parametrize("num_nodes", NUM_NODES_ARRAY)
def test_follower_catchup(swarm: Swarm, num_nodes: int):
    """Test that a follower that rejoins can catch up with missed operations"""
    if num_nodes < 3:
        pytest.skip("This test requires at least 3 nodes")

    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader != None
    follower_idx = (leader.i + 1) % num_nodes
    follower = swarm.nodes[follower_idx]

    # create topic with leader
    assert leader.create_topic("catchup_topic").json() == {"success": True}

    # stop follower
    follower.clean()

    # add messages while follower is down
    assert leader.put_message("catchup_topic", "message1").json() == {"success": True}
    assert leader.put_message("catchup_topic", "message2").json() == {"success": True}

    # restart follower and wait for catch-up
    follower.start()
    time.sleep(ELECTION_TIMEOUT)

    # kill the original leader to force election
    leader.clean(ELECTION_TIMEOUT)

    # find new leader
    new_leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert new_leader != None

    assert new_leader.get_message("catchup_topic").json() == {
        "success": True,
        "message": "message1",
    }
    assert new_leader.get_message("catchup_topic").json() == {
        "success": True,
        "message": "message2",
    }


@pytest.mark.parametrize("num_nodes", NUM_NODES_ARRAY)
def test_multiple_operations_ordering(swarm: Swarm, num_nodes: int):
    """Test multiple operations maintain proper ordering"""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader != None

    assert leader.create_topic("topic1").json() == {"success": True}
    assert leader.create_topic("topic2").json() == {"success": True}

    # add multiple messages to each topic
    assert leader.put_message("topic1", "A1").json() == {"success": True}
    assert leader.put_message("topic2", "B1").json() == {"success": True}
    assert leader.put_message("topic1", "A2").json() == {"success": True}
    assert leader.put_message("topic2", "B2").json() == {"success": True}

    assert leader.get_message("topic1").json() == {"success": True, "message": "A1"}
    assert leader.get_message("topic2").json() == {"success": True, "message": "B1"}
    assert leader.get_message("topic1").json() == {"success": True, "message": "A2"}
    assert leader.get_message("topic2").json() == {"success": True, "message": "B2"}


@pytest.mark.parametrize("num_nodes", NUM_NODES_ARRAY)
def test_message_order_after_restart(swarm: Swarm, num_nodes: int):
    """Test that  order is preserved after restart"""
    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader != None

    assert leader.create_topic("fifo_topic").json() == {"success": True}
    for i in range(1, 6):  # Add 5 messages
        assert leader.put_message("fifo_topic", f"message{i}").json() == {
            "success": True
        }

    for node in swarm.nodes:
        node.kill()
    time.sleep(ELECTION_TIMEOUT)
    swarm.start(ELECTION_TIMEOUT)

    # get new leader
    new_leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert new_leader != None

    # verify messages are retrieved in correct order
    for i in range(1, 6):
        response = new_leader.get_message("fifo_topic").json()
        assert response == {"success": True, "message": f"message{i}"}


@pytest.mark.parametrize("num_nodes", NUM_NODES_ARRAY)
def test_conflicting_log_resolution(swarm: Swarm, num_nodes: int):
    """Tests that a node with conflicting logs correctly reconciles with the leader"""
    if num_nodes < 3:
        pytest.skip("This test requires at least 3 nodes")

    leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert leader is not None

    assert leader.create_topic("conflict_topic").json() == {"success": True}
    assert leader.put_message("conflict_topic", "original_msg").json() == {
        "success": True
    }

    # kill leader and get a new one
    leader.clean(ELECTION_TIMEOUT)
    new_leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert new_leader is not None

    # add a different message with new leader
    assert new_leader.put_message("conflict_topic", "new_leader_msg").json() == {
        "success": True
    }

    # restart original leader (which has the original message but is missing the second)
    swarm.nodes[leader.i].start()
    time.sleep(ELECTION_TIMEOUT)

    # kill current leader to potentially make the original leader become leader again
    new_leader.clean(ELECTION_TIMEOUT)
    final_leader = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert final_leader is not None

    # Get messages - should come out in correct order regardless of which node is leader
    response1 = final_leader.get_message("conflict_topic").json()
    assert response1 == {"success": True, "message": "original_msg"}
    response2 = final_leader.get_message("conflict_topic").json()
    assert response2 == {"success": True, "message": "new_leader_msg"}
