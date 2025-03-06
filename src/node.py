from flask import Flask, request, jsonify
import json
import sys
import time
import random
import threading
import requests
from concurrent.futures import ThreadPoolExecutor


# Flask app
app = Flask(__name__)


# ThreadPoolExecutor to handle sending tasks concurrently
executor = ThreadPoolExecutor(max_workers=10)

# store topics
topics = {}

# Raft state
current_role = "Follower"
current_term = 0
last_heartbeat_time = 0
election_timeout = 0
leader_id = None
voted_for = None
other_nodes = []

# For election voting
election_votes = {}
votes_lock = threading.Lock()

# logging
log = []
commit_index = 0
last_applied = 0

# leader volatile state
next_index = {}
match_index = {}


# Configuration
HEARTBEAT_INTERVAL = 0.2  # seconds
MIN_ELECTION_TIMEOUT = 0.3  # seconds
MAX_ELECTION_TIMEOUT = 0.5  # seconds

# setup the threadpool
executor = ThreadPoolExecutor(max_workers=10)


#########################
# REST API Endpoints    #
#########################
# Only the leader will serve client requests.


@app.route("/topic", methods=["PUT"])
def create_topic():
    global current_role, leader_id
    if current_role != "Leader":
        # If not leader, return failure with leader info if known
        response = {"success": False}
        if leader_id is not None:
            response["leader_id"] = leader_id
        return jsonify(response), 400

    data = request.json
    topic = data.get("topic", None)
    command = {"type": "create_topic", "topic": request.json.get("topic")}
    log.append()

    # Error handling
    if not topic:
        return jsonify({"success": False}), 400
    if topic in topics:
        return jsonify({"success": False}), 400

    topics[topic] = []
    return jsonify({"success": True}), 201


@app.route("/topic", methods=["GET"])
def get_topics():
    global current_role, leader_id
    if current_role != "Leader":
        # If not leader, return failure with leader info if known
        response = {"success": False}
        if leader_id is not None:
            response["leader_id"] = leader_id
        return jsonify(response), 400

    return jsonify({"success": True, "topics": list(topics.keys())}), 200


@app.route("/message", methods=["PUT"])
def add_message():
    global current_role, leader_id
    if current_role != "Leader":
        # If not leader, return failure with leader info if known
        response = {"success": False}
        if leader_id is not None:
            response["leader_id"] = leader_id
        return jsonify(response), 400

    data = request.json
    topic = data.get("topic", None)
    message = data.get("message", None)

    if not topic or not message:
        return jsonify({"success": False}), 400

    if topic not in topics:
        return jsonify({"success": False}), 400

    topics[topic].append(message)
    return jsonify({"success": True}), 201


@app.route("/message/<topic>", methods=["GET"])
def get_message(topic):
    global current_role, leader_id
    if current_role != "Leader":
        # If not leader, return failure with leader info if known
        response = {"success": False}
        if leader_id is not None:
            response["leader_id"] = leader_id
        return jsonify(response), 400

    # Check if topic exists
    if topic not in topics:
        return jsonify({"success": False}), 400

    # Check if topic has messages
    if not topics[topic]:
        return jsonify({"success": False}), 400

    # Pop the first message
    message = topics[topic].pop(0)
    return jsonify({"success": True, "message": message}), 200


@app.route("/status", methods=["GET"])
def status():
    return jsonify({"role": current_role, "term": current_term}), 200


###########################
# Raft RPC Endpoints     #
###########################


@app.route("/request_vote", methods=["POST"])
def handle_request_vote():
    data = request.json
    response = request_vote(data)
    return jsonify(response), 200


@app.route("/append_entries", methods=["POST"])
def handle_append_entries():
    data = request.json
    response = append_entries(data)
    return jsonify(response), 200


###########################
# Log Functionality       #
###########################
def apply_command(command):
    """
    Applies a command to the log
    """
    if command["type"] == "create_topic":
        if command["topic"] not in topics:
            topics[command["topic"]] = []
            return True
        return False
    elif command["type"] == "add_message":
        if command["topic"] not in topics:
            return False
        topics[command["topic"]].append(command["message"])
        return True
    elif command["type"] == "get_message":
        if command["topic"] not in topics or not topics[command["topic"]]:
            return False
        return topics[command["topic"]].pop(0)
    return False


###########################
# Raft RPC Functionality  #
###########################
def request_vote(data):
    """
    Process a request for a vote from another node
    """
    # Set global variables to modify within the function
    global current_role, current_term, voted_for, log, last_heartbeat_time

    candidate_term = data.get("term", 0)
    candidate_id = data.get("candidate_id")

    print(
        f"Node {node_id} received vote request from {candidate_id} for term {candidate_term}"
    )

    # If candidate's term is less than current term, reject
    if candidate_term < current_term:
        print(
            f"Node {node_id} rejected vote: candidate term {candidate_term} < current term {current_term}"
        )
        return {"term": current_term, "vote_granted": False}

    # If it's a new term, update our term and consider voting
    if candidate_term > current_term:
        current_term = candidate_term
        voted_for = None  # Reset vote for new term
        # If we were leader or candidate, step down
        if current_role != "Follower":
            print(
                f"Node {node_id} stepping down to follower due to higher term {candidate_term}"
            )
            current_role = "Follower"

    # Decide whether to vote for this candidate. if we've already voted this term, don't vote again
    if voted_for is None or voted_for == candidate_id:
        # Vote for this candidate
        voted_for = candidate_id
        # Reset election timeout after voting
        last_heartbeat_time = time.time()
        reset_election_timeout()

        print(
            f"Node {node_id} granted vote to {candidate_id} for term {candidate_term}"
        )
        return {"term": current_term, "vote_granted": True}

    # Otherwise we have already voted for someone else
    print(f"Node {node_id} rejected vote: already voted for {voted_for}")
    return {
        "term": current_term,
        "vote_granted": False,
    }


def append_entries(data):
    """
    Process an AppendEntries RPC from another node (heartbeat or log entry)
    """
    # Process heartbeats and log entries
    global current_term, current_role, last_heartbeat_time, leader_id, voted_for

    leader_term = data.get("term", 0)
    leader_id_from_msg = data.get("leader_id")

    # If leader's term is less than current term, reject
    if leader_term < current_term:
        print(
            f"Node {node_id} rejected append entries: leader term {leader_term} < current term {current_term}"
        )
        return {
            "term": current_term,
            "success": False,
        }

    # Valid leader message received

    # If leader's term is greater, update our term
    if leader_term > current_term:
        current_term = leader_term
        voted_for = None  # Reset vote for new term

    # Accept this node as leader for our term
    leader_id = leader_id_from_msg

    # Always step down to follower when receiving valid append entries (can just always step down)
    if current_role != "Follower":
        print(
            f"Node {node_id} stepping down to follower due to valid append entries from {leader_id}"
        )
        current_role = "Follower"

    # Update heartbeat time and reset timeout
    last_heartbeat_time = time.time()
    reset_election_timeout()

    print(
        f"Node {node_id} received valid heartbeat from leader {leader_id} for term {leader_term}"
    )

    # Process log entries (not implemented for this basic version)
    # For now, just acknowledge the append entries
    return {
        "term": current_term,
        "success": True,
    }


def reset_election_timeout():
    global election_timeout
    # Add a timeout of 1.5 to 3 seconds
    election_timeout = time.time() + random.uniform(
        MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT
    )


#################################
# Election Timeout and Voting   #
#################################
def check_election_timeout():
    global current_role, election_timeout
    while True:
        # Check every 100ms
        time.sleep(0.1)

        # Start election if we're not a leader and timeout has expired.
        # our heartbeat resets election timeout anyway so we just compare current time to election timeout
        current_time = time.time()
        if current_role != "Leader" and current_time > election_timeout:
            print(
                f"Node {node_id} election timeout at time {current_time}, timeout was {election_timeout}"
            )
            start_election()


def start_election():
    """
    When a node times out, it starts an election.
    - Increment term
    - Become candidate
    - Vote for self
    - Send vote requests to other nodes
    """
    global current_role, current_term, voted_for, node_id, election_votes, leader_id

    if current_role == "Leader":
        return

    # Increment term and become a candidate
    current_term += 1
    current_role = "Candidate"
    voted_for = node_id

    # Reset election timeout
    reset_election_timeout()

    print(f"Node {node_id} starting election for term {current_term}")
    # Clear previous election votes
    with votes_lock:
        election_votes.clear()
        election_votes[node_id] = True  # Vote for self

        if len(other_nodes) == 0:
            current_role = "Leader"
            leader_id = node_id
            print(
                f"Node {node_id} became leader for term {current_term} (single-node cluster)"
            )
            become_leader()

    # Request votes from all other nodes
    for node in other_nodes:
        executor.submit(request_vote_from_node, node)


def request_vote_from_node(node):
    """Send a vote request to a single node"""
    global current_term, node_id, election_votes, current_role, voted_for
    try:
        url = f"http://{node['ip']}:{node['port']}/request_vote"
        print(f"Node {node_id} sending vote request to {url}")

        response = requests.post(
            url,
            json={
                "term": current_term,
                "candidate_id": node_id,
                "last_log_index": len(log),
                "last_log_term": 0 if not log else log[-1].get("term", 0),
            },
            timeout=1.0,
        )
        if response.status_code == 200:
            data = response.json()
            # If we get a higher term, update our term and step down
            if data.get("term", 0) > current_term:
                current_term = data.get("term")
                current_role = "Follower"
                voted_for = None
                print(
                    f"Node {node_id} stepping down due to higher term from {node['id']}"
                )
                return

            vote_granted = data.get("vote_granted", False)
            if vote_granted:
                print(
                    f"Node {node_id} received vote from {node['id']} for term {current_term}"
                )
            else:
                print(
                    f"Node {node_id} did not receive vote from {node['id']} for term {current_term}"
                )
            should_become_leader = False
            with votes_lock:
                election_votes[node["id"]] = vote_granted
                votes_recieved = sum(vote for vote in election_votes.values() if vote)
                total_nodes = len(other_nodes) + 1  # include self
                if votes_recieved > total_nodes // 2:
                    print(
                        f"Node {node_id} won election with {votes_recieved} votes out of {total_nodes}"
                    )
                    should_become_leader = True

            if should_become_leader:
                print(f"Node {node_id} became leader for term {current_term}")
                become_leader()

    except requests.exceptions.RequestException as e:
        print(f"Error requesting vote from {node['id']}: {e}")


def become_leader():
    """Transition to leader state"""
    global current_role, leader_id, node_id

    if current_role != "Candidate":
        return

    current_role = "Leader"
    leader_id = node_id

    print(f"Node {node_id} became leader for term {current_term}")

    # Send initial heartbeat to establish authority
    send_heartbeat_to_all()


##########################
# Heartbeat Functionality#
##########################


def send_heartbeat_to_node(node):
    """
    Send a heartbeat to a single node (empty AppendEntries)
    """
    global current_term, node_id, current_role, voted_for
    try:
        url = f"http://{node['ip']}:{node['port']}/append_entries"
        response = requests.post(
            url,
            json={
                "term": current_term,
                "leader_id": node_id,
                "prev_log_index": 0,
                "prev_log_term": 0,
                "entries": [],  # Empty for heartbeat
                "leader_commit": 0,
            },
            timeout=1.0,
        )
        if response.status_code == 200:
            # we do this for every server, so we need to check if we are still the leader
            data = response.json()
            if data.get("term", 0) > current_term:
                current_term = data.get("term")
                current_role = "Follower"
                voted_for = None
                print(
                    f"Node {node_id} stepping down due to higher term from {node['id']}"
                )

    except requests.exceptions.RequestException as e:
        print(f"Error sending heartbeat to {node['id']}: {e}")


def send_heartbeat_to_all():
    """Send a heartbeat to all other nodes (empty AppendEntries)"""
    global current_role

    if current_role != "Leader":
        return

    for node in other_nodes:
        executor.submit(send_heartbeat_to_node, node)


def heartbeat_loop():
    """Periodically send heartbeats if this node is the leader"""
    while True:
        time.sleep(HEARTBEAT_INTERVAL / 2)  # Send heartbeats at twice the interval rate
        if current_role == "Leader":
            send_heartbeat_to_all()


def main():
    global last_heartbeat_time, node_id, other_nodes

    if len(sys.argv) != 3:
        print("Usage: python3 src/node.py path_to_config index")
        sys.exit(1)

    config_path = sys.argv[1]
    node_index = int(sys.argv[2])
    node_id = str(node_index)

    with open(config_path, "r") as f:
        config = json.load(f)

    for i, address in enumerate(config["addresses"]):
        if i != node_index:
            node_info = {
                "id": str(i),
                "port": address["port"],
                "ip": address["ip"].replace("http://", ""),
            }

            other_nodes.append(node_info)

    node_address = config["addresses"][node_index]
    node_port = node_address["port"]
    node_ip = node_address["ip"].replace("http://", "")

    # Set initial election timeout and heartbeat time
    reset_election_timeout()
    last_heartbeat_time = time.time()

    # Start background threads
    threading.Thread(target=check_election_timeout, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    print(f"Node {node_id} starting on {node_ip}:{node_port}")

    # Start Flask server for client interactions
    app.run(host=node_ip, port=node_port, debug=False)


if __name__ == "__main__":
    main()
