import random
from threading import Timer
import threading
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify
import requests


node = None


class RPCTimer:
    def __init__(self, node_id, function, interval):
        self.function = function
        self.interval_range = interval
        # interval is a tuple of (lower_bound, upper_bound)
        self.interval = random.randint(interval[0], interval[1]) / 1000
        self.node_id = node_id
        self.type = "election" if function.__name__ == "start_election" else "heartbeat"
        # reference: https://docs.python.org/3/library/threading.html#timer-objects
        self.timer = Timer(self.interval, self.function)

    def reset_timer(self):
        self.timer.cancel()
        self.interval = (
            random.randint(self.interval_range[0], self.interval_range[1]) / 1000
        )
        self.timer = Timer(self.interval, self.function)
        self.timer.start()

    def stop_timer(self):
        self.timer.cancel()


class Node:
    def __init__(self, node_id, config, ip, port):
        self.id = node_id
        self.ip = ip
        self.port = port
        self.config = config

        # message queue and log state
        self.topics = {}
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {}
        self.match_index = {}

        # state variables
        self.current_term = 0
        self.current_role = "Follower"
        self.voted_for = None
        # equivalent to vote_count
        self.election_votes = {}

        # MAY NOT NEED BECAUSE WE JUST USE MATCHINDEX COMPARISION FOR THIS:
        self.commit_count = 0  # Number of threads that have committed
        self.commit_applied = False  # Whether the commit has been applied

        self.executor = ThreadPoolExecutor(max_workers=10)

        # LOCK
        self.lock = threading.Lock()

        self.other_nodes = []
        for i, address in enumerate(self.config["addresses"]):
            if i != int(self.id):
                other_node = {
                    "id": str(i),
                    "port": address["port"],
                    "ip": address["ip"].replace("http://", ""),
                }
                self.other_nodes.append(other_node)

        self.total_nodes = len(self.other_nodes) + 1

        self.heartbeat_timer = RPCTimer(self.id, self.send_heartbeat, (100, 200))
        # make timers for election and heartbeat
        self.election_timer = RPCTimer(self.id, self.start_election, (500, 1000))
        # start our election timer
        self.election_timer.timer.start()

    def start_election(self):
        with self.lock:
            self.current_term += 1
            self.current_role = "Candidate"
            # clear the prior votes as we are starting a new election
            self.election_votes = {self.id: True}

            self.election_timer.reset_timer()
        print(f"Node {self.id}: Starting election for term {self.current_term}")
        # check for a win here in case we are the only node (test case with 1 node)
        self.check_election_win()
        # if we haven't won, send out vote requests
        for each_node in self.other_nodes:
            self.executor.submit(self.send_vote_request, each_node)

    def check_election_win(self):
        """Check if we have won the election"""
        should_become_leader = False
        majority = (self.total_nodes // 2) + 1
        with self.lock:
            # count the votes we have received
            votes_received = sum(1 for vote in self.election_votes.values() if vote)

            if votes_received >= majority:
                # set this flag for proper handling of the lock
                should_become_leader = True
            # no matter what, lock gets released here
        if should_become_leader:
            self.become_leader()

    def become_leader(self):
        with self.lock:
            if self.current_role == "Leader":
                return
            self.current_role = "Leader"
            # stop the election timer
            self.election_timer.stop_timer()
            # initialize next_index and match_index for each node for log replication
            for each_node in self.other_nodes:
                # remem
                self.next_index[each_node["id"]] = len(self.log) + 1
                self.match_index[each_node["id"]] = 0
        # send a heartbeat now that we are the new leader
        self.send_heartbeat()

    def send_heartbeat(self):
        with self.lock:
            current_prev_log_index = len(self.log)
            # Use an empty list for entries if there are no new log entries
            heartbeat_entries = []

        print(f"Node {self.id}: Sending heartbeat for term {self.current_term}")

        # Submit heartbeat tasks for each follower (using self.other_nodes)
        for each_node in self.other_nodes:
            self.executor.submit(
                self.send_append_entries_to_node,
                each_node,
                current_prev_log_index,
                heartbeat_entries,
            )

        # Reset the heartbeat timer after sending heartbeats
        with self.lock:
            # only place where we reset the heartbeat timer, this function keeps getting called by the timer
            self.heartbeat_timer.reset_timer()

    def handle_vote_request(self, data):
        """Process a RequestVote RPC from a candidate"""
        print(f"Node {self.id}: Received vote request: {data}")

        with self.lock:
            # Extract data from request
            candidate_term = data.get("term", 0)
            candidate_id = data.get("candidate_id")

            response = {"term": self.current_term, "vote_granted": False}

            # If candidate's term is less than current term, reject vote
            if candidate_term < self.current_term:
                print(
                    f"Node {self.id}: Rejecting vote - candidate term {candidate_term} < my term {self.current_term}"
                )
                return response

            # If candidate's term is greater, update our term and step down
            if candidate_term > self.current_term:
                print(
                    f"Node {self.id}: Updating term from {self.current_term} to {candidate_term}"
                )
                self.current_term = candidate_term
                self.voted_for = None
                if self.current_role == "Leader":
                    self.heartbeat_timer.stop_timer()
                self.current_role = "Follower"

            # If we haven't voted for anyone in this term yet (or already voted for this candidate)
            if self.voted_for is None or self.voted_for == candidate_id:
                print(
                    f"Node {self.id}: Granting vote to {candidate_id} for term {candidate_term}"
                )
                self.voted_for = candidate_id
                response["vote_granted"] = True

                # Reset election timeout
                self.election_timer.reset_timer()
            else:
                print(
                    f"Node {self.id}: Rejecting vote - already voted for {self.voted_for}"
                )

            response["term"] = self.current_term
            return response

    def send_vote_request(self, target_node):
        """Send a RequestVote RPC to a target node"""
        try:
            with self.lock:
                if self.current_role != "Candidate":
                    return
                request_data = {
                    "term": self.current_term,
                    "candidate_id": self.id,
                    "last_log_index": len(self.log),
                    "last_log_term": self.log[-1]["term"] if self.log else 0,
                }
                request_term = self.current_term

            print(f"Node {self.id}: Sending vote request to {target_node['id']}")
            url = f"http://{target_node['ip']}:{target_node['port']}/request_vote"
            response = requests.put(url, json=request_data, timeout=1.0)

            if response.status_code == 200:
                data = response.json()
                print(f"Node {self.id}: Received vote response: {data}")

                vote_granted = data.get("vote_granted", False)
                response_term = data.get("term", 0)

                with self.lock:
                    # if the term has changed, update our term and step down
                    if response_term > self.current_term:
                        print(
                            f"Node {self.id}: Updating term from {self.current_term} to {response_term}"
                        )
                        self.current_term = response_term
                        self.voted_for = None
                        if self.current_role == "Leader":
                            self.heartbeat_timer.stop_timer()
                        self.current_role = "Follower"
                        self.election_timer.reset_timer()
                        return

                    # if we're still a candidate in the same term
                    if (
                        self.current_role == "Candidate"
                        and request_term == self.current_term
                    ):
                        if vote_granted:
                            self.election_votes[target_node["id"]] = True
                            print(
                                f"Node {self.id}: Vote granted by {target_node['id']}"
                            )
            # we're basically checking if we have won the election after each vote request, and if we have, we become the leader
            self.check_election_win()
        except requests.exceptions.RequestException:
            print(f"Node {self.id}: Error sending vote request to {target_node['id']}")

    ### Append Entries
    def handle_append_entries(self, data):
        """Process an AppendEntries RPC from a leader"""
        with self.lock:
            leader_term = data.get("term", 0)
            leader_id = data.get("leader_id")
            prev_log_index = data.get("prev_log_index", 0)
            prev_log_term = data.get("prev_log_term", 0)
            entries = data.get("entries", [])
            leader_commit = data.get("leader_commit", 0)

            response = {"term": self.current_term, "success": False}

            if leader_term < self.current_term:
                print(
                    f"Node {self.id}: Rejecting append entries - leader term {leader_term} < my term {self.current_term}"
                )
                return response

            # valid heartbeat
            self.election_timer.reset_timer()
            # if leader term is greater, update our term and step down
            if leader_term > self.current_term:
                print(
                    f"Node {self.id}: Updating term from {self.current_term} to {leader_term}"
                )
                self.current_term = leader_term
                self.voted_for = None
            if self.current_role == "Leader":
                self.heartbeat_timer.stop_timer()
            self.current_role = "Follower"

            response["term"] = self.current_term
            response["success"] = True

            return response

    def send_append_entries_to_node(self, target_node, prev_log_index, entries):
        """Send an AppendEntries RPC to a target node"""
        try:
            with self.lock:
                if self.current_role != "Leader":
                    return
                prev_log_term = 0
                if prev_log_index > 0 and prev_log_index <= len(self.log):
                    prev_log_term = self.log[prev_log_index - 1]["term"]

                request_data = {
                    "term": self.current_term,
                    "leader_id": self.id,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": entries,
                    "leader_commit": self.commit_index,
                }
                leader_term = self.current_term

            print(f"Node {self.id}: Sending append entries to {target_node['id']}")
            url = f"http://{target_node['ip']}:{target_node['port']}/append_entries"
            response = requests.put(url, json=request_data, timeout=1.0)

            if response.status_code == 200:
                data = response.json()
                print(f"Node {self.id}: Received append entries response: {data}")
                with self.lock:
                    if data.get("term", 0) > leader_term:
                        print(
                            f"Node {self.id}: Updating term from {self.current_term} to {data['term']}"
                        )
                        self.current_term = data["term"]
                        self.voted_for = None
                        if self.current_role == "Leader":
                            self.heartbeat_timer.stop_timer()
                        self.current_role = "Follower"
                        self.election_timer.reset_timer()
                        return
                    if (
                        data.get("success", False)
                        and self.current_role == "Leader"
                        and leader_term == self.current_term
                    ):
                        if entries:
                            next_index = prev_log_index + len(entries) + 1
                            match_index = prev_log_index + len(entries)
                            self.next_index[target_node["id"]] = next_index
                            self.match_index[target_node["id"]] = match_index
        except requests.exceptions.RequestException:
            print(
                f"Node {self.id}: Error sending append entries to {target_node['id']}"
            )


# RPC Endpoints


app = Flask(__name__)

# Assume a global 'node' object exists (instance of Node)


@app.route("/topic", methods=["PUT"])
def create_topic():
    # only the leader can create a topic
    with node.lock:
        if node.current_role != "Leader":
            return jsonify({"success": False}), 400

    data = request.get_json()
    topic = data.get("topic")
    if not topic:
        return jsonify({"success": False}), 400

    with node.lock:
        if topic in node.topics:
            return jsonify({"success": False}), 409
        node.topics[topic] = []  # Create a new empty message queue for the topic

    print(f"Node {node.id}: Created topic '{topic}'")
    return jsonify({"success": True}), 201


@app.route("/topic", methods=["GET"])
def list_topics():
    with node.lock:
        topics = list(node.topics.keys())
    return jsonify({"success": True, "topics": topics}), 200


@app.route("/message", methods=["PUT"])
def put_message():
    # only the leader can put a message
    with node.lock:
        if node.current_role != "Leader":
            return jsonify({"success": False}), 400
    data = request.get_json()
    topic = data.get("topic")
    message = data.get("message")
    if not topic or not message:
        return jsonify({"success": False}), 400

    with node.lock:
        if topic not in node.topics:
            return jsonify({"success": False}), 404
        node.topics[topic].append(message)

    print(f"Node {node.id}: Added message to topic '{topic}': {message}")
    return jsonify({"success": True}), 201


@app.route("/message/<topic>", methods=["GET"])
def get_message(topic):
    with node.lock:
        # only the leader can get a message
        if node.current_role != "Leader":
            return jsonify({"success": False}), 400
        if topic not in node.topics or len(node.topics[topic]) == 0:
            return jsonify({"success": False}), 404
        # FIFO: pop the first message
        message = node.topics[topic].pop(0)

    print(f"Node {node.id}: Retrieved message from topic '{topic}': {message}")
    return jsonify({"success": True, "message": message}), 200


@app.route("/request_vote", methods=["PUT"])
def handle_request_vote():
    data = request.get_json()
    return jsonify(node.handle_vote_request(data)), 200


@app.route("/append_entries", methods=["PUT"])
def handle_append_entries_request():
    data = request.get_json()
    return jsonify(node.handle_append_entries(data)), 200


@app.route("/status", methods=["GET"])
def get_status():
    with node.lock:
        role = node.current_role
        term = node.current_term
    return jsonify({"role": role, "term": term}), 200


if __name__ == "__main__":
    import sys, json

    if len(sys.argv) != 3:
        sys.exit("Usage: python3 node.py path_to_config index")

    path_to_config = sys.argv[1]
    index = int(sys.argv[2])

    with open(path_to_config, "r") as f:
        config = json.load(f)

    node_info = config["addresses"][index]
    node = Node(
        node_id=index,
        config=config,
        ip=node_info["ip"].replace("http://", ""),
        port=node_info["port"],
    )

    app.run(host=node.ip, port=node.port)
