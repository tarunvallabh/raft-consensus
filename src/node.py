import random
from threading import Timer
import threading
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify
import requests
import os
import json
import time
import signal
import sys
import shutil

node = None
PERSISTENT_STATE_DIR = "../test/node"


def clean_persistent_state():
    if os.path.exists(PERSISTENT_STATE_DIR):
        shutil.rmtree(PERSISTENT_STATE_DIR)
    os.makedirs(PERSISTENT_STATE_DIR, exist_ok=True)


# reference: https://avi.im/blag/2016/sigterm-in-python/
def sigterm_handler(signal, frame):
    print("Received SIGTERM, cleaning up persistent state...")
    clean_persistent_state()
    # handle the backgroud threads
    node.shutdown()
    sys.exit(0)


signal.signal(signal.SIGTERM, sigterm_handler)


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

        # LOCK, allow for re-entrant locks
        self.lock = threading.RLock()

        os.makedirs("node", exist_ok=True)
        # Load persisted state
        self.load_state()
        # Rebuild state from log
        self.rebuild_state()

        self.executor = ThreadPoolExecutor(max_workers=10)

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

    ### Persistent State
    def save_state(self):
        """Save persistent state to disk"""
        with self.lock:
            persistent_state = {
                "currentTerm": self.current_term,
                "votedFor": self.voted_for,
                "log": self.log,
            }

        with open(f"node/{self.id}.json", "w") as f:
            json.dump(persistent_state, f)

    def load_state(self):
        """Load persistent state from disk"""
        try:
            with open(f"node/{self.id}.json", "r") as f:
                persistent_state = json.load(f)

            with self.lock:
                self.current_term = persistent_state["currentTerm"]
                self.voted_for = persistent_state["votedFor"]
                self.log = persistent_state["log"]
        except FileNotFoundError:
            print(f"Node {self.id}: No persisted state found, using defaults")

    def rebuild_state(self):
        """Replay log entries to rebuild the in-memory state (state machine)"""
        # Start with a clean state for topics.
        self.topics = {}
        for entry in self.log:
            command = entry.get("command")
            if command == "ADD_TOPIC":
                topic = entry.get("topic")
                if topic and topic not in self.topics:
                    self.topics[topic] = []
                    print(f"Node {self.id}: Rebuilt topic {topic}")
            elif command == "PUT_MESSAGE":
                topic = entry.get("topic")
                message = entry.get("message")
                if topic:
                    # If the topic doesn't exist, create it.
                    if topic not in self.topics:
                        self.topics[topic] = []
                    self.topics[topic].append(message)
                    print(
                        f"Node {self.id}: Rebuilt message for topic {topic}: {message}"
                    )
            elif command == "GET_MESSAGE":
                topic = entry.get("topic")
                if topic in self.topics and self.topics[topic]:
                    popped = self.topics[topic].pop(0)
                    print(
                        f"Node {self.id}: Replayed GET_MESSAGE for topic {topic}: {popped}"
                    )

    def shutdown(self):
        """Shutdown the node"""
        with self.lock:
            self.heartbeat_timer.stop_timer()
            self.election_timer.stop_timer()
        self.executor.shutdown(wait=False)

    def start_election(self):
        """Start a new election"""
        with self.lock:
            self.current_term += 1
            self.current_role = "Candidate"
            # clear the prior votes as we are starting a new election
            self.election_votes = {self.id: True}
            # save state before the timer resets
            self.save_state()
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
        """Transition to the leader state"""
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
        """Send a heartbeat to all followers"""
        print(f"Node {self.id}: Sending heartbeat for term {self.current_term}")

        # Submit heartbeat tasks for each follower (using self.other_nodes)
        for each_node in self.other_nodes:
            self.executor.submit(self.send_append_entries_to_node, each_node)

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
                self.save_state()
                self.election_timer.reset_timer()

            # If we haven't voted for anyone in this term yet (or already voted for this candidate)
            last_log_index = data.get("last_log_index", 0)
            last_log_term = data.get("last_log_term", 0)
            if (
                self.voted_for is None or self.voted_for == candidate_id
            ) and self.is_candidate_log_up_to_date(last_log_index, last_log_term):
                print(
                    f"Node {self.id}: Granting vote to {candidate_id} for term {candidate_term}"
                )
                self.voted_for = candidate_id
                response["vote_granted"] = True
                self.save_state()

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
                        self.save_state()
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
                self.save_state()
            if self.current_role == "Leader":
                self.heartbeat_timer.stop_timer()
            self.current_role = "Follower"

            # Rule 2: check if we have the log entry at prev_log_index and the term matches
            if prev_log_index > 0:
                if prev_log_index > len(self.log) or (
                    len(self.log) >= prev_log_index
                    and self.log[prev_log_index - 1]["term"] != prev_log_term
                ):
                    print(
                        f"Node {self.id}: Rejecting append entries - log mismatch at index {prev_log_index}"
                    )
                    return {"term": self.current_term, "success": False}

            # Rule 3 and 4: now, we've passed the checks so we can start applying the entry!
            # if the existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
            if len(entries) > 0:
                if len(self.log) > prev_log_index:
                    # truncate the log if our log is longer than the leader's
                    self.log = self.log[:prev_log_index]
                self.log.extend(entries)

                self.save_state()

            # Rule 5: update commit index
            if leader_commit > self.commit_index:
                old_commit_index = self.commit_index
                self.commit_index = min(leader_commit, len(self.log))
                # if the commit index has changed, apply the log entries
                if old_commit_index != self.commit_index:
                    self.apply_committed_entries()

            response["term"] = self.current_term
            response["success"] = True

            return response

    def send_append_entries_to_node(self, target_node):
        """Send an AppendEntries RPC to a target node"""
        try:
            with self.lock:
                if self.current_role != "Leader":
                    return

                follower_id = target_node["id"]
                next_idx = self.next_index.get(follower_id, 1)
                # this is the index of the log entry immediately preceding new entries
                prev_log_index = next_idx - 1

                # get the term:
                prev_log_term = 0
                if prev_log_index > 0 and prev_log_index <= len(self.log):
                    prev_log_term = self.log[prev_log_index - 1]["term"]

                # if we have entries to send, get the entries from the log
                if next_idx <= len(self.log):
                    entries = self.log[next_idx - 1 :]
                else:
                    entries = []

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
                        self.save_state()
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
                            # update commit index. We call this every heartbeat, but it's only applied if the commit index has changed
                            self.update_commit_index()
                    elif (self.current_role == "Leader") and (
                        leader_term == self.current_term
                    ):
                        # decrement the next index and retry
                        # I can just retry the decrementing here maybe?
                        if self.next_index.get(target_node["id"], 1) > 1:
                            self.next_index[target_node["id"]] -= 1

        except requests.exceptions.RequestException:
            print(
                f"Node {self.id}: Error sending append entries to {target_node['id']}"
            )

    def apply_committed_entries(self):
        """Apply all committed entries to the state machine"""
        with self.lock:
            while self.last_applied < self.commit_index:
                # increment last_applied
                self.last_applied += 1
                # get the log entry
                entry = self.log[self.last_applied - 1]
                command = entry["command"]
                print(
                    f"Node {self.id}: Applying command {command} (entry {self.last_applied})"
                )

                # apply the command to the state machine
                if command == "ADD_TOPIC":
                    topic = entry["topic"]
                    if topic not in self.topics:
                        self.topics[topic] = []
                        print(f"Node {self.id}: Added topic {topic}")
                elif command == "PUT_MESSAGE":
                    topic = entry["topic"]
                    message = entry["message"]
                    if topic in self.topics:
                        self.topics[topic].append(message)
                        print(
                            f"Node {self.id}: Added message to topic {topic}: {message}"
                        )
                elif command == "GET_MESSAGE":
                    topic = entry["topic"]
                    if topic in self.topics and len(self.topics[topic]) > 0:
                        message = self.topics[topic].pop(0)
                        print(
                            f"Node {self.id}: Retrieved message from topic {topic}: {message}"
                        )
                elif command == "GET_TOPICS":
                    print(f"Node {self.id}: Read topics list")
                    pass
                # if its not changing the state, we don't need to do anything but we should still log it to ensure ordering

    def update_commit_index(self):
        """Update commit index if there exists an N > commitIndex such that
        a majority of matchIndex[i] ≥ N and log[N].term == currentTerm"""
        with self.lock:
            if self.current_role != "Leader":
                return

            # Check each index from current commit_index+1 to end of log
            old_commit_index = self.commit_index
            for N in range(self.commit_index + 1, len(self.log) + 1):
                # Only update commit_index for entries from current term
                if self.log[N - 1]["term"] == self.current_term:
                    # Count how many servers have this entry (including self)
                    replicated_count = 1  # Count self
                    for each_follower in self.match_index:
                        if self.match_index[each_follower] >= N:
                            replicated_count += 1

                    # If majority have this entry, update commit_index
                    if replicated_count > (self.total_nodes // 2):
                        self.commit_index = N
                    else:
                        # If we don't have majority for this entry, we won't for later ones either
                        break

            # If commit index changed, apply entries
            if self.commit_index > old_commit_index:
                print(
                    f"Node {self.id}: Commit index updated from {old_commit_index} to {self.commit_index}"
                )
                self.apply_committed_entries()

    def is_candidate_log_up_to_date(self, last_log_index, last_log_term):
        """Check if candidate's log is at least as up-to-date as receiver's log"""
        my_last_log_term = 0
        my_last_log_index = len(self.log)
        if self.log:
            my_last_log_term = self.log[-1]["term"]

        # If the terms are different, the one with the larger term is more up-to-date
        if last_log_term > my_last_log_term:
            return True
        elif last_log_term == my_last_log_term:
            # If terms are the same, the one with the longer log is more up-to-date
            return last_log_index >= my_last_log_index
        return False

    def wait_for_log_commit(self, log_index, timeout=1.0):
        """Wait until the specified log entry has been committed and applied"""
        majority = (self.total_nodes // 2) + 1
        start_time = time.time()

        while time.time() - start_time < timeout:
            with self.lock:
                # Check if already committed or can be committed
                if self.commit_index >= log_index:
                    return True

                confirmed_count = 1  # Leader counts as confirmed
                for follower_id in self.match_index:
                    if self.match_index[follower_id] >= log_index:
                        confirmed_count += 1

                if confirmed_count >= majority:
                    self.commit_index = log_index
                    self.apply_committed_entries()
                    return True

            time.sleep(0.005)

        return False  # Timeout occurred


# RPC Endpoints


app = Flask(__name__)


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
        log_index = len(node.log) + 1
        node.log.append(
            {"term": node.current_term, "command": "ADD_TOPIC", "topic": topic}
        )
        node.save_state()

    node.send_heartbeat()
    # update commit index
    if not node.wait_for_log_commit(log_index):
        # If timeout occurred, return failure
        return jsonify({"success": False}), 500

    print(f"Node {node.id}: Created topic '{topic}'")
    return jsonify({"success": True}), 201


@app.route("/topic", methods=["GET"])
def list_topics():
    with node.lock:
        if node.current_role != "Leader":
            return jsonify({"success": False}), 400

        # Just send heartbeats to verify leadership
        node.send_heartbeat()

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
        log_index = len(node.log) + 1
        node.log.append(
            {
                "command": "PUT_MESSAGE",
                "topic": topic,
                "message": message,
                "term": node.current_term,
            }
        )
        node.save_state()

    # Trigger replication
    node.send_heartbeat()

    # Update commit index based on replication status
    if not node.wait_for_log_commit(log_index):
        return jsonify({"success": False}), 500

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

        # Save the message to return before it gets popped
        message = node.topics[topic][0]

        # add to log, so we can pop later
        log_index = len(node.log) + 1
        node.log.append(
            {
                "command": "GET_MESSAGE",
                "topic": topic,
                "term": node.current_term,
            }
        )
        node.save_state()

    # Trigger replication
    node.send_heartbeat()
    # Update commit index based on replication status
    if not node.wait_for_log_commit(log_index):
        return jsonify({"success": False}), 500

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
