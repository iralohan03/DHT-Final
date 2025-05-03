#!/usr/bin/env python3
import socket, threading, json, sys, random, os, csv, time

def is_prime(n):
    """Check if a number is prime."""
    if n <= 1:
        return False
    if n <= 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True

def next_prime(n):
    """Find the smallest prime number greater than n."""
    if n <= 1:
        return 2
    prime = n
    found = False
    while not found:
        prime += 1
        if is_prime(prime):
            found = True
    return prime

class Peer:
    def __init__(self, mgr_ip, mgr_port):
        self.mgr = (mgr_ip, mgr_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", 0))
        self.port = self.sock.getsockname()[1]
        self.peer_id = None
        self.name = None
        self.next_peer = None
        self.data_store = {}
        self.ring_id = None
        self.ring_size = None
        self.dht_peers = []
        self.running = True
        self.year = None
        self.hash_table_size = None
        self.records_stored = 0
        self.pending_acks = 0
        self.is_leader = False
        self.join_pending = False
        self.leave_pending = False
        print(f"Peer on port {self.port}")

    def send(self, obj, addr):
        """Send a JSON message to the specified address."""
        print(f"Sending to {addr}: {obj}")
        self.sock.sendto(json.dumps(obj).encode(), addr)

    def register(self):
        """Register this peer with the manager."""
        self.name = input("peer name: ").strip()[:15]
        self.send({"command": "register", "peer_name": self.name, "peer_port": self.port}, self.mgr)
        data, addr = self.sock.recvfrom(65535)
        resp = json.loads(data.decode())
        if resp.get("command") == "set_id":
            self.peer_id = resp["peer_id"]
            print(f"Registered with ID {self.peer_id}")
        else:
            print(f"Registration failed: {resp}")

    def listen(self):
        """Listen for incoming messages and handle them."""
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65535)
                msg = json.loads(data.decode())
                print(f"Received from {addr}: {msg}")
                self.handle(msg, addr)
            except Exception as e:
                print(f"Error in listen: {e}")

    def handle(self, m, addr):
        """Handle incoming messages based on the command."""
        c = m.get("command")
        try:
            if c == "set_next_peer":
                self.next_peer = tuple(m["next_peer"])
                print(f"Next peer set to {self.next_peer}")
            
            elif c == "set_id":
                if "new_id" in m:
                    self.ring_id = m["new_id"]
                    self.ring_size = m["ring_size"]
                    self.dht_peers = m["dht_peers"]
                    self.is_leader = (self.ring_id == 0)
                    
                    if "year" in m:
                        self.year = m["year"]
                    
                    if self.ring_id < len(self.dht_peers) - 1:
                        nxt = self.dht_peers[(self.ring_id + 1) % self.ring_size]
                        self.next_peer = (nxt[1], int(nxt[2]))
                    print(f"Set as DHT member with ID {self.ring_id} in ring of size {self.ring_size}")
                    
                    # If this is the leader (ID 0), auto-start DHT construction
                    if self.is_leader:
                        # Initialize DHT construction
                        print("Starting DHT construction as leader")
                        threading.Thread(target=self.start_dht_construction, daemon=True).start()
                else:
                    self.peer_id = m["peer_id"]
                    print(f"Assigned ID {self.peer_id} by manager")
            
            elif c == "store":
                # Store record in local hash table
                event_id = m["event_id"]
                self.data_store[event_id] = m["event_data"]
                self.records_stored += 1
                print(f"Stored event {event_id} in local hash table")
                
                # Send acknowledgment
                if "ack_addr" in m:
                    ack_addr = tuple(m["ack_addr"])
                    self.send({"command": "store_ack", "event_id": event_id}, ack_addr)
            
            elif c == "forward_store":
                # Forward store message around the ring
                event_id = m["event_id"]
                event_data = m["event_data"]
                target_id = m["target_id"]
                
                # If this peer is the target, store locally
                if self.ring_id == target_id:
                    self.data_store[event_id] = event_data
                    self.records_stored += 1
                    print(f"Stored forwarded event {event_id} in local hash table")
                    
                    # Send acknowledgment back to leader
                    if "leader_addr" in m:
                        leader_addr = tuple(m["leader_addr"])
                        self.send({"command": "store_ack", "event_id": event_id}, leader_addr)
                else:
                    # Otherwise, forward to next peer in ring
                    print(f"Forwarding store of event {event_id} to next peer {self.next_peer}")
                    self.send(m, self.next_peer)
            
            elif c == "store_ack":
                # Record acknowledgment received for storing data
                self.pending_acks -= 1
                print(f"Received store acknowledgment. Pending acks: {self.pending_acks}")
                
                if self.pending_acks <= 0 and self.is_leader:
                    print("All data storage acknowledgments received")
                    
                    # Print distribution report
                    print("DHT Construction Complete")
                    print(f"Records stored at this node: {self.records_stored}")
                    
                    # Notify manager
                    self.send({"command": "dht-complete", "peer_name": self.name}, self.mgr)
            
            elif c == "find_event":
                self.find_event(m, addr)
            
            elif c == "found_event":
                if m["event_data"] is None:
                    print(f"Event {m['event_id']} not found in the DHT")
                else:
                    print(f"Found event {m['event_id']}:")
                    self.display_event(m['event_data'])
                    print(f"Path taken: {m['id_seq']}")
            
            elif c == "query-dht-response":
                self.process_query_response(m)
            
            elif c == "leave-dht-response":
                print(f"Leave DHT response: {m['return_code']}")
                if m["return_code"] == "SUCCESS":
                    self.leave_pending = True
                    # Initiate teardown of current DHT
                    if self.next_peer:
                        self.send({
                            "command": "teardown", 
                            "initiator_id": self.ring_id, 
                            "origin_addr": (socket.gethostbyname(socket.gethostname()), self.port)
                        }, self.next_peer)
            
            elif c == "join-dht-response":
                print(f"Join DHT response: {m['return_code']}")
                if m["return_code"] == "SUCCESS":
                    self.join_pending = True
                    # Leader will handle adding this peer to the DHT
                    # Find the leader from the response
                    self.send({"command": "join_request", "peer_name": self.name}, 
                              (self.mgr[0], self.mgr[1]))
            
            elif c == "join_request":
                # Handle request from a peer wanting to join the DHT
                if self.is_leader:
                    joining_peer = m.get("peer_name")
                    print(f"Received join request from {joining_peer}")
                    
                    # Add the peer to the DHT and reconfigure ring
                    self.add_peer_to_dht(joining_peer)
            
            elif c == "teardown":
                # Handle teardown message
                initiator_id = m.get("initiator_id")
                origin_addr = tuple(m.get("origin_addr", (None, None)))
                
                # Clear local data store
                self.data_store.clear()
                self.records_stored = 0
                print("Cleared local hash table")
                
                # If we're the initiator, complete teardown
                if self.ring_id == initiator_id:
                    print("Teardown cycle completed")
                    if origin_addr != (None, None):
                        self.send({"command": "teardown_complete"}, origin_addr)
                    
                    # If this was initiated by a leave-dht, handle the leave
                    if self.leave_pending:
                        self.handle_leave_dht()
                else:
                    # Forward to next peer
                    self.send(m, self.next_peer)
            
            elif c == "reset_id":
                # Handle ID reset during leave/join operations
                old_id = self.ring_id
                self.ring_id = m["new_id"]
                self.ring_size = m["ring_size"]
                self.dht_peers = m["dht_peers"]
                
                # Check if this peer is now the leader
                self.is_leader = (self.ring_id == 0)
                
                # Update next peer
                if len(self.dht_peers) > 0:
                    nxt = self.dht_peers[(self.ring_id + 1) % self.ring_size]
                    self.next_peer = (nxt[1], int(nxt[2]))
                
                print(f"Reset ID from {old_id} to {self.ring_id} in ring of size {self.ring_size}")
                
                # Forward reset_id if not the last peer
                initiator_id = m.get("initiator_id")
                if self.ring_id != initiator_id:
                    self.send(m, self.next_peer)
                else:
                    # If we're back to the initiator, start rebuild
                    print("Reset ID propagation completed")
                    # If this is a join operation, rebuild the DHT
                    if "rebuild_mode" in m and m["rebuild_mode"] == "join":
                        print("Starting DHT rebuild after join")
                        self.send({"command": "rebuild-dht"}, self.next_peer)
            
            elif c == "rebuild-dht":
                # Leader rebuilds the DHT after a peer joins/leaves
                if self.is_leader:  # Only leader should handle this
                    print("Starting DHT rebuild as the new leader")
                    threading.Thread(target=self.construct_dht, daemon=True).start()
                else:
                    # Forward to next peer if not leader
                    self.send(m, self.next_peer)
            
            elif c == "dht-complete-response":
                print(f"DHT complete response: {m['return_code']}")
            
            elif c == "teardown_complete":
                # Initiator received confirmation of teardown completion
                print("Teardown completed, informing manager")
                
                # If this was a teardown-dht command, inform manager
                if not self.leave_pending and not self.join_pending:
                    self.send({"command": "teardown-complete", "peer_name": self.name}, self.mgr)
            
            elif c == "teardown-dht-response":
                print(f"Teardown DHT response: {m['return_code']}")
                if m["return_code"] == "SUCCESS":
                    # Initiate teardown
                    self.send({
                        "command": "teardown", 
                        "initiator_id": self.ring_id
                    }, self.next_peer)
            
            elif c == "teardown-complete-response":
                print(f"Teardown complete response: {m['return_code']}")
            
            elif c == "dht-rebuilt-response":
                print(f"DHT rebuilt response: {m['return_code']}")
                
                # Reset states
                self.leave_pending = False
                self.join_pending = False
        
        except Exception as e:
            print(f"Error handling message {c}: {e}")

    def display_event(self, event_data):
        """Display the event data in a readable format."""
        event_fields = [
            "event_id", "state", "year", "month_name", "event_type", "cz_type", 
            "cz_name", "injuries_direct", "injuries_indirect", "deaths_direct", 
            "deaths_indirect", "damage_property", "damage_crops", "tor_f_scale"
        ]
        for i, field in enumerate(event_fields):
            if i < len(event_data):
                print(f"  {field}: {event_data[i]}")

    def find_event(self, m, origin):
        """Implement hot potato routing to find an event in the DHT."""
        event_id = m["event_id"]
        id_seq = m.get("id_seq", [])
        
        # Add this node to the sequence if not already present
        if self.ring_id not in id_seq:
            id_seq.append(self.ring_id)
        
        # Check if we have the event
        if int(event_id) in self.data_store:
            print(f"Found event {event_id} in local hash table")
            response = {
                "command": "found_event",
                "event_id": event_id,
                "event_data": self.data_store[int(event_id)],
                "id_seq": id_seq
            }
            self.send(response, origin)
            return
        
        # If we've visited all nodes, report failure
        if len(id_seq) >= self.ring_size:
            print(f"Event {event_id} not found after visiting all nodes")
            response = {
                "command": "found_event",
                "event_id": event_id,
                "event_data": None,
                "id_seq": id_seq
            }
            self.send(response, origin)
            return
        
        # Randomly select next node to visit from those not yet visited
        unvisited = [i for i in range(self.ring_size) if i not in id_seq]
        if not unvisited:
            # We've checked all nodes
            response = {
                "command": "found_event",
                "event_id": event_id,
                "event_data": None,
                "id_seq": id_seq
            }
            self.send(response, origin)
            return
            
        next_id = random.choice(unvisited)
        
        # Forward query to next node
        next_peer = self.dht_peers[next_id]
        next_addr = (next_peer[1], int(next_peer[2]))
        
        print(f"Forwarding find_event {event_id} to peer {next_id} at {next_addr}")
        forward_msg = {
            "command": "find_event",
            "event_id": event_id,
            "id_seq": id_seq
        }
        self.send(forward_msg, next_addr)

    def process_query_response(self, m):
        """Process response from manager for query-dht command."""
        if m["return_code"] != "SUCCESS":
            print("Query failed: DHT may not be active")
            return
        
        # Extract target peer info
        peer_info = m["peer"]
        target_addr = (peer_info[1], int(peer_info[2]))
        
        # Prompt for event ID to query
        event_id = input("event id to query: ").strip()
        try:
            event_id = int(event_id)
            
            # Prepare query message
            query_msg = {
                "command": "find_event",
                "event_id": event_id,
                "id_seq": []
            }
            
            # Send query to target peer
            print(f"Sending query for event {event_id} to {target_addr}")
            self.send(query_msg, target_addr)
            
        except ValueError:
            print("Event ID must be an integer")

    def handle_leave_dht(self):
        """Handle the process of leaving the DHT."""
        print("Initiating DHT reconfiguration after leaving")
        
        # Send reset_id command to the next peer to start renumbering
        new_dht_peers = [p for p in self.dht_peers if p[0] != self.name]
        reset_msg = {
            "command": "reset_id",
            "new_id": 0,  # Start with 0 for the next peer
            "ring_size": self.ring_size - 1,
            "dht_peers": new_dht_peers,
            "initiator_id": self.ring_id,
            "rebuild_mode": "leave"
        }
        
        # Remember which peer is becoming the new leader
        new_leader = self.dht_peers[(self.ring_id + 1) % self.ring_size][0]
        
        # Send reset_id to next peer
        self.send(reset_msg, self.next_peer)
        
        # Wait a bit for propagation to complete
        time.sleep(3)
        
        # Notify manager that rebuild is complete
        self.send({
            "command": "dht-rebuilt", 
            "peer_name": self.name, 
            "new_leader": new_leader
        }, self.mgr)
        
        # Clear DHT state
        self.ring_id = None
        self.ring_size = None
        self.next_peer = None
        self.dht_peers = []
        self.is_leader = False

    def add_peer_to_dht(self, peer_name):
        """Add a new peer to the DHT."""
        print(f"Adding peer {peer_name} to DHT")
        
        # Find peer in manager's list
        new_peer = None
        for peer in self.dht_peers:
            if peer[0] == peer_name:
                new_peer = peer
                break
                
        if not new_peer:
            print(f"Could not find peer {peer_name} in manager's list")
            return
            
        # Add peer to DHT
        new_dht_peers = self.dht_peers + [new_peer]
        
        # Send reset_id command to start renumbering
        reset_msg = {
            "command": "reset_id",
            "new_id": 0,  # Leader keeps ID 0
            "ring_size": self.ring_size + 1,
            "dht_peers": new_dht_peers,
            "initiator_id": 0,  # Leader is initiator
            "rebuild_mode": "join"
        }
        
        # Send reset_id to next peer
        self.send(reset_msg, self.next_peer)
        
        # Wait a bit for propagation to complete
        time.sleep(2)
        
        # Notify manager that rebuild is complete
        self.send({
            "command": "dht-rebuilt", 
            "peer_name": peer_name, 
            "new_leader": self.name
        }, self.mgr)
        
    def start_dht_construction(self):
        """Start the DHT construction process as leader."""
        try:
            print("Leader starting DHT construction")
            
            # For milestone, we'll use synthetic data
            # In a real implementation, you would read the CSV file
            self.construct_dht()
            
        except Exception as e:
            print(f"Error in DHT construction: {e}")

    def construct_dht(self):
        """Construct the DHT by distributing data across peers."""
        try:
            print("Constructing DHT with data distribution")
            
            # Set hash table size
            # In a real implementation, you would count records from the CSV file
            record_count = 50  # Synthetic data size
            self.hash_table_size = next_prime(2 * record_count)
            print(f"Hash table size set to {self.hash_table_size}")
            
            # Sample records for demonstration
            # In a real implementation, you would read real records from CSV
            event_counts = [0] * self.ring_size
            self.pending_acks = 0
            
            for i in range(record_count):
                event_id = 5000000 + i
                
                # Hash function 1: compute position in hash table
                pos = event_id % self.hash_table_size
                
                # Hash function 2: determine which peer stores the record
                target_id = pos % self.ring_size
                event_counts[target_id] += 1
                
                # Sample event data (in a real implementation, this would be from CSV)
                event_data = [
                    event_id, "STATE", str(self.year), "January", 
                    "Tornado", "C", "COUNTY", "0", "0", "0", "0", 
                    "0", "0", ""
                ]
                
                # If target is self, store locally
                if target_id == self.ring_id:
                    self.data_store[event_id] = event_data
                    self.records_stored += 1
                else:
                    # Otherwise, send to target peer using ring topology
                    # For record keeping, increment pending acks
                    self.pending_acks += 1
                    
                    # Forward store command through the ring to target
                    forward_msg = {
                        "command": "forward_store",
                        "event_id": event_id,
                        "event_data": event_data,
                        "target_id": target_id,
                        "leader_addr": [socket.gethostbyname(socket.gethostname()), self.port]
                    }
                    
                    # Send to next peer in ring
                    self.send(forward_msg, self.next_peer)
            
            # Print expected distribution
            print("Expected record distribution:")
            for i, count in enumerate(event_counts):
                print(f"  Node {i}: {count} records")
            
            # If there are no pending acknowledgments, report completion
            if self.pending_acks == 0:
                print("DHT Construction Complete - No distributed records")
                print(f"Records stored at this node: {self.records_stored}")
                self.send({"command": "dht-complete", "peer_name": self.name}, self.mgr)
            
        except Exception as e:
            print(f"Error in DHT construction: {e}")

    def cli(self):
        """Command-line interface for the peer."""
        print("Commands:")
        print("  setup-dht <n> <year> - Set up a DHT with n peers for storm data from year")
        print("  dht-complete - Signal completion of DHT setup")
        print("  query <event_id> - Query local DHT for an event")
        print("  query-dht - Query the DHT via manager")
        print("  leave-dht - Leave the DHT")
        print("  join-dht - Join the DHT")
        print("  teardown-dht - Tear down the DHT (leader only)")
        print("  exit - Exit the peer")
        
        while self.running:
            try:
                line = input("> ").strip()
                
                if line.startswith("setup-dht"):
                    try:
                        parts = line.split()
                        if len(parts) != 3:
                            print("Usage: setup-dht <n> <year>")
                            continue
                            
                        _, n, year = parts
                        n, year = int(n), int(year)
                        self.year = year
                        self.send({"command": "setup-dht", "peer_name": self.name, "n": n, "year": year}, self.mgr)
                    except ValueError:
                        print("Usage: setup-dht <n> <year> - n and year must be integers")
                
                elif line == "dht-complete":
                    self.send({"command": "dht-complete", "peer_name": self.name}, self.mgr)
                
                elif line.startswith("query"):
                    try:
                        parts = line.split()
                        if len(parts) != 2:
                            print("Usage: query <event_id>")
                            continue
                            
                        _, eid = parts
                        eid = int(eid)
                        # Query local DHT
                        self.find_event({"event_id": eid, "id_seq": []}, (socket.gethostbyname(socket.gethostname()), self.port))
                    except ValueError:
                        print("Usage: query <event_id> - event_id must be an integer")
                
                elif line == "query-dht":
                    # Query via manager
                    self.send({"command": "query-dht", "peer_name": self.name}, self.mgr)
                
                elif line == "leave-dht":
                    self.send({"command": "leave-dht", "peer_name": self.name}, self.mgr)
                
                elif line == "join-dht":
                    self.send({"command": "join-dht", "peer_name": self.name}, self.mgr)
                
                elif line == "teardown-dht":
                    self.send({"command": "teardown-dht", "peer_name": self.name}, self.mgr)
                
                elif line == "status":
                    self.print_status()
                
                elif line == "help":
                    print("Commands:")
                    print("  setup-dht <n> <year> - Set up a DHT with n peers for storm data from year")
                    print("  dht-complete - Signal completion of DHT setup")
                    print("  query <event_id> - Query local DHT for an event")
                    print("  query-dht - Query the DHT via manager")
                    print("  leave-dht - Leave the DHT")
                    print("  join-dht - Join the DHT")
                    print("  teardown-dht - Tear down the DHT (leader only)")
                    print("  status - Show peer status")
                    print("  exit - Exit the peer")
                
                elif line == "exit":
                    # Try to deregister first
                    if self.ring_id is not None:
                        print("Cannot exit while part of a DHT. Leave the DHT first.")
                        continue
                        
                    self.send({"command": "deregister", "peer_name": self.name}, self.mgr)
                    data, addr = self.sock.recvfrom(65535)
                    resp = json.loads(data.decode())
                    if resp.get("return_code") == "SUCCESS":
                        print("Successfully deregistered")
                        self.running = False
                        print("Exiting...")
                    else:
                        print(f"Deregistration failed: {resp}")
                
                else:
                    print("Unknown command. Type 'help' for available commands.")
            
            except Exception as e:
                print(f"Error in CLI: {e}")

    def print_status(self):
        """Print the current status of the peer."""
        print(f"Peer name: {self.name}")
        print(f"Peer ID: {self.peer_id}")
        print(f"Ring ID: {self.ring_id}")
        print(f"Ring size: {self.ring_size}")
        print(f"Is leader: {self.is_leader}")
        print(f"Records stored: {self.records_stored}")
        print(f"Pending acks: {self.pending_acks}")
        print(f"Next peer: {self.next_peer}")
        print(f"Join pending: {self.join_pending}")
        print(f"Leave pending: {self.leave_pending}")
                
    def start(self):
        """Start the peer process."""
        self.register()
        threading.Thread(target=self.listen, daemon=True).start()
        self.cli()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: peer.py <mgr_ip> <mgr_port>")
    Peer(sys.argv[1], int(sys.argv[2])).start()