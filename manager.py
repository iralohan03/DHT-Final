#!/usr/bin/env python3
import socket, threading, json, csv, sys, os, random

MANAGER_HOST = "0.0.0.0"
DEFAULT_MANAGER_PORT = 5000
CSV_FILE = "StormEvents_locations-ftp_v1.0_d2024_c20250317.csv"

class Manager:
    def __init__(self, host=MANAGER_HOST, port=DEFAULT_MANAGER_PORT):
        self.host, self.port = host, port
        self.peers, self.peer_names = {}, {}
        self.next_peer_id = 0
        self.dht_group, self.dht_active, self.dht_leader = [], False, None
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))
        self.events = []
        self.load_csv()
        self.running = True
        self.lock = threading.Lock()
        self.dht_year = None
        self.dht_status = "None"  # None, Setup, Active, Teardown, Rebuild
        print(f"Manager running on {self.host}:{self.port}")

    def load_csv(self):
        if os.path.exists(CSV_FILE):
            try:
                with open(CSV_FILE, newline="", encoding="utf-8") as f:
                    self.events.extend(list(csv.DictReader(f)))
                print(f"Loaded {len(self.events)} events from {CSV_FILE}")
            except Exception as e:
                print(f"Error loading CSV: {e}")
                
    def get_events_by_year(self, year):
        """Return events for a specific year"""
        year_str = str(year)
        return [event for event in self.events if event.get("YEARMONTH", "").startswith(year_str)]

    def run(self):
        threading.Thread(target=self.console, daemon=True).start()
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65535)
                msg = json.loads(data.decode())
                print(f"Received from {addr}: {msg}")
                self.handle(msg, addr)
            except Exception as e:
                print(f"Error in run: {e}")

    def handle(self, m, addr):
        cmd = m.get("command")
        try:
            if cmd == "register":
                self.cmd_register(m, addr)
            elif cmd == "leave-dht":
                self.cmd_leave_dht(m, addr)
            elif cmd == "join-dht":
                self.cmd_join_dht(m, addr)
            elif cmd == "setup-dht":
                self.cmd_setup_dht(m, addr)
            elif cmd == "dht-complete":
                self.cmd_dht_complete(m, addr)
            elif cmd == "query-dht":
                self.cmd_query_dht(m, addr)
            elif cmd == "deregister":
                self.cmd_deregister(m, addr)
            elif cmd == "teardown-dht":
                self.cmd_teardown_dht(m, addr)
            elif cmd == "teardown-complete":
                self.cmd_teardown_complete(m, addr)
            elif cmd == "dht-rebuilt":
                self.cmd_dht_rebuilt(m, addr)
            else:
                print(f"Unknown command: {cmd}")
                self.sock.sendto(json.dumps({"command": f"{cmd}-response", "return_code": "FAILURE", 
                                           "reason": "unknown-command"}).encode(), addr)
                
        except Exception as e:
            print(f"Error handling {cmd}: {e}")
            self.sock.sendto(json.dumps({"command": f"{cmd}-response", "return_code": "FAILURE", 
                                        "reason": f"error: {str(e)}"}).encode(), addr)

    def cmd_register(self, m, addr):
        name = m.get("peer_name")
        p_port = m.get("peer_port")
        
        with self.lock:
            if name in self.peer_names:
                self.sock.sendto(json.dumps({"command": "register-response", "return_code": "FAILURE", 
                                            "reason": "duplicate-name"}).encode(), addr)
                return
                
            pid = self.next_peer_id
            self.next_peer_id += 1
            
            self.peers[pid] = {
                "peer_name": name, 
                "ip": addr[0], 
                "p_port": p_port, 
                "state": "Free"
            }
            self.peer_names[name] = pid
            
        self.sock.sendto(json.dumps({"command": "set_id", "peer_id": pid}).encode(), addr)
        self.update_ring()
        print(f"Registered peer {name} with ID {pid}")

    def update_ring(self):
        with self.lock:
            group = self.dht_group if self.dht_group else list(self.peers.keys())
            
            if not group:
                return
                
            n = len(group)
            for i, pid in enumerate(group):
                if pid not in self.peers:
                    continue
                    
                nxt = self.peers[group[(i + 1) % n]]
                
                msg = {
                    "command": "set_next_peer",
                    "next_peer": [nxt["ip"], nxt["p_port"]]
                }
                self.sock.sendto(json.dumps(msg).encode(), (self.peers[pid]["ip"], self.peers[pid]["p_port"]))

    def cmd_setup_dht(self, m, addr):
        leader_name = m.get("peer_name")
        n = int(m.get("n")) if m.get("n") is not None else 0
        year = m.get("year")
        
        with self.lock:
            if self.dht_status != "None":
                self.sock.sendto(json.dumps({"command": "setup-dht-response", "return_code": "FAILURE", 
                                           "reason": "dht-exists"}).encode(), addr)
                return
                
            if leader_name not in self.peer_names or n < 3 or len(self.peers) < n:
                reason = "invalid-params"
                if leader_name not in self.peer_names:
                    reason = "unknown-peer"
                elif n < 3:
                    reason = "n-too-small"
                elif len(self.peers) < n:
                    reason = "not-enough-peers"
                    
                self.sock.sendto(json.dumps({"command": "setup-dht-response", "return_code": "FAILURE", 
                                            "reason": reason}).encode(), addr)
                return
                
            leader_id = self.peer_names[leader_name]
            
            free = [pid for pid in self.peers if self.peers[pid]["state"] == "Free"]
            
            if leader_id not in free or len(free) < n:
                reason = "not-enough-free"
                self.sock.sendto(json.dumps({"command": "setup-dht-response", "return_code": "FAILURE", 
                                            "reason": reason}).encode(), addr)
                return
                
            free.remove(leader_id)
            
            sel = random.sample(free, n - 1)
            
            self.dht_group = [leader_id] + sel
            
            for pid in self.dht_group:
                self.peers[pid]["state"] = "Leader" if pid == leader_id else "InDHT"
                
            self.dht_leader = leader_id
            self.dht_year = year
            self.dht_status = "Setup"
            
            dht_tuples = [
                [self.peers[pid]["peer_name"], self.peers[pid]["ip"], self.peers[pid]["p_port"]] 
                for pid in self.dht_group
            ]
            
        # Get events for the specified year
        year_events = self.get_events_by_year(year)
        
        self.sock.sendto(json.dumps({
            "command": "setup-dht-response",
            "return_code": "SUCCESS",
            "dht_peers": dht_tuples,
            "year": year,
            "event_count": len(year_events)
        }).encode(), addr)
        
        for i, pid in enumerate(self.dht_group):
            info = self.peers[pid]
            self.sock.sendto(json.dumps({
                "command": "set_id",
                "new_id": i,
                "ring_size": n,
                "dht_peers": dht_tuples,
                "year": year
            }).encode(), (info["ip"], info["p_port"]))
            
        self.update_ring()
        print(f"DHT setup with {n} peers, leader: {leader_name}, year: {year}")

    def cmd_dht_complete(self, m, addr):
        pname = m.get("peer_name")
        
        with self.lock:
            if self.dht_status != "Setup":
                self.sock.sendto(json.dumps({"command": "dht-complete-response", "return_code": "FAILURE", 
                                           "reason": "invalid-state"}).encode(), addr)
                return
                
            if self.peer_names.get(pname) != self.dht_leader:
                self.sock.sendto(json.dumps({"command": "dht-complete-response", "return_code": "FAILURE", 
                                            "reason": "not-leader"}).encode(), addr)
                return
                
            self.dht_active = True
            self.dht_status = "Active"
            
        self.sock.sendto(json.dumps({"command": "dht-complete-response", "return_code": "SUCCESS"}).encode(), addr)
        print("DHT setup completed")

    def cmd_query_dht(self, m, addr):
        pname = m.get("peer_name")
        
        with self.lock:
            if self.dht_status != "Active" or pname not in self.peer_names:
                reason = "dht-inactive" if self.dht_status != "Active" else "unknown-peer"
                self.sock.sendto(json.dumps({"command": "query-dht-response", "return_code": "FAILURE", 
                                            "reason": reason}).encode(), addr)
                return
            
            pid = self.peer_names[pname]
            if pid in self.dht_group and self.peers[pid]["state"] != "Free":
                self.sock.sendto(json.dumps({"command": "query-dht-response", "return_code": "FAILURE", 
                                           "reason": "peer-in-dht"}).encode(), addr)
                return
                
            target = random.choice(self.dht_group)
            tinfo = self.peers[target]
            
            peer_tuple = [tinfo["peer_name"], tinfo["ip"], tinfo["p_port"]]
            
        self.sock.sendto(json.dumps({
            "command": "query-dht-response",
            "return_code": "SUCCESS",
            "peer": peer_tuple
        }).encode(), addr)
        print(f"Query request from {pname}, directed to {tinfo['peer_name']}")

    def cmd_leave_dht(self, m, addr):
        pname = m.get("peer_name")
        
        with self.lock:
            if self.dht_status != "Active":
                self.sock.sendto(json.dumps({"command": "leave-dht-response", "return_code": "FAILURE", 
                                           "reason": "dht-not-active"}).encode(), addr)
                return
                
            if pname not in self.peer_names:
                self.sock.sendto(json.dumps({"command": "leave-dht-response", "return_code": "FAILURE", 
                                            "reason": "unknown-peer"}).encode(), addr)
                return
                
            pid = self.peer_names[pname]
            
            if pid not in self.dht_group:
                self.sock.sendto(json.dumps({"command": "leave-dht-response", "return_code": "FAILURE", 
                                            "reason": "not-in-dht"}).encode(), addr)
                return
                
            # Set status to Rebuild while peer is leaving
            self.dht_status = "Rebuild"
            
        self.sock.sendto(json.dumps({"command": "leave-dht-response", "return_code": "SUCCESS"}).encode(), addr)
        print(f"Peer {pname} leaving DHT")

    def cmd_join_dht(self, m, addr):
        pname = m.get("peer_name")
        
        with self.lock:
            if self.dht_status != "Active":
                self.sock.sendto(json.dumps({"command": "join-dht-response", "return_code": "FAILURE", 
                                           "reason": "dht-not-active"}).encode(), addr)
                return
                
            if pname not in self.peer_names:
                self.sock.sendto(json.dumps({"command": "join-dht-response", "return_code": "FAILURE", 
                                            "reason": "unknown-peer"}).encode(), addr)
                return
                
            pid = self.peer_names[pname]
            
            if self.peers[pid]["state"] != "Free":
                self.sock.sendto(json.dumps({"command": "join-dht-response", "return_code": "FAILURE", 
                                            "reason": "not-free"}).encode(), addr)
                return
                
            # Set status to Rebuild while peer is joining
            self.dht_status = "Rebuild"
            
        self.sock.sendto(json.dumps({"command": "join-dht-response", "return_code": "SUCCESS"}).encode(), addr)
        print(f"Peer {pname} joining DHT")

    def cmd_dht_rebuilt(self, m, addr):
        pname = m.get("peer_name")
        new_leader = m.get("new_leader")
        
        with self.lock:
            if self.dht_status != "Rebuild":
                self.sock.sendto(json.dumps({"command": "dht-rebuilt-response", "return_code": "FAILURE", 
                                           "reason": "invalid-state"}).encode(), addr)
                return
                
            if pname not in self.peer_names:
                self.sock.sendto(json.dumps({"command": "dht-rebuilt-response", "return_code": "FAILURE", 
                                            "reason": "unknown-peer"}).encode(), addr)
                return
            
            # Handle peer leaving
            pid = self.peer_names[pname]
            if pid in self.dht_group:
                self.dht_group.remove(pid)
                self.peers[pid]["state"] = "Free"
            # Handle peer joining
            else:
                self.dht_group.append(pid)
                self.peers[pid]["state"] = "InDHT"
                
            # Update leader if needed
            if new_leader and new_leader in self.peer_names:
                new_leader_id = self.peer_names[new_leader]
                if new_leader_id in self.dht_group:
                    # Update old leader state
                    if self.dht_leader is not None and self.dht_leader in self.dht_group:
                        self.peers[self.dht_leader]["state"] = "InDHT"
                    
                    # Set new leader
                    self.dht_leader = new_leader_id
                    self.peers[new_leader_id]["state"] = "Leader"
            
            # If leader was removed and no new leader specified, choose first peer in group
            if (self.dht_leader not in self.dht_group or self.dht_leader is None) and self.dht_group:
                self.dht_leader = self.dht_group[0]
                self.peers[self.dht_leader]["state"] = "Leader"
                
            self.dht_active = True
            self.dht_status = "Active"
            
        self.sock.sendto(json.dumps({"command": "dht-rebuilt-response", "return_code": "SUCCESS"}).encode(), addr)
        print(f"DHT rebuild completed by {pname}, new leader: {new_leader if new_leader else 'Not specified'}")
        print(f"DHT now has {len(self.dht_group)} peers")

    def cmd_deregister(self, m, addr):
        pname = m.get("peer_name")
        
        with self.lock:
            if pname not in self.peer_names:
                self.sock.sendto(json.dumps({"command": "deregister-response", "return_code": "FAILURE", 
                                            "reason": "unknown-peer"}).encode(), addr)
                return
                
            pid = self.peer_names[pname]
            
            if self.peers[pid]["state"] != "Free":
                self.sock.sendto(json.dumps({"command": "deregister-response", "return_code": "FAILURE", 
                                            "reason": "not-free"}).encode(), addr)
                return
                
            del self.peers[pid]
            del self.peer_names[pname]
            
        self.sock.sendto(json.dumps({"command": "deregister-response", "return_code": "SUCCESS"}).encode(), addr)
        print(f"Peer {pname} deregistered")

    def cmd_teardown_dht(self, m, addr):
        pname = m.get("peer_name")
        
        with self.lock:
            if self.dht_status != "Active":
                self.sock.sendto(json.dumps({"command": "teardown-dht-response", "return_code": "FAILURE", 
                                           "reason": "dht-not-active"}).encode(), addr)
                return
                
            if pname not in self.peer_names or self.peer_names[pname] != self.dht_leader:
                reason = "unknown-peer" if pname not in self.peer_names else "not-leader"
                self.sock.sendto(json.dumps({"command": "teardown-dht-response", "return_code": "FAILURE", 
                                            "reason": reason}).encode(), addr)
                return
                
            self.dht_status = "Teardown"
                
            for pid in self.dht_group:
                info = self.peers[pid]
                self.sock.sendto(json.dumps({"command": "teardown"}).encode(), (info["ip"], info["p_port"]))
            
        self.sock.sendto(json.dumps({"command": "teardown-dht-response", "return_code": "SUCCESS"}).encode(), addr)
        print("DHT teardown initiated")

    def cmd_teardown_complete(self, m, addr):
        pname = m.get("peer_name")
        
        with self.lock:
            if self.dht_status != "Teardown":
                self.sock.sendto(json.dumps({"command": "teardown-complete-response", "return_code": "FAILURE", 
                                           "reason": "invalid-state"}).encode(), addr)
                return
                
            if pname not in self.peer_names or self.peer_names[pname] != self.dht_leader:
                reason = "unknown-peer" if pname not in self.peer_names else "not-leader"
                self.sock.sendto(json.dumps({"command": "teardown-complete-response", "return_code": "FAILURE", 
                                            "reason": reason}).encode(), addr)
                return
                
            for pid in self.dht_group:
                self.peers[pid]["state"] = "Free"
                
            self.dht_group = []
            self.dht_active = False
            self.dht_leader = None
            self.dht_year = None
            self.dht_status = "None"
            
            self.update_ring()
            
        self.sock.sendto(json.dumps({"command": "teardown-complete-response", "return_code": "SUCCESS"}).encode(), addr)
        print("DHT teardown completed")

    def console(self):
        print("Manager console. Type 'help' for commands, 'exit' to quit.")
        while self.running:
            try:
                cmd = input().strip()
                
                if cmd == "exit":
                    print("Shutting down manager...")
                    self.running = False
                    
                elif cmd == "peers":
                    with self.lock:
                        if not self.peers:
                            print("No peers registered")
                        else:
                            print("Registered peers:")
                            for pid, info in self.peers.items():
                                print(f"  {pid}: {info['peer_name']} ({info['ip']}:{info['p_port']}) - {info['state']}")
                                
                elif cmd == "dht":
                    with self.lock:
                        print(f"DHT status: {self.dht_status}")
                        if not self.dht_group:
                            print("No active DHT")
                        else:
                            print(f"DHT active: {self.dht_active}")
                            print(f"DHT year: {self.dht_year}")
                            if self.dht_leader is not None:
                                print(f"DHT leader: {self.peers[self.dht_leader]['peer_name']} (ID {self.dht_leader})")
                            print(f"DHT members ({len(self.dht_group)}):")
                            for pid in self.dht_group:
                                info = self.peers[pid]
                                print(f"  {pid}: {info['peer_name']} ({info['ip']}:{info['p_port']}) - {info['state']}")
                
                elif cmd == "reset":
                    with self.lock:
                        print("Resetting DHT state...")
                        for pid in self.dht_group:
                            self.peers[pid]["state"] = "Free"
                        self.dht_group = []
                        self.dht_active = False
                        self.dht_leader = None
                        self.dht_year = None
                        self.dht_status = "None"
                        print("DHT state reset")
                
                elif cmd == "help":
                    print("Available commands:")
                    print("  peers - Show registered peers")
                    print("  dht - Show DHT information")
                    print("  reset - Reset DHT state (for debugging)")
                    print("  exit - Exit the program")
                    
                else:
                    print(f"Unknown command: {cmd}")
                    print("Type 'help' for available commands")
                    
            except Exception as e:
                print(f"Error in console: {e}")

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_MANAGER_PORT
    Manager(port=port).run()