import os, sys, argparse,random, time, threading, json
class Nodes:
    def __init__(self, storage_path :str, node_name :str, id :int):
        # NOTE: Indexing for logs start from 1, 0 is reserved for no logs or NULL
        # TODO: Figure out how to handle logs (format of log for easy read and write)
        peer_path = "peer.json"

        metadata_template = {
            "currentTerm": 0,
            "votedFor"   :-1,
        }

        self.STATES = {
            "leader"   : 0,
            "candidate": 1,
            "follower" : 2
        }

        self.ID = id
        self.current_state = self.STATES["follower"]
        self.file_path = None
        self.meta_data = None
        self.log_fd    = None
        self.dump_fd   = None
        self.logs   = None
        self.dump   = None
        self.peers  = None
        self.socket = None

        # volatile state for node
        self.commit_index = 0
        self.last_applied = 0
        self.lease_time_leader = 0

        # volatile state for leader
        self.next_index  = dict() # next entry to sent to servers
        self.match_index = dict() # last entry replicated by server
        self.lease_timer = 10000 #ms

        self.create_dir()
        self.create_metadata()
        self.create_logs()
        self.create_dumps()
        self.load_peers()

    def start_server(self):
        # TODO :add listeners for vote, heartbeat etc.
        self.follower_timeout_extension = threading.Event()
        self.follower_timeout_thread = threading.Thread(target=self.time_out_callback)
    def time_out_callback(self):
        while True:
            while self.follower_timeout_extension.wait(random.random()*5 + 5):
                print("recieved heartbeat")
                self.follower_timeout_extension.clear()

            print("follower timeout changing to candidate")
            result = self.change_to_candidate()
            if(result == False):
                continue
            else:
                print("something wrong happened")
                break


    def change_to_candidate(self):
        self.current_state = self.STATES["candidate"]
        print("become a candidate starting election")
        results  = self.start_election()
        if(results):
            return self.become_leader()
        else:
            return False
    
    def start_election(self):
        f = open(self.meta_data,"r")
        x = json.load(f)
        f.close()

        x["current_term"] += 1
        x["votedfor"] = self.ID

        f = open(self.meta_data,"w")
        json.dump(x,f,indent=4)
        f.close()
        votes = self.ask_for_votes(x["current_term"])
        if votes < (len(self.peers.keys()) + 1)/2:
            return False
        else:
            return True

    def ask_for_votes(self,term):
        votes = 1
        for i in self.peers.keys():
            if self.send_vote_request(self.ID,self.peers[i],term):
                votes += 1
        return votes
    
    def send_vote_request(self,candidate_id,voters_socket,term): 
        # TODO: write send_vote_request and recieve vote request stubs
        return False
    
    def become_leader(self):
        # TODO: what happens after we become leader?
        return

    def create_dir(self,d_path: str):
        if not os.path.exists(d_path):
            os.mkdir(d_path)
            print(f"Directory '{d_path}' created successfully.")
        else:
            print(f"Directory '{d_path}' already exists.")
        return 
    
    def create_metadata(self,m_path: str):
        self.meta_data = os.path.join(self.file_path,m_path)
        if not os.path.isfile(self.meta_data):
            f = open(self.meta_data ,"w")
            json.dump(self.meta_data,f,indent=4)
            f.close()
        else:
            print(f"Metadata found at {self.meta_data}")
        return
    
    def create_logs(self,l_path: str):
        self.logs = os.path.join(self.file_path,l_path)
        if not os.path.isfile(self.logs):
            self.log_fd = open(self.logs ,"w+")
        else:
            self.log_fd = open(self.logs, "r+")
            print(f"logs found at {self.logs}")
    
    def create_dumps(self,d_path: str):
        self.dump = os.path.join(self.file_path,d_path)
        self.dump_fd = open(d_path,"a+")
    
    def load_peers(self,p_path: str):
        try:
            f = open(p_path,"r")
            self.peers = json.load(f)
            f.close()
        except FileNotFoundError:
            raise FileNotFoundError("no list of peers given")
        
        try:
            self.socket = self.peers[self.ID]
        except:
            raise IndexError("could not file ID in peers")
        
        self.peers.pop(self.ID)

        for index in self.peers.keys():
            self.next_index[index]  = self.get_last_log_index() + 1
            self.match_index[index] = 0
        return
    
    def get_last_log_index(self) -> int:
        log_len = sum(1 for line in self.log_fd)
        self.log_fd.seek(0)
        return len(log_len)
    
    def append_log_entry(self,value: dict) -> int:
        if value["type"] == "noop":
            self.log_fd.seek(0,2)
            ret = self.log_fd.write(f"NO-OP {value["term"]}")
            self.log_fd.flush()
        else:
            self.log_fd.seek(0,2)
            ret = self.log_fd.write(f"SET {value["key"]} {value["value"]} {value["term"]}")
            self.log_fd.flush()
        self.log_fd.seek(0)
        return ret




