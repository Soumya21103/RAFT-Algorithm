import os, asyncio
import threading, json, math, random
import Node_pb2_grpc as ngpc
from Node_pb2_grpc import NodeStub
from concurrent import futures
import client_pb2_grpc as cgpc
import Node_pb2 as gnd
import client_pb2 as cl
import grpc
import sys
import time

'''
TODO: Stuff remaining to do
1. implement Client DONE
2. Debug connections DONE
3. TODO: add code for dump
5.  and log  DONE
4. implement leader lease DONE
6. code for recovery #done

NOTE:
1. Didn't implement election timer properly
'''
STATES = {
    "fol": 0,
    "can": 1,
    "lea": 2
}

class ClientServiser(cgpc.clientServicer):
    def __init__(self,pnode) -> None:
        super().__init__()
        self.pnode: Node = pnode

    
    def ServeClient(self, request: cl.ServeClientArgs, context):
        if self.pnode.state == STATES["lea"]:
            s = request.Request.split()
            self.pnode.dump(f"Node {self.pnode.ID} (leader) received an \"{s}\" request.")

            if s[0] == "set":
                with self.pnode.m_lock:
                    f = open(self.pnode.m_path,"r")
                    current_term = int(f.readline().split()[-1])
                    f.close()
                    pass
                with self.pnode.l_lock:
                    f = open(self.pnode.l_path,"a")
                    f.write(f"SET {s[1]} {s[2]} {current_term}\n")
                    f.close()
                    self.pnode.acked_length[self.pnode.ID], _ = self.pnode.get_log_len_and_term()
                    pass
            elif s[0] == "get":
                with self.pnode.l_lock:
                    res = self.pnode.get_latest_key_value(s[1])
                    pass
                return cl.ServeClientReply(Data=res,LeaderID=str(self.pnode.ID),Success=True)
            return cl.ServeClientReply(Data=None,LeaderID=str(self.pnode.ID),Success=True)
        return cl.ServeClientReply(Data=None,LeaderID=str(self.pnode.current_leader),Success=False)
    
class NodeServicer(ngpc.NodeServicer):
    def __init__(self,parent_node) -> None:
        super().__init__()
        self.pnode: Node = parent_node
    
    def requestLog(self, request: gnd.logRequest, context):
        #TODO: Something about election timer seems odd
        # print("DEBUG:",request.suffix)
        self.pnode.set_lease_time(request.leader_lease)
        self.pnode.got_replicate_req.set()
        with self.pnode.m_lock:
            f = open(self.pnode.m_path,"r")
            current_term = int(f.readline().split()[-1])
            voted_for = int(f.readline().split()[-1])
            f.close()
            pass

        with self.pnode.l_lock:
            l_len, _ = self.pnode.get_log_len_and_term()
            lp_term = self.pnode.get_log_and_term_at_ind(request.pref_len - 1)
            pass
        
        if request.c_term > current_term:
            current_term = request.c_term
            voted_for = -1

        if request.c_term == current_term:
            self.pnode.state = STATES["fol"]
            self.pnode.current_leader = request.l_id
        
        log_ok = (l_len >= request.pref_len) and (request.pref_len == 0 or lp_term == request.pref_term)
        if request.c_term == current_term and log_ok:
            self.pnode.append_entries(request.pref_len,request.l_commit,list(request.suffix))
            ack = request.pref_len + len(request.suffix)
            with self.pnode.m_lock:
                f = open(self.pnode.m_path,"w")
                f.writelines([f"current_term {current_term}\n",f"voted_for {voted_for}\n"])
                f.close()
                pass
            with self.pnode.d_lock:
                pass
            self.pnode.dump(f"NODE {self.pnode.ID}: request log success from leader {request.l_id}")
            return gnd.logResponse(f_id=self.pnode.ID,term=current_term,ack=ack,sucess=True)
        else:
            with self.pnode.m_lock:
                f = open(self.pnode.m_path,"w")
                f.writelines([f"current_term {current_term}\n",f"voted_for {voted_for}\n"])
                f.close()
                pass
            self.pnode.dump(f"NODE {self.pnode.ID}: request log failure from leader {request.l_id}")
            return gnd.logResponse(f_id=self.pnode.ID,term=current_term,ack=0,sucess=False)
    
    def requestVote(self, request: gnd.voteRequest, context):
        self.pnode.got_replicate_req.set()
        with self.pnode.m_lock:
            f = open(self.pnode.m_path,"r")
            text = [i.split() for i in f.readlines()]
            f.close()
            current_term = int(text[0][-1])
            voted_for = int(text[1][-1])
            pass

        with self.pnode.l_lock:
            log_len, last_term = self.pnode.get_log_len_and_term()
            pass
        
        if request.c_term > current_term:
            current_term = request.c_term
            self.pnode.state = STATES["fol"]
            voted_for = -1
        
        log_ok = (request.c_log_term > last_term) or (request.c_log_term == last_term and request.c_log_len >= log_len)

        if(request.c_term == current_term and log_ok and (voted_for in [request.c_id,-1])):
            voted_for = request.c_id
            with self.pnode.m_lock:
                f = open(self.pnode.m_path,"w")
                f.writelines([f"current_term {current_term}\n",f"voted_for {voted_for}\n"])
                f.close()
                pass
            self.pnode.dump(f"NODE {self.pnode.ID}: Vote granted for Node {request.c_id} in term {current_term}.")
            l_time = self.pnode.get_lease_time()
            return gnd.voteResponse(term = current_term, granted=True,node_id=self.pnode.ID,lease_time=l_time if l_time > 1e-3 else 0)
        else:
            with self.pnode.m_lock:
                    f = open(self.pnode.m_path,"w")
                    f.writelines([f"current_term {current_term}\n",f"voted_for {voted_for}\n"])
                    f.close()
                    pass
            self.pnode.dump(f"NODE {self.pnode.ID}: Vote denied for Node {request.c_id} in term {current_term}.")
            l_time = self.pnode.get_lease_time()
            return gnd.voteResponse(term = current_term, granted=False,node_id=self.pnode.ID,lease_time=l_time if l_time > 1e-3 else 0)
        

class Node:
    # if not sufficient load next 10 lines check for appends do till start of file is not reached
    # NOTE: ORDER OF LOCKS SHOULD ALWAYS BE Meta, Logs, Dumps NOT THE OTHER WAY AROUND
    def dump(self,text: str):
        with self.d_lock:
            f = open(self.d_path,"a")
            f.write(text+'\n')
            f.close()
            pass

    def __init__(self,node_id,storage_path,did_restart):

        self.ID: int = node_id
        self.storage_path = os.path.join(storage_path,str(self.ID))
        self.m_path = os.path.join(self.storage_path,"metadata.txt")
        self.l_path = os.path.join(self.storage_path,"logs.txt")
        self.d_path = os.path.join(self.storage_path,"dump.txt")
        self.c_path = "client_service_port.json"
        self.peers = None

        self.m_lock = threading.Lock()
        self.l_lock = threading.Lock()
        self.d_lock = threading.Lock()
        
        self.commit_len = 0
        self.state = STATES["fol"]
        self.current_leader = None
        self.election_timer = None
        self.leader_lease = 0
        self.got_replicate_req = threading.Event()
        self.got_broadcast_req = threading.Event()
        self.updated_commit_len = threading.Event()

        self.vote_recieved = set()
        self.sent_length  = dict()
        self.acked_length = dict()
        self.renew_lease_time = True
        if did_restart:
            self.recover_session()
        else:
            self.create_session()
        self.load_peers(os.path.join("peers.json"))
    
    def recover_session(self):
        # TODO: implement later
        # if not os.path.exists(self.storage_path):
        #     os.mkdir(self.storage_path)
        #     print(f"Directory '{self.storage_path}' created successfully.")
        # else:
        #     print(f"Directory '{self.storage_path}' already exists.")

        # # create meta data
        # f = open(self.m_path,"w")
        # f.writelines(["current_term 0\n","voted_for -1\n"])
        # f.close()

        # create logs and dump
        # f = open(self.l_path,"w")
        # f.close()
        # f = open(self.d_path,"w")
        # f.close()
        return
    def create_session(self):

        # creating storage _path
        if not os.path.exists(self.storage_path):
            os.mkdir(self.storage_path)
            print(f"Directory '{self.storage_path}' created successfully.")
        else:
            print(f"Directory '{self.storage_path}' already exists.")

        # create meta data
        f = open(self.m_path,"w")
        f.writelines(["current_term 0\n","voted_for -1\n"])
        f.close()

        # create logs and dump
        f = open(self.l_path,"w")
        f.close()
        f = open(self.d_path,"w")
        f.close()
        return 
    
    def load_peers(self,p_path: str):
        try:
            f = open(p_path,"r")
            temp = json.load(f)
            f.close()
            self.peers = dict()
            for i in temp.keys():
                self.peers[int(i)] = temp[i]
            
        except FileNotFoundError:
            raise FileNotFoundError("no list of peers given")
        
        try:
            self.socket = self.peers[self.ID]
            f = open(self.c_path,"r")
            temp = json.load(f)
            f.close()
            self.socket_c = temp[str(self.ID)]
        except:
            raise IndexError("could not file ID in peers")
        
        self.peers.pop(self.ID)
    
    def start_server(self):
        tasks = [self.follower_task,self.candidate_task,self.leader_task]
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        server_c = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        ngpc.add_NodeServicer_to_server(NodeServicer(self),server)
        cgpc.add_clientServicer_to_server(ClientServiser(self),server_c)
        server.add_insecure_port(self.socket)
        server_c.add_insecure_port(self.socket_c)
        print(self.socket_c)
        server.start()
        server_c.start()
        try:
            while True:
                tasks[self.state]()
        except KeyboardInterrupt as e:
            server.stop(0)
            server_c.stop(0)
            print("closing_server")

        server.wait_for_termination()
        print("server closed")
        return
    
    def follower_task(self):
        if self.got_replicate_req.wait(random.random()*5 + 5): # election timer
            self.got_replicate_req.clear()
        else:
            self.state = STATES["can"]
            self.dump(f"Node {self.ID}: election timer timed out, Starting election.")
        return
    
    def candidate_task(self):
        with self.m_lock:
            f = open(self.m_path,"r+")
            test = f.readlines()
            current_term = int(test[0].split()[1]) + 1
            voted_for = self.ID
            self.vote_recieved.add(self.ID)
            f.seek(0)
            f.truncate()
            f.writelines([f"current_term {current_term}\n",f"voted_for {voted_for}"])
            f.close()
            pass
        
        print(f"NODE {self.ID}: starting election")
        with self.l_lock:
            log_len, last_term = self.get_log_len_and_term()
            pass
        ret = asyncio.run(self.election_handler(self.ID,current_term,log_len,last_term)) 
        return
    
    def leader_task(self):
        TIMEOUT = 1
        lease_time = self.get_lease_time()
        self.dump(f"Leader {self.ID} sending heartbeat & Renewing Lease")
        if lease_time < 1e-3:
            self.dump(f"Leader {self.ID} lease renewal failed. Stepping Down.")
            self.state = STATES["fol"]
            return
        ret = asyncio.run(self.replication_call(lease_time))
        if ret == False:
            return
        if self.got_broadcast_req.wait(TIMEOUT):
            self.got_broadcast_req.clear()
        return

    
    async def replication_call(self,lease_time) -> bool:
        tasks = [asyncio.create_task(self.replicate_logs(self.ID,i,lease_time)) for i in self.peers.keys()]
        ret = await asyncio.gather(*tasks)
        n = 0
        for i in ret:
            if i == None:
                n+= 1
        if n > len(ret)/2:
            print("couldn't find anyone lease won't be renewed next time")
            self.renew_lease_time = False
            return True
        self.renew_lease_time = True
        if False in ret:
            return False
        return True
    
    async def election_handler(self,c_id,c_term,c_log_len,c_log_term) -> bool:
        TIMEOUT = 3
        votes   = 1
        res = await (asyncio.gather(*[
                    asyncio.create_task(
                        self.vote_request(i,c_id,c_term,c_log_len,c_log_term,TIMEOUT)
                        ) for i in self.peers.keys()
                    ]))
        
        res: list[gnd.voteResponse] = [i for i in res if i != None]
        for i in res:
            lease_time= self.get_lease_time()
            self.set_lease_time(max(lease_time,i.lease_time))
            if i.term == c_term and i.granted:
                self.vote_recieved.add(i.node_id)
            elif i.term > c_term:
                self.dump(f"NODE {self.ID}: becoming follower")
                self.state = STATES["fol"]
                with self.m_lock:
                    f = open(self.m_path,"w")
                    f.writelines('current_term '+str(i.term)+'\n','voted_for '+str(-1)+'\n')
                    f.close()
                    pass
                return False
            if len(self.vote_recieved) >= math.ceil((len(self.peers)+1)/2):
                self.dump("New Leader waiting for Old Leader Lease to timeout.")
                req_time = self.get_lease_time()
                if (req_time > 0):
                    time.sleep(req_time)
                self.dump(f"NODE {self.ID}: became the leader for term {c_term}.")
                self.state = STATES["lea"]
                self.current_leader = self.ID
                with self.l_lock:
                    log_len,_ = self.get_log_len_and_term()
                    pass
                for i in self.peers.keys():
                    self.sent_length[i] = log_len
                    self.acked_length[i] = 0
                return True
        
        if (len(set(res)) == 1 and res[0] == None):
            time.sleep(TIMEOUT)
        return False
        

    async def vote_request(self,s_id,c_id,c_term,c_log_len,c_log_term,e_timeout) -> gnd.voteResponse:
        res = None
        try:
            with grpc.insecure_channel(self.peers[s_id], options=[('grpc.connect_timeout_ms', e_timeout*1000),]) as channel:
                stub = NodeStub(channel)
                res = stub.requestVote(gnd.voteRequest(
                    c_id=int(c_id),
                    c_term=c_term,
                    c_log_len=c_log_len,
                    c_log_term=c_log_term
                    ))
        except grpc.RpcError as e:
            if isinstance(e, grpc.Call):
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    print(f"Timeout occurred for Node: {s_id}: ", e.details())
                else:
                    print(f"UNable to connect to remote host",e.details())
                return None
            
        return res
    
    async def replicate_logs(self,l_id,f_id,lease_time) -> bool:
        # returns if the logs are up to date after call
        prefix_len = self.sent_length[f_id]
        # print(prefix_len)
        with self.m_lock:
            f = open(self.m_path,"r")
            current_term = int(f.readline().split()[-1])
            f.close()
            pass
        with self.l_lock:
            suffix = self.get_log_suffix(prefix_len)
            prefix_term = 0
            if prefix_len > 0:
                prefix_term = self.get_log_and_term_at_ind(prefix_len -1)
            pass
        try:
            with grpc.insecure_channel(self.peers[f_id]) as channel:
                stub = NodeStub(channel=channel)
                # print(suffix)
                res = stub.requestLog(gnd.logRequest(
                    l_id=l_id,
                    c_term=current_term,
                    pref_len=prefix_len,
                    pref_term=prefix_term,
                    suffix=suffix,
                    leader_lease=lease_time
                ))
        except grpc.RpcError as e:
            self.dump(f"Error occurred while sending RPC to Node {f_id}")
            return None
        # print(res)
        return self.log_response(res)

    def log_response(self,result : gnd.logResponse) -> bool:
        ''' returns False if replicate_log need to be called again'''
        with self.m_lock:
            f = open(self.m_path,"r")
            current_term = int(f.readline().split()[-1])
            f.close()
            pass
        if(result.term == current_term and self.state == STATES["lea"]):
            if result.sucess == True and result.ack > self.acked_length[result.f_id]:
                self.sent_length[result.f_id]  = result.ack
                self.acked_length[result.f_id] = result.ack
                self.commit_log_entries()
            elif self.sent_length[result.f_id] > 0:
                self.sent_length[result.f_id] -= 1
                return False
        elif result.term > current_term:
            with self.m_lock:
                f = open(self.m_path,"w")
                f.writelines([f"current_term {result.term}\n",f"voted_for {-1}\n"])
                f.close()
                pass
            self.state = STATES["fol"]
            self.dump(f"NODE {self.ID} Stepping down")
        return True
    
    def commit_log_entries(self):
        # TODO: Implement this
        # print("commiting entries")
        def ack_len(length):
            return len([i for i in self.acked_length.keys() if self.acked_length[i] >= length])
        
        with self.m_lock:
            f = open(self.m_path,"r")
            current_term = int(f.readline().split()[-1])
            f.close()
            pass
        
        with self.l_lock:
            log_len,_ = self.get_log_len_and_term()
            pass

        min_acks = math.ceil((len(self.peers.keys())+1)/2)
        ready = list([i for i in range(1,log_len+1) if ack_len(i) > min_acks])
        
        # print("lenready::: ",len(ready),ready)
        if (len(ready) != 0) and (max(ready) > self.commit_len):
            with self.l_lock:
                # print("lenready::: ",len(ready),ready,ready[0])
                log_term = self.get_log_and_term_at_ind(max(ready) - 1)
                pass
            if((log_term == current_term)):
                self.commit_len = max(ready)
                self.updated_commit_len.set()
                if (self.state == STATES["lea"]):
                    self.dump(f"Node {self.ID} (leader) committed the entry till {self.commit_len} to the state machine.")
                elif (self.state == STATES["fol"]):
                    self.dump(f"Node {self.ID} (follower) committed the entry till {self.commit_len} to the state machine.")
        return
    
    def on_general_timeout(self):
        self.state = STATES["can"]
        return
    
    def get_log_len_and_term(self):
        f = open(self.l_path)
        x = f.readlines()
        l = len(x)
        f.close()
        term = 0
        if l > 0:
            term = int(x[-1].split()[-1])
        return l, term
    
    def get_log_and_term_at_ind(self,ind):
        f = open(self.l_path)
        x = f.readlines()
        l = len(x)
        f.close()
        term = 0
        if l > 0:
            term = int(x[ind].split()[-1])
        return term
    
    def get_log_suffix(self,pref_len:int):
        f = open(self.l_path,"r")
        c = ''
        for j in range(pref_len):
            c = f.readline()
            # print(c)
        ret = []
        c = f.readline()
        while c != '':
            ret.append(c.strip())
            c = f.readline()
        f.close()
        return ret
    
    def get_latest_key_value(self,key: str) -> str:
        f = open(self.l_path,"r")
        c = ''
        ret = None
        for j in range(self.commit_len):
            c = f.readline()
            if c.startswith("SET"):
                x = c.split()
                if x[1] == key:
                    ret = x[2]
        f.close()
        return ret
    
    def append_entries(self,p_len,l_com,suffix: list[str]):
        #TODO: Implement this
        # print("appending entries")
        with self.l_lock:
            log_len, _ = self.get_log_len_and_term()

            if len(suffix) > 0 and log_len > p_len:
                index = min(log_len,p_len+len(suffix)) - 1
                index_term = self.get_log_and_term_at_ind(index)
                if index_term != int(suffix[index-p_len].split()[-1]):
                    self.delete_log_from_index(p_len - 1)

            if p_len + len(suffix) > log_len:
                f = open(self.l_path,"a")
                f.writelines([i.strip() + '\n' for i in suffix[log_len-p_len:]])
                f.close()
            pass
        if l_com > self.commit_len:
            self.commit_len = l_com
            self.dump(f"Node {self.ID} accepted AppendEntries RPC from {leader id}.")
        else:
            self.dump(f"Node {self.ID} rejected AppendEntries RPC from {leader id}.")
        return
    
    def delete_log_from_index(self,index):
        f = open(self.l_path,"r+")
        for i in range(index):
            f.readline()
        f.truncate()
        f.close()

    def get_lease_time(self) -> float:
        '''Returns time remaining in exiration of leader lease and renews it if already expired 
        
        Only call if remaining lease time is needed DO NOT modify lease time directly'''
        # TODO:(check) The leader needs to step down if it cannot reacquire the lease (did not receive a successful acknowledgment from the majority of followers within the lease duration).
        rem_time = self.leader_lease - time.time()
        if(self.renew_lease_time and self.state == STATES["lea"]):
            self.dump("renewing lease for leader")
            self.leader_lease = time.time() + 5
            with self.m_lock:
                f = open(self.m_path,"r")
                current_term = int(f.readline().split()[-1])
                f.close()
                pass
            with self.l_lock:
                f = open(self.l_path,"a")
                f.write(f"NO_OP {current_term}\n")
                f.close()
                self.acked_length[self.ID], _ = self.get_log_len_and_term()
                pass
            return 5
        return rem_time
    
    def set_lease_time(self, interval: float):
        ''' for usage with rpc only'''
        self.leader_lease = time.time() + interval

if __name__ == "__main__":
    Node(node_id=int(sys.argv[1]),storage_path=sys.argv[2],did_restart=sys.argv[3]).start_server()

