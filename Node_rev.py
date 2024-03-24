import os, asyncio
import threading, json, math, random
import Node_pb2_grpc as ngpc
from Node_pb2_grpc import NodeStub
from concurrent import futures
import Node_pb2 as gnd
import grpc

STATES = {
    "fol": 0,
    "can": 1,
    "lea": 2
}

class NodeServicer(ngpc.NodeServicer):
    def __init__(self,parent_node) -> None:
        super().__init__()
        self.pnode: Node = parent_node
    
    def requestLog(self, request: gnd.logRequest, context):
        #TODO: Something about election timer seems odd
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
            self.pnode.append_entries(request.pref_len,request.l_commit,list(request.suffix)) #TODO: implement apend entries
            ack = request.pref_len + len(request.suffix)
            with self.pnode.m_lock:
                f = open(self.pnode.m_path,"w")
                f.writelines([f"current_term {current_term}\n",f"voted_for {voted_for}\n"])
                f.close()
                pass
            return gnd.logResponse(f_id=self.pnode.ID,term=current_term,ack=ack,sucess=True)
        else:
            with self.pnode.m_lock:
                f = open(self.pnode.m_path,"w")
                f.writelines([f"current_term {current_term}\n",f"voted_for {voted_for}\n"])
                f.close()
                pass
            return gnd.logResponse(f_id=self.pnode.ID,term=current_term,ack=0,sucess=False)
    
    def requestVote(self, request: gnd.voteRequest, context):
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
            return gnd.voteResponse(term = current_term, granted=True,node_id=self.pnode.ID)
        else:
            with self.pnode.m_lock:
                    f = open(self.pnode.m_path,"w")
                    f.writelines([f"current_term {current_term}\n",f"voted_for {voted_for}\n"])
                    f.close()
                    pass
            return gnd.voteResponse(term = current_term, granted=False,node_id=self.pnode.ID)
        

class Node:
    # if not sufficient load next 10 lines check for appends do till start of file is not reached
    # NOTE: ORDER OF LOCKS SHOULD ALWAYS BE Meta, Logs, Dumps NOT THE OTHER WAY AROUND
    def __init__(self,node_id,storage_path,did_restart):

        self.ID = node_id
        self.storage_path = storage_path
        self.m_path = os.path.join(self.storage_path,"metadata.txt")
        self.l_path = os.path.join(self.storage_path,"logs.txt")
        self.d_path = os.path.join(self.storage_path,"dump.txt")
        self.peers = None

        self.m_lock = threading.Lock()
        self.l_lock = threading.Lock()
        self.d_lock = threading.Lock()
        
        self.commit_len = 0
        self.state = STATES["fol"]
        self.current_leader = None
        self.election_timer = None

        self.lease_released = None
        self.got_replicate_req = threading.Event()
        self.got_broadcast_req = threading.Event()

        self.vote_recieved = set()
        self.sent_length  = dict()
        self.acked_length = dict()
        if did_restart:
            self.recover_session()
        else:
            self.create_session()
        self.load_peers(os.path.join("peers.json"))
    
    def recover_session(self):
        # TODO: implement later
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
            self.peers = json.load(f)
            f.close()
        except FileNotFoundError:
            raise FileNotFoundError("no list of peers given")
        
        try:
            self.socket = self.peers[self.ID]
        except:
            raise IndexError("could not file ID in peers")
        
        self.peers.pop(self.ID)
    
    def start_server(self):
        tasks = [self.follower_task,self.candidate_task,self.leader_task]

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        ngpc.add_NodeServicer_to_server(NodeServicer(self),server)
        server.add_insecure_port(self.socket)
        server.start()
        try:
            while True:
                tasks[self.state]()
        except KeyboardInterrupt as e:
            server.stop()
            print("closing_server")

        server.wait_for_termination()
        print("server closed")
        return
    
    def follower_task(self):
        if self.got_replicate_req.wait(random.random()*5 + 5):
            self.got_replicate_req.clear()
        else:
            self.state = STATES["can"]
            print("Timeout occured")
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
        
        with self.l_lock:
            _, last_term = self.get_log_len_and_term()
            pass
        asyncio.run(self.election_handler())
        return
    
    def leader_task(self):
        TIMEOUT = 5
        ret = asyncio.run(self.replication_call(TIMEOUT))
        if ret == False:
            return
        self.got_broadcast_req.wait(TIMEOUT)
        self.got_broadcast_req.clear()
        return

    
    async def replication_call(self,timeout) -> bool:
        tasks = [asyncio.create_task(self.replicate_logs(self.ID,i)) for i in self.peers.keys()]
        ret = await asyncio.gather(tasks)
        if False in ret:
            return False
        return True
    
    async def election_handler(self,c_id,c_term,c_log_len,c_log_term):
        TIMEOUT = 5
        votes   = 1
        res = await (asyncio.gather([
                    asyncio.create_task(
                        self.vote_request(i,self.ID,c_id,c_term,c_log_len,c_log_term,TIMEOUT)
                        ) for i in self.peers.keys()
                    ]))
        for i in res:
            if gnd.voteResponse.term == c_term and gnd.voteResponse.granted:
                self.vote_recieved.add(gnd.voteResponse.node_id)
            elif gnd.voteResponse.term > c_term:
                self.state = STATES["fol"]
                with self.m_lock:
                    f = open(self.m_path,"w")
                    f.writelines('current_term '+str(gnd.voteResponse.term)+'\n','voted_for '+str(-1)+'\n')
                    f.close()
                    pass
                return
            if len(self.vote_recieved) >= math.ceil((len(self.peers)+1)/2):
                self.state = STATES["lea"]
                self.current_leader = self.ID
                return
        return 
        

    async def vote_request(self,s_id,c_id,c_term,c_log_len,c_log_term,e_timeout) -> gnd.voteResponse:
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
                    return None
            raise e
        return res
    
    async def replicate_logs(self,l_id,f_id) -> bool:
        # returns if the logs are up to date after call
        prefix_len = self.sent_length[f_id]
        with self.m_lock:
            f = open(self.m_path,"r")
            current_term = f.readline().split()[-1]
            f.close()
            pass
        with self.l_lock:
            suffix = self.get_log_suffix(prefix_len)
            prefix_term = 0
            if prefix_len > 0:
                prefix_term = self.get_log_and_term_at_ind(prefix_len -1)
            pass
        with grpc.insecure_channel(self.peers(f_id)) as channel:
            stub = NodeStub(channel=channel)
            res = stub.requestLog(gnd.logRequest(
                l_id=l_id,
                c_term=current_term,
                pref_len=prefix_len,
                pref_term=prefix_term,
                suffix=suffix
            ))
        return self.log_response(res)

    def log_response(self,result : gnd.logResponse) -> bool:
        ''' returns False if replicate_log need to be called again'''
        with self.m_lock:
            f = open(self.m_path,"r")
            current_term = f.readline().split()[-1]
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
        return True
    
    def commit_log_entries(self):
        # TODO: Implement this
        
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
        ret = []
        while c != '':
            c = f.readline()
            ret.append(c.strip())
        return ret
    
    def append_entries(self,p_len,l_com,suffix):
        #TODO: Implement this
        return

        
