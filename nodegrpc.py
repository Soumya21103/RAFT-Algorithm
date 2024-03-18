from Node_pb2_grpc import NodeServicer
import Node_pb2 as np2
from Node import Nodes

class NodeServicer(NodeServicer):
    def __init__(self,parent_node: Nodes) -> None:
        super().__init__()
        self.parent_node = parent_node
 
    def requestVote(self, request:np2.voteRequest, context):
        # TODO : MAKE METADATA GET MORE EFFICIENT   
        if (request.term > self.parent_node.get_current_term()):
            self.parent_node.set_current_term(request.term)
            self.parent_node.become_follower_signal.set()
            self.parent_node.set_voted_for(-1)
        last_term = 0
        if(self.parent_node.get_last_log_index() > 0):
            last_term = self.parent_node.get_log_entry_term(self.parent_node.get_last_log_index() - 1)
        log_ok = (request.last_log_term > last_term) or (
            (request.last_log_term ==  last_term) and request.last_log_index >= self.parent_node.get_last_log_index()
            )
        if (request.term == self.parent_node.get_current_term()) and (log_ok) and (self.parent_node.get_voted_for() in [-1,request.candidate_id]):
            self.set_vote_for(request.candidate_id)
            return np2.voteResponse(term=self.parent_node.get_current_term(),granted=True,node_id=self.parent_node.ID)
        else:
            return np2.voteResponse(term=self.parent_node.get_current_term(),granted=False,node_id=self.parent_node.ID)
        
    