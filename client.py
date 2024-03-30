import grpc
import client_pb2 as client__pb2
import client_pb2_grpc as client__pb2_grpc

# server_address = 'localhost:50051'

NODE_LIST = [
    "localhost:50051",
    "localhost:50052",
    "localhost:50053",
    "localhost:50054",
    "localhost:50055",
]


class RaftClient:
    def __init__(self, server_address):
        self.server_address = server_address
        self.channel = grpc.insecure_channel(server_address)
        self.stub = client__pb2_grpc.clientStub(self.channel)

    def servicer(self, request):
        args = client__pb2.ServeClientArgs(Request=request)
        reply = self.stub.ServeClient(args)
        return reply.Data, reply.LeaderID, reply.Success


def main():
    client = RaftClient(NODE_LIST[0]) #####

    while True:
        command = input("=> ")
        if command == "exit":
            break

        arr = command.split(" ")
        flag = 0

        if arr[0] == "get":
            if len(arr) == 2:
                flag = 1
        elif arr[0] == "set":
            if len(arr) == 3:
                flag = 1

        if flag == 1:
            data, lid, suc = client.servicer(command)
            print("Data: ", data)
            print("LeaderID: ", lid)
            print("Success: ", suc)
        else:
            print("Invalid Command")


if __name__ == "__main__":
    main()
