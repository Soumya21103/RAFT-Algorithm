import grpc, json
import client_pb2 as client__pb2
import client_pb2_grpc as client__pb2_grpc

def servicer(command: str,server_address: str):
    with grpc.insecure_channel(server_address) as channel:
        stub = client__pb2_grpc.clientStub(channel=channel)
        res: client__pb2.ServeClientReply = stub.ServeClient(client__pb2.ServeClientArgs(
            Request=command
        ))
        return res.Data, res.LeaderID, res.Success

def main():
    with open("client_service_port.json","r") as f:
        NODE_LIST = json.load(f)
    current_leader = 0
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
            try:
                data, lid, suc = servicer(command, NODE_LIST[str(current_leader)])
            except:
                print("unable to locate host service may be down")
                data, lid, suc = None
            if lid != current_leader:
                if current_leader == None:
                    print("unable to locate host service may be down")
                    current_leader += 1
                    current_leader %= len(NODE_LIST)
                else:
                    current_leader = int(lid)
            if suc == False:
                print("request rejected try again")

            print("Data: ", data)
            print("LeaderID: ", lid)
            print("Success: ", suc)
        else:
            print("Invalid Command")


if __name__ == "__main__":
    main()
