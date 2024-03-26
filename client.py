import grpc
import client_pb2 as client__pb2
import client_pb2_grpc as client__pb2_grpc

NODE_LIST = [
    "localhost:50051",
    "localhost:50052",
    "localhost:50053",
    "localhost:50054",
    "localhost:50055",
]


def main():
    # put a loop here to keep the client running
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
            channel = grpc.insecure_channel(NODE_LIST[0])
            # create a stub
            stub = client__pb2_grpc.clientStub(channel)
            # create a request
            response = stub.ServeClient(client__pb2.ServeClientArgs(Request=command))
            print("Data: ",response.Data)
            print("LeaderID: ", response.LeaderID)
            print("Success: ", response.Success)
        else:
            print("Invalid Command")


if __name__ == "__main__":
    main()
