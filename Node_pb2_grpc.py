# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import Node_pb2 as Node__pb2


class NodeStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.requestVote = channel.unary_unary(
                '/Node.Node/requestVote',
                request_serializer=Node__pb2.voteRequest.SerializeToString,
                response_deserializer=Node__pb2.voteResponse.FromString,
                )
        self.requestLog = channel.unary_unary(
                '/Node.Node/requestLog',
                request_serializer=Node__pb2.logRequest.SerializeToString,
                response_deserializer=Node__pb2.logResponse.FromString,
                )


class NodeServicer(object):
    """Missing associated documentation comment in .proto file."""

    def requestVote(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def requestLog(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_NodeServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'requestVote': grpc.unary_unary_rpc_method_handler(
                    servicer.requestVote,
                    request_deserializer=Node__pb2.voteRequest.FromString,
                    response_serializer=Node__pb2.voteResponse.SerializeToString,
            ),
            'requestLog': grpc.unary_unary_rpc_method_handler(
                    servicer.requestLog,
                    request_deserializer=Node__pb2.logRequest.FromString,
                    response_serializer=Node__pb2.logResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'Node.Node', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Node(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def requestVote(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/Node.Node/requestVote',
            Node__pb2.voteRequest.SerializeToString,
            Node__pb2.voteResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def requestLog(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/Node.Node/requestLog',
            Node__pb2.logRequest.SerializeToString,
            Node__pb2.logResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)