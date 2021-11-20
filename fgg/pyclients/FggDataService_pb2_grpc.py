# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

import FggDataService_pb2 as FggDataService__pb2


class FggDataServiceStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.requestData = channel.unary_stream(
        '/fgg.grpc.FggDataService/requestData',
        request_serializer=FggDataService__pb2.FggMsg.SerializeToString,
        response_deserializer=FggDataService__pb2.FggData.FromString,
        )
    self.persistData = channel.stream_unary(
        '/fgg.grpc.FggDataService/persistData',
        request_serializer=FggDataService__pb2.FggData.SerializeToString,
        response_deserializer=FggDataService__pb2.FggMsg.FromString,
        )
    self.queryData = channel.unary_unary(
        '/fgg.grpc.FggDataService/queryData',
        request_serializer=FggDataService__pb2.FggMsg.SerializeToString,
        response_deserializer=FggDataService__pb2.FggMsg.FromString,
        )


class FggDataServiceServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def requestData(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def persistData(self, request_iterator, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def queryData(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_FggDataServiceServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'requestData': grpc.unary_stream_rpc_method_handler(
          servicer.requestData,
          request_deserializer=FggDataService__pb2.FggMsg.FromString,
          response_serializer=FggDataService__pb2.FggData.SerializeToString,
      ),
      'persistData': grpc.stream_unary_rpc_method_handler(
          servicer.persistData,
          request_deserializer=FggDataService__pb2.FggData.FromString,
          response_serializer=FggDataService__pb2.FggMsg.SerializeToString,
      ),
      'queryData': grpc.unary_unary_rpc_method_handler(
          servicer.queryData,
          request_deserializer=FggDataService__pb2.FggMsg.FromString,
          response_serializer=FggDataService__pb2.FggMsg.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'fgg.grpc.FggDataService', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
