package fgg.grpc;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.7.0)",
    comments = "Source: FggDataService.proto")
 */
public final class FggDataServiceGrpc {

  private FggDataServiceGrpc() {}

  public static final String SERVICE_NAME = "fgg.grpc.FggDataService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<fgg.grpc.FggDataServiceOuterClass.FggMsg,
      fgg.grpc.FggDataServiceOuterClass.FggData> METHOD_REQUEST_DATA =
      io.grpc.MethodDescriptor.<fgg.grpc.FggDataServiceOuterClass.FggMsg, fgg.grpc.FggDataServiceOuterClass.FggData>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
          .setFullMethodName(generateFullMethodName(
              "fgg.grpc.FggDataService", "requestData"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              fgg.grpc.FggDataServiceOuterClass.FggMsg.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              fgg.grpc.FggDataServiceOuterClass.FggData.getDefaultInstance()))
          .setSchemaDescriptor(new FggDataServiceMethodDescriptorSupplier("requestData"))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<fgg.grpc.FggDataServiceOuterClass.FggData,
      fgg.grpc.FggDataServiceOuterClass.FggMsg> METHOD_PERSIST_DATA =
      io.grpc.MethodDescriptor.<fgg.grpc.FggDataServiceOuterClass.FggData, fgg.grpc.FggDataServiceOuterClass.FggMsg>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
          .setFullMethodName(generateFullMethodName(
              "fgg.grpc.FggDataService", "persistData"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              fgg.grpc.FggDataServiceOuterClass.FggData.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              fgg.grpc.FggDataServiceOuterClass.FggMsg.getDefaultInstance()))
          .setSchemaDescriptor(new FggDataServiceMethodDescriptorSupplier("persistData"))
          .build();
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<fgg.grpc.FggDataServiceOuterClass.FggMsg,
      fgg.grpc.FggDataServiceOuterClass.FggMsg> METHOD_QUERY_DATA =
      io.grpc.MethodDescriptor.<fgg.grpc.FggDataServiceOuterClass.FggMsg, fgg.grpc.FggDataServiceOuterClass.FggMsg>newBuilder()
          .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(generateFullMethodName(
              "fgg.grpc.FggDataService", "queryData"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              fgg.grpc.FggDataServiceOuterClass.FggMsg.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
              fgg.grpc.FggDataServiceOuterClass.FggMsg.getDefaultInstance()))
          .setSchemaDescriptor(new FggDataServiceMethodDescriptorSupplier("queryData"))
          .build();

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static FggDataServiceStub newStub(io.grpc.Channel channel) {
    return new FggDataServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static FggDataServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new FggDataServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static FggDataServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new FggDataServiceFutureStub(channel);
  }

  /**
   */
  public static abstract class FggDataServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void requestData(fgg.grpc.FggDataServiceOuterClass.FggMsg request,
        io.grpc.stub.StreamObserver<fgg.grpc.FggDataServiceOuterClass.FggData> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_REQUEST_DATA, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<fgg.grpc.FggDataServiceOuterClass.FggData> persistData(
        io.grpc.stub.StreamObserver<fgg.grpc.FggDataServiceOuterClass.FggMsg> responseObserver) {
      return asyncUnimplementedStreamingCall(METHOD_PERSIST_DATA, responseObserver);
    }

    /**
     */
    public void queryData(fgg.grpc.FggDataServiceOuterClass.FggMsg request,
        io.grpc.stub.StreamObserver<fgg.grpc.FggDataServiceOuterClass.FggMsg> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_QUERY_DATA, responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_REQUEST_DATA,
            asyncServerStreamingCall(
              new MethodHandlers<
                fgg.grpc.FggDataServiceOuterClass.FggMsg,
                fgg.grpc.FggDataServiceOuterClass.FggData>(
                  this, METHODID_REQUEST_DATA)))
          .addMethod(
            METHOD_PERSIST_DATA,
            asyncClientStreamingCall(
              new MethodHandlers<
                fgg.grpc.FggDataServiceOuterClass.FggData,
                fgg.grpc.FggDataServiceOuterClass.FggMsg>(
                  this, METHODID_PERSIST_DATA)))
          .addMethod(
            METHOD_QUERY_DATA,
            asyncUnaryCall(
              new MethodHandlers<
                fgg.grpc.FggDataServiceOuterClass.FggMsg,
                fgg.grpc.FggDataServiceOuterClass.FggMsg>(
                  this, METHODID_QUERY_DATA)))
          .build();
    }
  }

  /**
   */
  public static final class FggDataServiceStub extends io.grpc.stub.AbstractStub<FggDataServiceStub> {
    private FggDataServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FggDataServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FggDataServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FggDataServiceStub(channel, callOptions);
    }

    /**
     */
    public void requestData(fgg.grpc.FggDataServiceOuterClass.FggMsg request,
        io.grpc.stub.StreamObserver<fgg.grpc.FggDataServiceOuterClass.FggData> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(METHOD_REQUEST_DATA, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<fgg.grpc.FggDataServiceOuterClass.FggData> persistData(
        io.grpc.stub.StreamObserver<fgg.grpc.FggDataServiceOuterClass.FggMsg> responseObserver) {
      return asyncClientStreamingCall(
          getChannel().newCall(METHOD_PERSIST_DATA, getCallOptions()), responseObserver);
    }

    /**
     */
    public void queryData(fgg.grpc.FggDataServiceOuterClass.FggMsg request,
        io.grpc.stub.StreamObserver<fgg.grpc.FggDataServiceOuterClass.FggMsg> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_QUERY_DATA, getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class FggDataServiceBlockingStub extends io.grpc.stub.AbstractStub<FggDataServiceBlockingStub> {
    private FggDataServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FggDataServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FggDataServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FggDataServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<fgg.grpc.FggDataServiceOuterClass.FggData> requestData(
        fgg.grpc.FggDataServiceOuterClass.FggMsg request) {
      return blockingServerStreamingCall(
          getChannel(), METHOD_REQUEST_DATA, getCallOptions(), request);
    }

    /**
     */
    public fgg.grpc.FggDataServiceOuterClass.FggMsg queryData(fgg.grpc.FggDataServiceOuterClass.FggMsg request) {
      return blockingUnaryCall(
          getChannel(), METHOD_QUERY_DATA, getCallOptions(), request);
    }
  }

  /**
   */
  public static final class FggDataServiceFutureStub extends io.grpc.stub.AbstractStub<FggDataServiceFutureStub> {
    private FggDataServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FggDataServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FggDataServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FggDataServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<fgg.grpc.FggDataServiceOuterClass.FggMsg> queryData(
        fgg.grpc.FggDataServiceOuterClass.FggMsg request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_QUERY_DATA, getCallOptions()), request);
    }
  }

  private static final int METHODID_REQUEST_DATA = 0;
  private static final int METHODID_QUERY_DATA = 1;
  private static final int METHODID_PERSIST_DATA = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final FggDataServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(FggDataServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_REQUEST_DATA:
          serviceImpl.requestData((fgg.grpc.FggDataServiceOuterClass.FggMsg) request,
              (io.grpc.stub.StreamObserver<fgg.grpc.FggDataServiceOuterClass.FggData>) responseObserver);
          break;
        case METHODID_QUERY_DATA:
          serviceImpl.queryData((fgg.grpc.FggDataServiceOuterClass.FggMsg) request,
              (io.grpc.stub.StreamObserver<fgg.grpc.FggDataServiceOuterClass.FggMsg>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PERSIST_DATA:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.persistData(
              (io.grpc.stub.StreamObserver<fgg.grpc.FggDataServiceOuterClass.FggMsg>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class FggDataServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    FggDataServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return fgg.grpc.FggDataServiceOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("FggDataService");
    }
  }

  private static final class FggDataServiceFileDescriptorSupplier
      extends FggDataServiceBaseDescriptorSupplier {
    FggDataServiceFileDescriptorSupplier() {}
  }

  private static final class FggDataServiceMethodDescriptorSupplier
      extends FggDataServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    FggDataServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (FggDataServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new FggDataServiceFileDescriptorSupplier())
              .addMethod(METHOD_REQUEST_DATA)
              .addMethod(METHOD_PERSIST_DATA)
              .addMethod(METHOD_QUERY_DATA)
              .build();
        }
      }
    }
    return result;
  }
}
