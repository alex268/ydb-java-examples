package tech.ydb.example;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import tech.ydb.proto.StatusCodesProtos;
import tech.ydb.proto.query.YdbQuery;
import tech.ydb.proto.query.v1.QueryServiceGrpc;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class RandomErrorChannel implements Consumer<ManagedChannelBuilder<?>>, ClientInterceptor {
    @Override
    public void accept(ManagedChannelBuilder<?> t) {
        t.intercept(this);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        if (ThreadLocalRandom.current().nextInt(6) > 2) {
            if (method == QueryServiceGrpc.getCreateSessionMethod()) {
                RespT aborted = (RespT) YdbQuery.CreateSessionResponse.newBuilder()
                        .setStatus(StatusCodesProtos.StatusIds.StatusCode.ABORTED)
                        .build();
                return new ErrorCall<>(aborted);
            }

            if (method == QueryServiceGrpc.getExecuteQueryMethod()) {
                RespT aborted = (RespT) YdbQuery.ExecuteQueryResponsePart.newBuilder()
                        .setStatus(StatusCodesProtos.StatusIds.StatusCode.BAD_SESSION)
                        .build();
                return new ErrorCall<>(aborted);
            }

            if (method == QueryServiceGrpc.getCommitTransactionMethod()) {
                RespT aborted = (RespT) YdbQuery.CommitTransactionResponse.newBuilder()
                        .setStatus(StatusCodesProtos.StatusIds.StatusCode.BAD_SESSION)
                        .build();
                return new ErrorCall<>(aborted);
            }
        }

        return next.newCall(method, callOptions);
    }

    private class ErrorCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

        private final RespT errorMsg;

        public ErrorCall(RespT errorMsg) {
            this.errorMsg = errorMsg;
        }

        @Override
        public void start(Listener<RespT> listener, Metadata headers) {
            ForkJoinPool.commonPool().execute(() -> {
                listener.onMessage(errorMsg);
                listener.onClose(Status.OK, new Metadata());
            });
        }

        @Override
        public void request(int numMessages) { }

        @Override
        public void cancel(String message, Throwable cause) { }

        @Override
        public void halfClose() { }

        @Override
        public void sendMessage(ReqT message) { }
    }
}
