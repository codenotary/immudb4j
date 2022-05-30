package io.codenotary.immudb4j;

import io.grpc.*;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;


public class ImmuServerUUIDInterceptor implements ClientInterceptor {

    private static final String SERVER_UUID = "immudb-uuid";

    private final ImmuClient client;

    public ImmuServerUUIDInterceptor(ImmuClient client) {
        this.client = client;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {

                SimpleForwardingClientCallListener<RespT> listener = new SimpleForwardingClientCallListener<RespT>(responseListener) {

                    @Override
                    public void onHeaders(Metadata headers) {
                        String serverUuid = headers.get(Metadata.Key.of(SERVER_UUID, Metadata.ASCII_STRING_MARSHALLER));
                        if (serverUuid != null && !serverUuid.equals(client.getCurrentServerUuid())) {
                            client.setCurrentServerUuid(serverUuid);
                        }
                        super.onHeaders(headers);
                    }
                };
                super.start(listener, headers);
            }

        };

    }

}
