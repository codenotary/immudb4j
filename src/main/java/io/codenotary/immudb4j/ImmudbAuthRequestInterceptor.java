package io.codenotary.immudb4j;

import io.grpc.*;


public class ImmudbAuthRequestInterceptor implements ClientInterceptor {

    private static final String SESSION_ID = "sessionid";

    private final ImmuClient client;

    public ImmudbAuthRequestInterceptor(ImmuClient client) {
        this.client = client;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions, Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                final Session session = client.getSession();

                if (session != null) {
                    headers.put(Metadata.Key.of(SESSION_ID, Metadata.ASCII_STRING_MARSHALLER), session.getSessionID());
                }

                super.start(responseListener, headers);
            }

        };
    }

}
