/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package io.codenotary.immudb4j;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

public class ImmudbAuthRequestInterceptor implements ClientInterceptor {

    private static final String SESSION_ID = "sessionid";
    private static final String TRANSACTION_ID = "transactionid";

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

                    if (session.getTransactionID() != null) {
                        headers.put(Metadata.Key.of(TRANSACTION_ID, Metadata.ASCII_STRING_MARSHALLER), session.getTransactionID());
                    }
                }

                super.start(responseListener, headers);
            }

        };
    }

}
