/*
Copyright 2019-2021 vChain, Inc.

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

//
// TODO: TBD usage
//
public class ImmuClientOptions {

    private static final String ADMIN_TOKEN_FILE_SUFFIX = "_admin";

    private String dir;
    private String address;
    private int port;

    private int healthCheckRetries;

    private boolean useMTLs;
    private MTLsOptions mtlsOptions;


    private boolean auth;
    private int maxRecvMsgSize;

    private String currentDatabase;

    public static ImmuClientOptions DefaultOptions() {
        ImmuClientOptions opts = new ImmuClientOptions();
        opts.dir = ".";
        opts.address = "127.0.0.1";
        opts.port = 3322;
        opts.healthCheckRetries = 5;
        opts.useMTLs = false;
        opts.auth = true;
        opts.maxRecvMsgSize = 4 * 1024 * 1024; // 4 MB

        return opts;
    }

}
