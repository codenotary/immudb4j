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

/**
 * ImmuState represents the root of the Merkle Tree
 * that links all the transactions within a database.
 */
public class ImmuState {

    public final String database;
    public final long txId;
    public final byte[] txHash;
    public final byte[] signature;

    public ImmuState(String database, long txId, byte[] txHash, byte[] signature) {
        this.database = database;
        this.txId = txId;
        this.txHash = txHash;
        this.signature = signature;
    }

}
