/*
Copyright 2019-2020 vChain, Inc.

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
package io.codenotary.immudb4j.crypto;

/**
 * ImmutableState represents the root of the Merkle Tree that links
 * all the database
 */
public class ImmutableState {

    private final String database;
    private final long txId;
    private final byte[] txHash;

    public ImmutableState(String database, long txId, byte[] txHash) {
        this.database = database;
        this.txId = txId;
        this.txHash = txHash;
    }

    public String getDatabase() {
        return this.database;
    }

    public long getTxId() {
        return this.txId;
    }

    public byte[] getTxHash() {
        return this.txHash;
    }

}
