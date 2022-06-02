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

import io.codenotary.immudb.ImmudbProto;

import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.Signature;

/**
 * ImmuState represents the state within a database.
 * It includes the latest transaction's id and hash,
 * plus an optional signature, if the server is configured to do so.
 */
public class ImmuState {

    private final String database;
    private final long txId;
    private final byte[] txHash;
    private final byte[] signature;

    ImmuState(String database, long txId, byte[] txHash, byte[] signature) {
        this.database = database;
        this.txId = txId;
        this.txHash = txHash;
        this.signature = signature;
    }

    // This method should remain visible within this immudb4j package
    // (and not `public`) since this is its usage scope.
    boolean checkSignature(PublicKey pubKey) {
        if (pubKey == null) {
            return true;
        }

        if (signature != null && signature.length > 0) {
            try {
                Signature sig = Signature.getInstance("SHA256withECDSA");
                sig.initVerify(pubKey);
                sig.update(toBytes());
                return sig.verify(signature);
            } catch (Exception e) {
                return false;
            }
        }

        return false;
    }

    public String getDatabase() {
        return database;
    }

    public long getTxId() {
        return txId;
    }

    public byte[] getTxHash() {
        return txHash;
    }

    public byte[] getSignature() {
        return signature;
    }

    private byte[] toBytes() {
        byte[] b = new byte[4 + database.length() + 8 + Consts.SHA256_SIZE];
        int i = 0;
        Utils.putUint32(database.length(), b, i);
        i += 4;
        Utils.copy(database.getBytes(StandardCharsets.UTF_8), b, i);
        i += database.length();
        Utils.putUint64(txId, b, i);
        i += 8;
        Utils.copy(txHash, b, i);
        return b;
    }

    // This method is not public. It is visible only within the immudb4j package
    // since this should only be used by ImmuClient, just to hide any gRPC stuff.
    static ImmuState valueOf(ImmudbProto.ImmutableState state) {
        return new ImmuState(
                state.getDb(),
                state.getTxId(),
                state.getTxHash().toByteArray(),
                state.getSignature().getSignature().toByteArray()
        );
    }

}
