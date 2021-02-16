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

import java.security.PublicKey;
import java.util.Base64;

/**
 * ImmuState represents the state within a database.
 * It includes the latest transaction id and hash,
 * plus an optional signature, if the server is configured to do so.
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

    public boolean CheckSignature(PublicKey key) throws Exception {
        if (signature == null) {
            throw new Exception("ImmuState has no signature to check against");
        }
        // TODO @dxps: to-be-implemented, see pkg/api/schema/state.go:50
        // return signer.Verify(state.ToBytes(), state.Signature.Signature, key)
        return true;
    }

    @Override
    public String toString() {
        Base64.Encoder enc = Base64.getEncoder();
        return "ImmuState{ " +
                "database='" + database + '\'' +
                ", txId=" + txId +
                ", txHash(base64)=" + enc.encodeToString(txHash) +
                ", signature(base64)=" + enc.encodeToString(signature) +
                " }";
    }
}
