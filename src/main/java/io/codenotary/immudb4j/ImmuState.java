/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
import io.codenotary.immudb4j.crypto.CryptoUtils;
import io.codenotary.immudb4j.crypto.ECDSASignature;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.Security;
import java.security.Signature;
import java.util.Arrays;
import java.util.Base64;

/**
 * ImmuState represents the state within a database.
 * It includes the latest transaction's id and hash,
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

    // This method should remain visible within this immudb4j package
    // (and not public) since this is its usage scope.
    boolean checkSignature(PublicKey pubKey) {
        if (signature != null && signature.length > 0) {
            // The Golang call: signer.Verify(state.ToBytes(), state.Signature.Signature, key)
            // that is implemented in `pkg/signer/ecdsa.go:86`.

            byte[] hash = CryptoUtils.sha256Sum(toBytes());
            try {
                // Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
                Signature sig = Signature.getInstance("SHA256withECDSA"); // , BouncyCastleProvider.PROVIDER_NAME);
                sig.initVerify(pubKey);
                sig.update(hash);
                return sig.verify(signature);

                // The `signature` coming from immudb server is an ASN.1 encoded value of
                // a `ecdsaSignature{R: r, S: s}` Golang struct, where r, s are the result
                // of `r, s, err := ecdsa.Sign(sig.rand, sig.privateKey, hash[:])` call.

//                ECDSASignature ecdsaSig = ECDSASignature.decodeFromDER(signature);
//                System.out.println("[dbg] checkSignature > ecdsa sign: \n\tr=" + ecdsaSig.r + " \n\ts=" + ecdsaSig.s
//                    + "\n\thash=" + Arrays.toString(hash));
//                 return ecdsaSig.checkSignature(hash, pubKey);


            } catch (Exception e) { //ignored) {
                System.err.println("checkSignature > e: " + e.getMessage());
            }
        }
        return false;
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
