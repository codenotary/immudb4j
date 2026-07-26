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
import io.codenotary.immudb4j.crypto.DualProof;
import io.codenotary.immudb4j.exceptions.VerificationException;

import java.security.NoSuchAlgorithmException;

public class VerifiableTx {
    private final Tx tx;
    private final DualProof dualProof;
    private final Signature signature;

    private VerifiableTx(Tx tx, DualProof dualProof, Signature signature) {
        this.tx = tx;
        this.dualProof = dualProof;
        this.signature = signature;
    }

    public static VerifiableTx valueOf(ImmudbProto.VerifiableTx verifiableTx) throws VerificationException {
        Tx innerTx;
        try {
            innerTx = Tx.valueOf(verifiableTx.getTx());
        } catch (NoSuchAlgorithmException e) {
            throw new VerificationException("Failed to build VerifiableTx",e);
        }
        return new VerifiableTx(
                innerTx,
                DualProof.valueOf(verifiableTx.getDualProof()),
                Signature.valueOf(verifiableTx.getSignature())
        );
    }

    public Tx getTx() {
        return tx;
    }

    public DualProof getDualProof() {
        return dualProof;
    }

    public Signature getSignature() {
        return signature;
    }
}
