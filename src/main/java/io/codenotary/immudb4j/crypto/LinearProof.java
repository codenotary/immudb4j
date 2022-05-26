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
package io.codenotary.immudb4j.crypto;

import io.codenotary.immudb.ImmudbProto;
import io.codenotary.immudb4j.Utils;

public class LinearProof {

    public final long sourceTxId;
    public final long targetTxId;
    public final byte[][] terms;

    public LinearProof(long sourceTxId, long targetTxId, byte[][] terms) {
        this.sourceTxId = sourceTxId;
        this.targetTxId = targetTxId;
        this.terms = terms;
    }

    public static LinearProof valueOf(ImmudbProto.LinearProof proof) {
        return new LinearProof(
                proof.getSourceTxId(),
                proof.getTargetTxId(),
                Utils.convertSha256ListToBytesArray(proof.getTermsList())
        );
    }

    @Override
    public String toString() {
        return "LinearProof{" +
                "sourceTxId=" + sourceTxId +
                ", targetTxId=" + targetTxId +
                ", terms=" + Utils.toStringAsBase64Values(terms) +
                '}';
    }

}
