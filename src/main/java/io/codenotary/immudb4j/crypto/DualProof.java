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
package io.codenotary.immudb4j.crypto;

import io.codenotary.immudb.ImmudbProto;
import io.codenotary.immudb4j.TxMetadata;
import io.codenotary.immudb4j.Utils;

public class DualProof {

    public final TxMetadata sourceTxMetadata;
    public final TxMetadata targetTxMetadata;
    public final byte[][] inclusionProof;
    public final byte[][] consistencyProof;
    public final byte[] targetBlTxAlh;
    public final byte[][] lastInclusionProof;
    public final LinearProof linearProof;

    public DualProof(TxMetadata sourceTxMetadata,
                     TxMetadata targetTxMetadata,
                     byte[][] inclusionProof,
                     byte[][] consistencyProof,
                     byte[] targetBlTxAlh,
                     byte[][] lastInclusionProof,
                     LinearProof linearProof) {
        this.sourceTxMetadata = sourceTxMetadata;
        this.targetTxMetadata = targetTxMetadata;
        this.inclusionProof = inclusionProof;
        this.consistencyProof = consistencyProof;
        this.targetBlTxAlh = targetBlTxAlh;
        this.lastInclusionProof = lastInclusionProof;
        this.linearProof = linearProof;
    }

    public static DualProof valueOf(ImmudbProto.DualProof proof) {
        return new DualProof(
                TxMetadata.valueOf(proof.getSourceTxMetadata()),
                TxMetadata.valueOf(proof.getTargetTxMetadata()),
                Utils.convertSha256ListToBytesArray(proof.getInclusionProofList()),
                Utils.convertSha256ListToBytesArray(proof.getConsistencyProofList()),
                proof.getTargetBlTxAlh().toByteArray(),
                Utils.convertSha256ListToBytesArray(proof.getLastInclusionProofList()),
                LinearProof.valueOf(proof.getLinearProof())
        );
    }

    @Override
    public String toString() {
        return "DualProof{" +
                "sourceTxMetadata=" + sourceTxMetadata +
                ", targetTxMetadata=" + targetTxMetadata +
                ", inclusionProof=" + Utils.toStringAsBase64Values(inclusionProof) +
                ", consistencyProof=" + Utils.toStringAsBase64Values(consistencyProof) +
                ", targetBlTxAlh=" + Utils.asBase64(targetBlTxAlh) +
                ", lastInclusionProof=" + Utils.toStringAsBase64Values(lastInclusionProof) +
                ", linearProof=" + linearProof +
                '}';
    }

}
