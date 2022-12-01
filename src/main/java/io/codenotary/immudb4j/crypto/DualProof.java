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
import io.codenotary.immudb4j.TxHeader;
import io.codenotary.immudb4j.Utils;

public class DualProof {

    public final TxHeader sourceTxHeader;
    public final TxHeader targetTxHeader;
    public final byte[][] inclusionProof;
    public final byte[][] consistencyProof;
    public final byte[] targetBlTxAlh;
    public final byte[][] lastInclusionProof;
    public final LinearProof linearProof;
    public final LinearAdvanceProof linearAdvanceProof;

    public DualProof(TxHeader sourceTxHeader,
            TxHeader targetTxHeader,
            byte[][] inclusionProof,
            byte[][] consistencyProof,
            byte[] targetBlTxAlh,
            byte[][] lastInclusionProof,
            LinearProof linearProof,
            LinearAdvanceProof linearAdvanceProof) {
        this.sourceTxHeader = sourceTxHeader;
        this.targetTxHeader = targetTxHeader;
        this.inclusionProof = inclusionProof;
        this.consistencyProof = consistencyProof;
        this.targetBlTxAlh = targetBlTxAlh;
        this.lastInclusionProof = lastInclusionProof;
        this.linearProof = linearProof;
        this.linearAdvanceProof = linearAdvanceProof;
    }

    public static DualProof valueOf(ImmudbProto.DualProof proof) {
        return new DualProof(
                TxHeader.valueOf(proof.getSourceTxHeader()),
                TxHeader.valueOf(proof.getTargetTxHeader()),
                Utils.convertSha256ListToBytesArray(proof.getInclusionProofList()),
                Utils.convertSha256ListToBytesArray(proof.getConsistencyProofList()),
                proof.getTargetBlTxAlh().toByteArray(),
                Utils.convertSha256ListToBytesArray(proof.getLastInclusionProofList()),
                LinearProof.valueOf(proof.getLinearProof()),
                LinearAdvanceProof.valueOf(proof.getLinearAdvanceProof())
        );
    }

}
