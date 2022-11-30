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

import java.util.ArrayList;
import java.util.List;

import io.codenotary.immudb.ImmudbProto;
import io.codenotary.immudb4j.Utils;

public class LinearAdvanceProof {

    public final List<InclusionProof> inclusionProofs;
    public final byte[][] terms;

    public LinearAdvanceProof(List<InclusionProof> inclusionProofs, byte[][] terms) {
        this.inclusionProofs = inclusionProofs;
        this.terms = terms;
    }

    public static LinearAdvanceProof valueOf(ImmudbProto.LinearAdvanceProof proof) {
        final List<InclusionProof> inclusionProofs = new ArrayList<>(proof.getInclusionProofsCount());

        for (int i = 0; i < proof.getInclusionProofsCount(); i++) {
            inclusionProofs.set(i, InclusionProof.valueOf(proof.getInclusionProofs(i)));
        }

        return new LinearAdvanceProof(
                inclusionProofs,
                Utils.convertSha256ListToBytesArray(proof.getLinearProofTermsList()));
    }

}
