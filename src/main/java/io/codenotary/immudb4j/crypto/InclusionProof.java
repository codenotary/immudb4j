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

public class InclusionProof {

    public final int leaf;
    public final int width;
    public final byte[][] terms;

    public InclusionProof(int leaf, int width, byte[][] terms) {
        this.leaf = leaf;
        this.width = width;
        this.terms = terms;
    }

    public static InclusionProof valueOf(ImmudbProto.InclusionProof proof) {
        return new InclusionProof(proof.getLeaf(), proof.getWidth(), CryptoUtils.digestsFrom(proof.getTermsList()));
    }

}
