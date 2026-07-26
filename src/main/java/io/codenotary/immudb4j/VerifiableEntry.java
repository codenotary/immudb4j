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

import io.codenotary.immudb4j.crypto.InclusionProof;

import java.util.Objects;

public class VerifiableEntry extends Entry {
    private final Entry entry;
    private final VerifiableTx verifiableTx;
    private final InclusionProof inclusionProof;

    private VerifiableEntry(Entry entry, VerifiableTx verifiableTx, InclusionProof inclusionProof) {
        super(entry.getTx(),
                entry.getKey(),
                entry.getValue(),
                entry.getMetadata(),
                entry.getReferenceBy(),
                entry.getRevision());
        this.entry = entry;
        this.verifiableTx = verifiableTx;
        this.inclusionProof = inclusionProof;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Entry getEntry() {
        return entry;
    }

    public VerifiableTx getVerifiableTx() {
        return verifiableTx;
    }

    public InclusionProof getInclusionProof() {
        return inclusionProof;
    }

    public static class Builder {
        private Entry entry;
        private VerifiableTx verifiableTx;
        private InclusionProof inclusionProof;

        private Builder() {

        }

        public Builder withEntry(Entry entry) {
            this.entry = entry;
            return this;
        }

        public Builder withVerifiableTx(VerifiableTx verifiableTx) {
            this.verifiableTx = verifiableTx;
            return this;
        }

        public Builder withInclusionProof(InclusionProof inclusionProof) {
            this.inclusionProof = inclusionProof;
            return this;
        }

        public VerifiableEntry build() {
            Objects.requireNonNull(this.entry, "'entry' cant be null");
            Objects.requireNonNull(this.verifiableTx, "'verifiableTx' cant be null");
            Objects.requireNonNull(this.inclusionProof, "'inclusionProof' cant be null");
            return new VerifiableEntry(this.entry, this.verifiableTx, this.inclusionProof);
        }
    }

}
