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
import io.codenotary.immudb4j.crypto.InclusionProof;
import io.codenotary.immudb4j.exceptions.VerificationException;

public class VerifiableEntry extends Entry {
    private final Entry entry;
    private final VerifiableTx verifiableTx;
    private final InclusionProof inclusionProof;

    private VerifiableEntry(Entry entry, VerifiableTx verifiableTx, InclusionProof inclusionProof) {
        super(entry.getKey(), entry.getValue());
        this.entry = entry;
        this.verifiableTx = verifiableTx;
        this.inclusionProof = inclusionProof;
    }

    public static VerifiableEntry valueOf(ImmudbProto.VerifiableEntry verifiableEntry) throws VerificationException {
        return new VerifiableEntry(
                Entry.valueOf(verifiableEntry.getEntry()),
                VerifiableTx.valueOf(verifiableEntry.getVerifiableTx()),
                InclusionProof.valueOf(verifiableEntry.getInclusionProof())
        );
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

    @Override
    public long getTx() {
        return entry.getTx();
    }

    @Override
    public byte[] getKey() {
        return entry.getKey();
    }

    @Override
    public byte[] getValue() {
        return entry.getValue();
    }

    @Override
    public KVMetadata getMetadata() {
        return entry.getMetadata();
    }

    @Override
    public Reference getReferenceBy() {
        return entry.getReferenceBy();
    }

    @Override
    public long getRevision() {
        return entry.getRevision();
    }

    @Override
    public byte[] getEncodedKey() {
        return entry.getEncodedKey();
    }

    @Override
    public byte[] digestFor(int version) {
        return entry.digestFor(version);
    }
}
