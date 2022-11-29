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

public class Entry {
    private long tx;

    private byte[] key;

    private byte[] value;

    private KVMetadata metadata;

    private Reference referencedBy;

    private long revision;

    private Entry() {}

    public Entry(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public static Entry valueOf(ImmudbProto.Entry e) {
        final Entry entry = new Entry();

        entry.tx = e.getTx();
        entry.key = e.getKey().toByteArray();
        entry.value = e.getValue().toByteArray();

        if (e.hasMetadata()) {
            entry.metadata = KVMetadata.valueOf(e.getMetadata());
        }

        if (e.hasReferencedBy()) {
            entry.referencedBy = Reference.valueOf(e.getReferencedBy());
        }

        entry.revision = e.getRevision();

        return entry;
    }

    public long getTx() {
        return tx;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public KVMetadata getMetadata() {
        return metadata;
    }

    public Reference getReferenceBy() {
        return referencedBy;
    }

    public long getRevision() {
        return revision;
    }

    public byte[] getEncodedKey() {
        if (referencedBy == null) {
            return Utils.wrapWithPrefix(key, Consts.SET_KEY_PREFIX);
        }

        return Utils.wrapWithPrefix(referencedBy.getKey(), Consts.SET_KEY_PREFIX);
    }

    public byte[] digestFor(int version) {
        final KV kv;

        if (referencedBy == null) {
            kv = new KV(
                    getEncodedKey(),
                    metadata,
                    Utils.wrapWithPrefix(value, Consts.PLAIN_VALUE_PREFIX)
            );
        } else {
            kv = new KV(
                    getEncodedKey(),
                    referencedBy.getMetadata(),
                    Utils.wrapReferenceValueAt(Utils.wrapWithPrefix(key, Consts.SET_KEY_PREFIX), referencedBy.getAtTx())
            );
        }

        return kv.digestFor(version);
    }

}
