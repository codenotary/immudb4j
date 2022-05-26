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
import io.codenotary.immudb4j.crypto.CryptoUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * KVPair is a simple implementation of KV, representing a key value pair.
 */
public class KVPair implements KV {

    private final byte[] key;
    private final KVMetadata metadata;
    private final byte[] value;
    private final long txId;

    public static KV from(ImmudbProto.Entry entry) {
        KVMetadata metadata = null;

        if (entry.hasMetadata()) {
            metadata = new KVMetadata();
            metadata.asDeleted(entry.getMetadata().getDeleted());
            
            if (entry.getMetadata().hasExpiration()) {
                metadata.expiresAt(entry.getMetadata().getExpiration().getExpiresAt());
            }

            metadata.asNonIndexable(entry.getMetadata().getNonIndexable());
        }

        return new KVPair(entry.getKey().toByteArray(),
                metadata,
                entry.getValue().toByteArray(),
                entry.getTx()
        );
    }

    public static KV from(ImmudbProto.ZEntry zEntry) {
        return KVPair.from(zEntry.getEntry());
    }

    public KVPair(byte[] key, byte[] value) {
        this(key, null, value, 0);
    }

    public KVPair(byte[] key, byte[] value, long txId) {
        this(key, null, value, txId);
    }

    public KVPair(byte[] key, KVMetadata metadata, byte[] value, long txId) {
        this.key = key;
        this.metadata = metadata;
        this.value = value;
        this.txId = txId;
    }

    public KVPair(String key, byte[] value) {
        this(key, null, value);
    }

    public KVPair(String key, KVMetadata metadata, byte[] value) {
        this.key = key.getBytes(StandardCharsets.UTF_8);
        this.metadata = metadata;
        this.value = value;
        this.txId = 0;
    }

    public KVPair(byte[] key, KVMetadata metadata, byte[] value) {
        this.key = key;
        this.metadata = metadata;
        this.value = value;
        this.txId = 0;
    }

    @Override
    public byte[] getKey() {
        return this.key;
    }

    @Override
    public KVMetadata getMetadata() {
        return this.metadata;
    }

    @Override
    public byte[] getValue() {
        return this.value;
    }

    @Override
    public long getTxId() {
        return this.txId;
    }

    @Override
    public byte[] digestFor(int version) {
        switch (version) {
            case 0: return digest_v0();
            case 1: return digest_v1();
        }

        throw new RuntimeException("unsupported tx header version");
    }

    public byte[] digest_v0() {
        if (metadata != null) {
             throw new RuntimeException("metadata is unsupported when in 1.1 compatibility mode");
        }

        byte[] b = new byte[key.length + Consts.SHA256_SIZE];

        Utils.copy(key, b);

        byte[] hvalue = CryptoUtils.sha256Sum(value);
        Utils.copy(hvalue, b, key.length);

        return CryptoUtils.sha256Sum(b);
    }

    public byte[] digest_v1() {
        byte[] mdbs = null;
        int mdLen = 0;

        if (metadata != null) {
            mdbs = metadata.serialize();
            mdLen = mdbs.length;
        }

        ByteBuffer bytes =  ByteBuffer.allocate(2 + mdLen + 2 + key.length + Consts.SHA256_SIZE);
        bytes.order(ByteOrder.BIG_ENDIAN);
        
        bytes.putShort((short)mdLen);
        if (mdLen > 0) {
            bytes.put(mdbs);
        }
       
        bytes.putShort((short)key.length);
        bytes.put(key);
        
        bytes.put(CryptoUtils.sha256Sum(value));

        return CryptoUtils.sha256Sum(bytes.array());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KVPair kvPair = (KVPair) o;
        return Arrays.equals(key, kvPair.key) && Arrays.equals(value, kvPair.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return String.format("KVPair{key=%s, value=%s}", Arrays.toString(key), Arrays.toString(value));
    }

}
