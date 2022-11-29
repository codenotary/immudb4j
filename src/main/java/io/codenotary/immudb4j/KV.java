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

import io.codenotary.immudb4j.crypto.CryptoUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * KV represents a key value pair.
 */
class KV {
    private final byte[] key;

    private final KVMetadata metadata;

    private final byte[] value;

    KV(byte[] key, KVMetadata metadata, byte[] value) {
        this.key = key;
        this.metadata = metadata;
        this.value = value;
    }

    byte[] getKey() {
        return this.key;
    }

    KVMetadata getMetadata() {
        return this.metadata;
    }

    byte[] getValue() {
        return this.value;
    }

    byte[] digestFor(int version) {
        switch (version) {
            case 0: return digest_v0();
            case 1: return digest_v1();
        }

        throw new RuntimeException("unsupported tx header version");
    }

    byte[] digest_v0() {
        if (metadata != null) {
            throw new RuntimeException("metadata is unsupported when in 1.1 compatibility mode");
        }

        byte[] b = new byte[key.length + Consts.SHA256_SIZE];

        Utils.copy(key, b);

        byte[] hvalue = CryptoUtils.sha256Sum(value);
        Utils.copy(hvalue, b, key.length);

        return CryptoUtils.sha256Sum(b);
    }

    byte[] digest_v1() {
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

}
