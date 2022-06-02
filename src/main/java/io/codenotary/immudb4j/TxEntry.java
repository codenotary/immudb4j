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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import io.codenotary.immudb.ImmudbProto;
import io.codenotary.immudb4j.crypto.CryptoUtils;

public class TxEntry {

    private byte[] key;
    private KVMetadata metadata;
    private int vLength;
    private byte[] hVal;

    private TxEntry(byte[] key, KVMetadata metadata, int vLength, byte[] hVal) {
        this.key = new byte[key.length];
        System.arraycopy(key, 0, this.key, 0, key.length);

        this.metadata = metadata;

        this.vLength = vLength;
        this.hVal = hVal;
    }

    public static TxEntry valueOf(ImmudbProto.TxEntry txe) {
        KVMetadata md = null;

        if (txe.hasMetadata()) {
            md = KVMetadata.valueOf(txe.getMetadata());
        }

        return new TxEntry(
                        txe.getKey().toByteArray(),
                        md,
                        txe.getVLen(),
                        CryptoUtils.digestFrom(txe.getHValue().toByteArray())
                );
    }

    public byte[] getKey() {
        return key;
    }

    public KVMetadata getMetadata() {
        return metadata;
    }

    public byte[] getHVal() {
        return hVal;
    }

    public int getVLength() {
        return vLength;
    }

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

        System.arraycopy(key, 0, b, 0, key.length);
        System.arraycopy(hVal, 0, b, key.length, hVal.length);

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
        
        bytes.put(hVal);

        return CryptoUtils.sha256Sum(bytes.array());
    }

}
