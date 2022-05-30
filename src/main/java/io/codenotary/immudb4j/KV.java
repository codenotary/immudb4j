package io.codenotary.immudb4j;

import io.codenotary.immudb4j.crypto.CryptoUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * KV represents a key value pair.
 */
class KV {
    private final byte[] key;

    private final KVMetadata metadata;

    private final byte[] value;

    KV(String key, byte[] value) {
        this(key.getBytes(StandardCharsets.UTF_8), null, value);
    }

    KV(byte[] key, byte[] value) {
        this(key, null, value);
    }

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

}
