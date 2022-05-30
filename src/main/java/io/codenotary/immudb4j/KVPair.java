package io.codenotary.immudb4j;

/**
 * KVPair represents a key value pair.
 */
public class KVPair {
    private final byte[] key;

    private final byte[] value;

    public KVPair(String key, byte[] value) {
        this(Utils.toByteArray(key), value);
    }

    public KVPair(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        return this.key;
    }

    public byte[] getValue() {
        return this.value;
    }

}
