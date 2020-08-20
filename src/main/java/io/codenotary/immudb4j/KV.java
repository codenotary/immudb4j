package io.codenotary.immudb4j;

public interface KV {
    byte[] getKey();
    byte[] getValue();
}