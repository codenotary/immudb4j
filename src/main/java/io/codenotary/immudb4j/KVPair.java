/*
Copyright 2019-2021 vChain, Inc.

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

/**
 * KVPair is a simple implementation of KV, representing a key value pair.
 */
public class KVPair implements KV {

    private final byte[] key;
    private final byte[] value;

    static KV from(ImmudbProto.Entry entry) {
        return new KVPair(entry.getKey().toByteArray(), entry.getValue().toByteArray());
    }

    static KV from(ImmudbProto.ZEntry zEntry) {
        return KVPair.from(zEntry.getEntry());
    }

    public KVPair(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public KVPair(String key, byte[] value) {
        this.key = key.getBytes(StandardCharsets.UTF_8);
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return this.key;
    }

    @Override
    public byte[] getValue() {
        return this.value;
    }

    @Override
    public byte[] digest() {
        byte[] b = new byte[key.length + Consts.SHA256_SIZE];

        Utils.copy(key, b);

        byte[] hvalue = CryptoUtils.sha256Sum(value);
        Utils.copy(hvalue, b, key.length);

        return CryptoUtils.sha256Sum(b);
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
