/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

public class TxEntry {

    private byte[] k;
    private int kLength;
    private int vLength;
    private byte[] hVal;
    private long vOff;

    public TxEntry(byte[] k) {
        this.k = k;
        this.kLength = k.length;
    }

    public TxEntry(byte[] k, int vLength, byte[] hVal, long vOff) {
        this.kLength = k.length;
        this.vLength = vLength;
        this.hVal = hVal;
        this.vOff = vOff;
        this.k = new byte[kLength];
        System.arraycopy(k, 0, this.k, 0, kLength);
    }

    public byte[] Key() {
        return k;
    }

    public byte[] getKey() {
        byte[] key = new byte[kLength];
        System.arraycopy(k, 0, key, 0, kLength);
        return key;
    }

    public void setKey(byte[] key) {
        kLength = key.length;
        k = new byte[kLength];
        System.arraycopy(key, 0, k, 0, kLength);
    }

    public byte[] getHVal() {
        return hVal;
    }

    public void setHVal(byte[] hVal) {
        this.hVal = hVal;
    }

    public int getVLength() {
        return vLength;
    }

    public void setVLength(int vLength) {
        this.vLength = vLength;
    }

    public long getVOff() {
        return vOff;
    }

    public void setVOff(long vOff) {
        this.vOff = vOff;
    }

    public byte[] digest() {
        byte[] b = new byte[kLength + Consts.SHA256_SIZE];

        System.arraycopy(k, 0, b, 0, kLength);
        System.arraycopy(hVal, 0, b, kLength, hVal.length);

        return CryptoUtils.sha256Sum(b);
    }

}
