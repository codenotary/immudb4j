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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ZEntry {

    private static final int setLenLen = 8;
    private static final int scoreLen = 8;
    private static final int keyLenLen = 8;
    private static final int txIDLen = 8;

    private byte[] set;

    private byte[] key;

    private Entry entry;

    private double score;

    private long atTx;

    private ZEntry() {}

    public ZEntry(byte[] set, byte[] key, double score, long atTx, Entry entry) {
        this.set = set;
        this.key = key;
        this.score = score;
        this.atTx = atTx;
        this.entry = entry;
    }

    public static ZEntry valueOf(ImmudbProto.ZEntry e) {
        final ZEntry entry = new ZEntry();

        entry.set = e.getSet().toByteArray();
        entry.key = e.getKey().toByteArray();
        entry.entry = Entry.valueOf(e.getEntry());
        entry.score = e.getScore();
        entry.atTx = e.getAtTx();

        return entry;
    }

    public byte[] getSet() {
        return set;
    }

    public byte[] getKey() {
        return key;
    }

    public Entry getEntry() {
        return entry;
    }

    public double getScore() {
        return score;
    }

    public long getAtTx() {
        return atTx;
    }

    public byte[] getEncodedKey() {
        byte[] encodedKey = Utils.wrapWithPrefix(key, Consts.SET_KEY_PREFIX);

        ByteBuffer zKey = ByteBuffer.allocate(setLenLen+set.length+scoreLen+keyLenLen+encodedKey.length+txIDLen);
        zKey.order(ByteOrder.BIG_ENDIAN);

        zKey.putLong(set.length);
        zKey.put(set);
        zKey.putDouble(score);

        zKey.putLong(encodedKey.length);
        zKey.put(encodedKey);

        zKey.putLong(atTx);

        return Utils.wrapWithPrefix(zKey.array(), Consts.SORTED_SET_KEY_PREFIX);
    }

    public byte[] digestFor(int version) {
        final KV kv = new KV(
                    getEncodedKey(),
                    null,
                    null
        );

        return kv.digestFor(version);
    }
    
}
