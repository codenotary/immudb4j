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

import io.codenotary.immudb.ImmudbProto;
import io.codenotary.immudb4j.crypto.CryptoUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;

public class TxHeader {

    private final int version;
    public  final long id;
    public  final byte[] prevAlh;
    public  final long ts;
    public  final int nEntries;
    public  final byte[] eh;
    public  final long blTxId;
    public  final byte[] blRoot;

    private static final int TS_SIZE = 8;
    private static final int SHORT_SSIZE = 2;
    private static final int LONG_SSIZE = 4;

    private static final int maxTxMetadataLen = 0;

    private TxHeader(int version, long id, byte[] prevAlh, long ts, int nEntries,
                      byte[] eh, long blTxId, byte[] blRoot) {
        this.version = version;
        this.id = id;
        this.prevAlh = prevAlh;
        this.ts = ts;
        this.nEntries = nEntries;
        this.eh = eh;
        this.blTxId = blTxId;
        this.blRoot = blRoot;
    }

    public static TxHeader valueOf(ImmudbProto.TxHeader hdr) {
        return new TxHeader(
                hdr.getVersion(),
                hdr.getId(),
                hdr.getPrevAlh().toByteArray(),
                hdr.getTs(),
                hdr.getNentries(),
                hdr.getEH().toByteArray(),
                hdr.getBlTxId(),
                hdr.getBlRoot().toByteArray()
        );
    }

    public int getVersion() {
        return version;
    }

    public long getId() {
        return id;
    }

    public byte[] getEh() {
        return eh;
    }

    public byte[] alh() {
        // txID + prevAlh + innerHash
        ByteBuffer bytes = ByteBuffer.allocate(Consts.TX_ID_SIZE + 2 * Consts.SHA256_SIZE);

        bytes.putLong(id);
        bytes.put(prevAlh);
        bytes.put(innerHash());

        return CryptoUtils.sha256Sum(bytes.array());
    }

    private byte[] innerHash() {
        // ts + version + (mdLen + md)? + nentries + eH + blTxID + blRoot
        ByteBuffer bytes = ByteBuffer.allocate(TS_SIZE +
                SHORT_SSIZE + (SHORT_SSIZE + maxTxMetadataLen) +
                LONG_SSIZE + Consts.SHA256_SIZE +
                Consts.TX_ID_SIZE + Consts.SHA256_SIZE);

        bytes.order(ByteOrder.BIG_ENDIAN);

        bytes.putLong(ts);
        bytes.putShort((short)version);

        switch (version) {
            case 0: {
                bytes.putShort((short)nEntries);
                break;
            }
            case 1: {
                // TODO: add support for TxMetadata
                int mdLen = 0;
                bytes.putShort((short)mdLen);

                bytes.putInt(nEntries);
                break;
            }
            default: {
                throw new RuntimeException(String.format("missing tx hash calculation method for version %d", version));
            }
        }

        // following records are currently common in versions 0 and 1
        bytes.put(eh);
        bytes.putLong(blTxId);
        bytes.put(blRoot);

        return CryptoUtils.sha256Sum(bytes.array());
    }

    @Override
    public String toString() {
        Base64.Encoder enc = Base64.getEncoder();
        byte[] alh = alh();
        return "TxHeader{" +
                "version=" + version +
                ", id=" + id +
                ", prevAlh=" + enc.encodeToString(prevAlh) +
                ", prevAlh=" + Utils.toString(prevAlh) +
                ", ts=" + ts +
                ", nEntries=" + nEntries +
                ", eh=" + enc.encodeToString(eh) +
                ", eh=" + Utils.toString(eh) +
                ", blTxId=" + blTxId +
                ", blRoot=" + enc.encodeToString(blRoot) +
                ", blRoot=" + Utils.toString(blRoot) +
                ", alh=" + enc.encodeToString(alh) +
                ", alh=" + Utils.toString(alh) +
                '}';
    }

}
