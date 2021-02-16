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

import java.util.Base64;

public class TxMetadata {

    public final long id;
    public final byte[] prevAlh;
    public final long ts;
    public final int nEntries;
    public final byte[] eh;
    public final long blTxId;
    public final byte[] blRoot;

    private static final int TS_SIZE = 8;

    public TxMetadata(long id, byte[] prevAlh, long ts, int nEntries,
                      byte[] eh, long blTxId, byte[] blRoot) {
        this.id = id;
        this.prevAlh = prevAlh;
        this.ts = ts;
        this.nEntries = nEntries;
        this.eh = eh;
        this.blTxId = blTxId;
        this.blRoot = blRoot;
    }

    public byte[] alh() {
        byte[] bi = new byte[Consts.TX_ID_SIZE + 2 * Consts.SHA256_SIZE];

        Utils.putUint64(id, bi);
        System.arraycopy(prevAlh, 0, bi, Consts.TX_ID_SIZE, prevAlh.length);

        byte[] bj = new byte[TS_SIZE + 4 + Consts.SHA256_SIZE + Consts.TX_ID_SIZE + Consts.SHA256_SIZE];
        Utils.putUint64(ts, bj);
        Utils.putUint32(nEntries, bj, TS_SIZE);
        System.arraycopy(eh, 0, bj, TS_SIZE + 4, eh.length);
        Utils.putUint64(blTxId, bj, TS_SIZE + 4 + Consts.SHA256_SIZE);
        System.arraycopy(blRoot, 0, bj, TS_SIZE + 4 + Consts.SHA256_SIZE + Consts.TX_ID_SIZE, blRoot.length);
        byte[] innerHash = CryptoUtils.sha256Sum(bj);

        System.arraycopy(innerHash, 0, bi, Consts.TX_ID_SIZE + Consts.SHA256_SIZE, innerHash.length);

        return CryptoUtils.sha256Sum(bi);
    }

    public static TxMetadata valueOf(ImmudbProto.TxMetadata txMd) {
        return new TxMetadata(
                txMd.getId(),
                txMd.getPrevAlh().toByteArray(),
                txMd.getTs(),
                txMd.getNentries(),
                txMd.getEH().toByteArray(),
                txMd.getBlTxId(),
                txMd.getBlRoot().toByteArray()
        );
    }

    @Override
    public String toString() {
        Base64.Encoder enc = Base64.getEncoder();
        byte[] alh = alh();
        return "TxMetadata{" +
                "id=" + id +
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
