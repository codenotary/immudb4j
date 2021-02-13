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
import java.util.ArrayList;
import java.util.List;

public class TxMetadata {

    private static final int TX_ID_SIZE = 8;

    public final long id;
    public final byte[] prevAlh;
    public final byte[] alh;
    public final long timestamp;
    public final int nEntries;
    public final byte[] eh;
    public final long blTxId;
    public final byte[] blRoot;

    public TxMetadata(long id, byte[] prevAlh, long timestamp, int nEntries,
                      byte[] eh, long blTxId, byte[] blRoot) {
        this.id = id;
        this.prevAlh = prevAlh;
        this.timestamp = timestamp;
        this.nEntries = nEntries;
        this.eh = eh;
        this.blTxId = blTxId;
        this.blRoot = blRoot;
        this.alh = buildAlh();
    }

    private byte[] buildAlh() {
        byte[] bi = new byte[TX_ID_SIZE + 2 * CryptoUtils.SHA256_SIZE];
        // to be cont'd
        // (embedded/store/tx.go line 93)
        return null;
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

}
