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
import io.codenotary.immudb4j.crypto.HTree;
import io.codenotary.immudb4j.crypto.InclusionProof;
import io.codenotary.immudb4j.exceptions.MaxWidthExceededException;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

public class Tx {

    private long id;
    private long ts;
    private long blTxId;
    private byte[] blRoot;
    private byte[] prevAlh;

    private final List<TxEntry> entries;

    private HTree htree;

    private byte[] alh;
    private byte[] innerHash;


    private Tx(long id, List<TxEntry> entries, HTree htree) {
        this.id = id;
        this.entries = entries;
        this.htree = htree;
    }

    public Tx(int nEntries, int maxKeyLength) {
        entries = new ArrayList<>(nEntries);
        for (int i = 0; i < nEntries; i++) {
            entries.add(new TxEntry(new byte[maxKeyLength]));
        }
    }

    static Tx valueFrom(List<TxEntry> entries) {
        HTree hTree = new HTree(entries.size());
        return new Tx(0, entries, hTree);
    }

    static Tx valueOf(ImmudbProto.Tx stx) throws NoSuchAlgorithmException, MaxWidthExceededException {

        List<TxEntry> entries = new ArrayList<>(stx.getEntriesCount());
        stx.getEntriesList().forEach(txe -> entries.add(
                new TxEntry(txe.getKey().toByteArray(), txe.getVLen(),
                        CryptoUtils.digestFrom(txe.getHValue().toByteArray()), txe.getVOff())
        ));
        ImmudbProto.TxMetadata stxMd = stx.getMetadata();
        Tx tx = valueFrom(entries);
        tx.id = stxMd.getId();
        tx.prevAlh = CryptoUtils.digestFrom(stxMd.getPrevAlh().toByteArray());
        tx.ts = stxMd.getTs();
        tx.blTxId = stxMd.getBlTxId();
        tx.blRoot = CryptoUtils.digestFrom(stxMd.getBlRoot().toByteArray());

        tx.buildHashTree();
        tx.calcAlh();

        return tx;
    }

    static Tx valueOfWithDecodedEntries(ImmudbProto.Tx stx) throws NoSuchAlgorithmException, MaxWidthExceededException {

        List<TxEntry> txEntries = new ArrayList<>(stx.getEntriesCount());
        stx.getEntriesList().forEach(stxe -> {
            byte[] key = stxe.getKey().toByteArray();
            key = Arrays.copyOfRange(key, 1, key.length);
            txEntries.add(
                    new TxEntry(key,
                            stxe.getVLen(),
                            CryptoUtils.digestFrom(stxe.getHValue().toByteArray()),
                            stxe.getVOff())
            );
        });
        ImmudbProto.TxMetadata stxMd = stx.getMetadata();
        Tx tx = valueFrom(txEntries);
        tx.id = stxMd.getId();
        tx.prevAlh = CryptoUtils.digestFrom(stxMd.getPrevAlh().toByteArray());
        tx.ts = stxMd.getTs();
        tx.blTxId = stxMd.getBlTxId();
        tx.blRoot = CryptoUtils.digestFrom(stxMd.getBlRoot().toByteArray());

        tx.buildHashTree();
        tx.calcAlh();

        return tx;
    }

    public long getId() {
        return id;
    }

    public byte[] getAlh() {
        return alh;
    }

    public byte[] eh() {
        return htree.root();
    }

    public TxMetadata metadata() {
        byte[] prevAlh = new byte[Consts.SHA256_SIZE];
        byte[] blRoot = new byte[Consts.SHA256_SIZE];
        Utils.copy(this.prevAlh, prevAlh);
        Utils.copy(this.blRoot, blRoot);

        return new TxMetadata(id, prevAlh, ts, entries.size(), eh(), blTxId, blRoot);
    }

    public void buildHashTree() throws MaxWidthExceededException, NoSuchAlgorithmException {
        byte[][] digests = new byte[entries.size()][Consts.SHA256_SIZE];
        for (int i = 0; i < entries.size(); i++) {
            digests[i] = entries.get(i).digest();
        }
        htree.buildWith(digests);
    }

    /**
     * Calculate the Accumulative Linear Hash (ALH) up to this transaction.
     * Alh is calculated as hash(txID + prevAlh + hash(ts + nentries + eH + blTxID + blRoot))
     * Inner hash is calculated so to reduce the length of linear proofs.
     */
    public void calcAlh() {
        calcInnerHash();

        byte[] bi = new byte[Consts.TX_ID_SIZE + 2 * Consts.SHA256_SIZE];
        Utils.putUint64(id, bi);
        Utils.copy(prevAlh, bi, Consts.TX_ID_SIZE);
        Utils.copy(innerHash, bi, Consts.TX_ID_SIZE + Consts.SHA256_SIZE);

        alh = CryptoUtils.sha256Sum(bi);
    }

    private void calcInnerHash() {
        byte[] bj = new byte[Consts.TS_SIZE + 4 + Consts.SHA256_SIZE + Consts.TX_ID_SIZE + Consts.SHA256_SIZE];
        Utils.putUint64(ts, bj);
        Utils.putUint32(entries.size(), bj, Consts.TS_SIZE);
        Utils.copy(eh(), bj, Consts.TS_SIZE + 4);
        Utils.putUint64(blTxId, bj, Consts.TS_SIZE + 4 + Consts.SHA256_SIZE);
        Utils.copy(blRoot, bj, Consts.TS_SIZE + 4 + Consts.SHA256_SIZE + Consts.TX_ID_SIZE);

        innerHash = CryptoUtils.sha256Sum(bj);
    }

    public InclusionProof proof(byte[] key) throws NoSuchElementException, IllegalArgumentException {
        int kindex = indexOf(key);
        if (kindex < 0) {
            throw new NoSuchElementException();
        }
        return htree.inclusionProof(kindex);
    }


    private int indexOf(byte[] key) {
        for (int i = 0; i < entries.size(); i++) {
            if (Arrays.equals(entries.get(i).getKey(), key)) {
                return i;
            }
        }
        return -1;
    }

}
