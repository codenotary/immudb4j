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
import io.codenotary.immudb4j.crypto.HTree;
import io.codenotary.immudb4j.crypto.InclusionProof;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

public class Tx {

    private final TxHeader header;
    private final List<TxEntry> entries;
    private final HTree htree;

    private Tx(TxHeader header, List<TxEntry> entries, HTree htree) {
        this.header = header;
        this.entries = entries;
        this.htree = htree;
    }

    public static Tx valueOf(ImmudbProto.Tx stx) throws NoSuchAlgorithmException {
        final TxHeader header = TxHeader.valueOf(stx.getHeader());

        final List<TxEntry> entries = new ArrayList<>(stx.getEntriesCount());

        stx.getEntriesList().forEach(txe -> { entries.add(TxEntry.valueOf(txe)); });

        final  HTree hTree = new HTree(entries.size());

        final Tx tx = new Tx(header, entries, hTree);

        tx.buildHashTree();

        if (!Arrays.equals(tx.header.getEh(), hTree.root())) {
            throw new RuntimeException("corrupted data, eh doesn't match expected value");
        }

        return tx;
    }

    public TxHeader getHeader() {
        return header;
    }

    public List<TxEntry> getEntries() {
        return new ArrayList<>(entries);
    }

    public void buildHashTree() throws NoSuchAlgorithmException {
        byte[][] digests = new byte[entries.size()][Consts.SHA256_SIZE];
        for (int i = 0; i < entries.size(); i++) {
            digests[i] = entries.get(i).digestFor(header.getVersion());
        }
        htree.buildWith(digests);
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
