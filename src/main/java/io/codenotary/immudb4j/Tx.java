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
import io.codenotary.immudb4j.crypto.HTree;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Tx {

    private long id;
    private long timestamp;
    private long blTxId;
    private byte[] blRoot;
    private byte[] prefAlh;

    private int nEntries;
    private List<TxEntry> entries;

    private HTree htree;

    private byte[] alh;
    private byte[] innerHash;


    public Tx(int nEntries, int maxKeyLength) {
        this.nEntries = nEntries;
        // TODO: to be continued, see embedded/store/tx.go:55
    }

    static Tx valueOf(ImmudbProto.Tx tx) {

        ImmudbProto.TxMetadata txMd = tx.getMetadata();
        List<String> keys = new ArrayList<>(tx.getEntriesCount());
        tx.getEntriesList().forEach(txEntry -> keys.add(txEntry.getKey().toString(StandardCharsets.UTF_8)));
        //return new Tx(txMd.getId(), txMd.getTs(), keys);
        return null;
    }

}
