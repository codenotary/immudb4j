package io.codenotary.immudb4j;

import io.codenotary.immudb.ImmudbProto;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Transaction {

    public final long id;
    public final long timestamp;
    public final List<String> keys;

    private Transaction(long id, long timestamp, List<String> keys) {
        this.id = id;
        this.timestamp = timestamp;
        this.keys = keys;
    }

    static Transaction valueOf(ImmudbProto.Tx tx) {

        ImmudbProto.TxMetadata txMd = tx.getMetadata();
        List<String> keys = new ArrayList<>(tx.getEntriesCount());
        tx.getEntriesList().forEach(txEntry -> keys.add(txEntry.getKey().toString(StandardCharsets.UTF_8)));
        return new Transaction(txMd.getId(), txMd.getTs(), keys);
    }

}
