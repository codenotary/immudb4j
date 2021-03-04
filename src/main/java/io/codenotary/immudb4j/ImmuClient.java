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

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.codenotary.immudb.ImmuServiceGrpc;
import io.codenotary.immudb.ImmudbProto;
import io.codenotary.immudb.ImmudbProto.ScanRequest;
import io.codenotary.immudb4j.basics.Pair;
import io.codenotary.immudb4j.basics.Triple;
import io.codenotary.immudb4j.crypto.CryptoUtils;
import io.codenotary.immudb4j.crypto.DualProof;
import io.codenotary.immudb4j.crypto.InclusionProof;
import io.codenotary.immudb4j.exceptions.CorruptedDataException;
import io.codenotary.immudb4j.exceptions.MaxWidthExceededException;
import io.codenotary.immudb4j.exceptions.VerificationException;
import io.codenotary.immudb4j.user.Permission;
import io.codenotary.immudb4j.user.User;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The official immudb Java Client.
 *
 * @author Jeronimo Irazabal
 * @author Marius Ileana
 */
public class ImmuClient {

    private static final String AUTH_HEADER = "authorization";
    private final ImmuServiceGrpc.ImmuServiceBlockingStub stub;
    private final boolean withAuth;
    private final ImmuStateHolder stateHolder;
    private ManagedChannel channel;
    private String authToken;
    private String currentServerUuid;
    private String currentDb = "defaultdb";
    private PublicKey serverSigningKey;

    public ImmuClient(Builder builder) {
        this.stub = createStubFrom(builder);
        this.withAuth = builder.isWithAuth();
        this.stateHolder = builder.getStateHolder();
        this.serverSigningKey = builder.getServerSigningKey();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private ImmuServiceGrpc.ImmuServiceBlockingStub createStubFrom(Builder builder) {
        channel = ManagedChannelBuilder.forAddress(builder.getServerUrl(), builder.getServerPort())
                .usePlaintext()
                .intercept(new ImmuServerUUIDInterceptor(this))
                .build();
        return ImmuServiceGrpc.newBlockingStub(channel);
    }

    private boolean checkSignature(ImmudbProto.ImmutableState state) {
        if (serverSigningKey != null) {
            return ImmuState.valueOf(state).checkSignature(serverSigningKey);
        }
        return true;
    }

    // ---------------------------------------------------------------------
    // These two currentServerUuid related methods are not publicly exposed,
    // since these should be called by the ImmuServerUUIDInterceptor only.

    void setCurrentServerUuid(String serverUuid) {
        currentServerUuid = serverUuid;
    }

    String getCurrentServerUuid() {
        return currentServerUuid;
    }
    // ---------------------------------------------------------------------

    public synchronized void shutdown() {
        if (channel == null) {
            return;
        }
        channel.shutdown();
        if (!channel.isShutdown()) {
            try {
                channel.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // nothing to do here.
            }
        }
        channel = null;
    }

    public synchronized boolean isShutdown() {
        return channel == null;
    }

    private ImmuServiceGrpc.ImmuServiceBlockingStub getStub() {
        if (!withAuth || authToken == null) {
            return stub;
        }
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(AUTH_HEADER, Metadata.ASCII_STRING_MARSHALLER), "Bearer " + authToken);

        return MetadataUtils.attachHeaders(stub, metadata);
    }

    public synchronized void login(String username, String password) {
        ImmudbProto.LoginRequest loginRequest = ImmudbProto.LoginRequest
                .newBuilder()
                .setUser(ByteString.copyFrom(username, StandardCharsets.UTF_8))
                .setPassword(ByteString.copyFrom(password, StandardCharsets.UTF_8))
                .build();

        ImmudbProto.LoginResponse loginResponse = getStub().login(loginRequest);
        authToken = loginResponse.getToken();
    }

    public synchronized void logout() {
        // noinspection ResultOfMethodCallIgnored
        getStub().logout(com.google.protobuf.Empty.getDefaultInstance());
        authToken = null;
    }

    /**
     * Get the locally saved state of the current database.
     * If nothing exists already, it is fetched from the server and save it locally.
     */
    public ImmuState state() {
        ImmuState state = stateHolder.getState(currentServerUuid, currentDb);
        if (state == null) {
            state = currentState();
            stateHolder.setState(currentServerUuid, state);
        }
        return state;
    }

    /**
     * Get the current database state that exists on the server.
     * It may throw a RuntimeException if server's state signature verification fails
     * (if this feature is enabled on the client side, at least).
     */
    public ImmuState currentState() {
        Empty empty = com.google.protobuf.Empty.getDefaultInstance();
        ImmudbProto.ImmutableState state = getStub().currentState(empty);

        boolean verifPassed;
        try {
            verifPassed = checkSignature(state);
        } catch (Exception e) {
            throw new RuntimeException("Could not perform the state signature verification. Reason: " + e.getMessage());
        }
        if (!verifPassed) {
            throw new RuntimeException("State signature verification failed");
        }

        return ImmuState.valueOf(state);
    }

    //
    // ========== DATABASE ==========
    //

    public void createDatabase(String database) {
        ImmudbProto.Database db = ImmudbProto.Database.newBuilder().setDatabasename(database).build();
        // noinspection ResultOfMethodCallIgnored
        getStub().createDatabase(db);
    }

    public synchronized void useDatabase(String database) {
        ImmudbProto.Database db = ImmudbProto.Database.newBuilder().setDatabasename(database).build();
        ImmudbProto.UseDatabaseReply response = getStub().useDatabase(db);
        authToken = response.getToken();
        currentDb = database;
    }

    public List<String> databases() {
        Empty empty = com.google.protobuf.Empty.getDefaultInstance();
        ImmudbProto.DatabaseListResponse res = getStub().databaseList(empty);
        List<String> list = new ArrayList<>(res.getDatabasesCount());
        for (ImmudbProto.Database db : res.getDatabasesList()) {
            list.add(db.getDatabasename());
        }
        return list;
    }


    //
    // ========== EXECALL ==========
    //

    /**
     * execAll can be used to submit in one call multiple operations like set, setReference, and zAdd.
     * For setting KVs, <code>kvList</code> contains pairs of (key, value), as in the <code>set(key, value)</code> call.
     * For setting references, <code>refList</code> contains pairs of (key, referencedKey), as in the <code>setReference(key, referencedKey)</code> call.
     * For doing <code>zAdd</code>s, <code>zaddList</code> contains triples of (set, score, key), as in the <code>zAdd(set, score, key)</code> call.
     */
    public TxMetadata execAll(
            List<Pair<byte[], byte[]>> kvList,
            List<Pair<byte[], byte[]>> refList,
            List<Triple<String, Double, String>> zaddList) {

        int opsCount = 0;
        opsCount += kvList != null ? kvList.size() : 0;
        opsCount += refList != null ? refList.size() : 0;
        if (opsCount == 0) {
            return null;
        }
        List<ImmudbProto.Op> operations = new ArrayList<>(opsCount);

        if (kvList != null) {
            ImmudbProto.Op.Builder opb = ImmudbProto.Op.newBuilder();
            ImmudbProto.KeyValue.Builder kvb = ImmudbProto.KeyValue.newBuilder();
            kvList.forEach(pair -> {
                opb.setKv(kvb
                        .setKey(ByteString.copyFrom(pair.a))
                        .setValue(ByteString.copyFrom(pair.b))
                        .build()
                );
                operations.add(opb.build());
            });
        }

        if (refList != null) {
            ImmudbProto.Op.Builder opb = ImmudbProto.Op.newBuilder();
            refList.forEach(pair -> {
                opb.setRef(ImmudbProto.ReferenceRequest.newBuilder()
                        .setKey(ByteString.copyFrom(pair.a))
                        .setReferencedKey(ByteString.copyFrom(pair.b))
                        .build()
                );
                operations.add(opb.build());
            });
        }

        if (zaddList != null) {
            ImmudbProto.Op.Builder opb = ImmudbProto.Op.newBuilder();
            zaddList.forEach(triple -> {
                ImmudbProto.ZAddRequest req = opb.getZAddBuilder()
                        .setSet(ByteString.copyFrom(triple.a, StandardCharsets.UTF_8))
                        .setScore(triple.b)
                        .setKey(ByteString.copyFrom(triple.c, StandardCharsets.UTF_8))
                        .build();
                opb.setZAdd(req);
                operations.add(opb.build());
            });
        }

        ImmudbProto.ExecAllRequest.Builder reqb = ImmudbProto.ExecAllRequest.newBuilder();
        operations.forEach(reqb::addOperations);

        return TxMetadata.valueOf(getStub().execAll(reqb.build()));
    }

    //
    // ========== GET ==========
    //

    public byte[] get(String key) throws Exception {
        return get(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] get(byte[] key) throws Exception {
        ImmudbProto.KeyRequest req = ImmudbProto.KeyRequest.newBuilder().setKey(ByteString.copyFrom(key)).build();
        ImmudbProto.Entry entry;
        try {
            entry = getStub().get(req);
        } catch (StatusRuntimeException e) {
            throw new Exception(e.getMessage(), e);
        }
        return entry.getValue().toByteArray();
    }

    public List<KV> getAll(List<String> keys) {
        List<ByteString> keysBS = new ArrayList<>(keys.size());
        for (String key : keys) {
            keysBS.add(ByteString.copyFrom(key, StandardCharsets.UTF_8));
        }
        return getAllBS(keysBS);
    }

    private List<KV> getAllBS(List<ByteString> keys) {
        ImmudbProto.KeyListRequest req = ImmudbProto.KeyListRequest.newBuilder().addAllKeys(keys).build();
        ImmudbProto.Entries entries = getStub().getAll(req);
        List<KV> result = new ArrayList<>(entries.getEntriesCount());
        for (ImmudbProto.Entry entry : entries.getEntriesList()) {
            result.add(KVPair.from(entry));
        }
        return result;
    }

    public KV getAt(byte[] key, long txId) {
        ImmudbProto.Entry entry = getStub().get(
                ImmudbProto.KeyRequest.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .setAtTx(txId)
                        .build());
        return KVPair.from(entry);
    }

    public KV getSince(byte[] key, long txId) {
        ImmudbProto.Entry entry = getStub().get(
                ImmudbProto.KeyRequest.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .setSinceTx(txId)
                        .build());
        return KVPair.from(entry);
    }

    /**
     * @deprecated This method is deprecated and it will be removed in the next release.
     * Please use verifiedGet instead.
     */
    public byte[] safeGet(String key) throws VerificationException {
        return safeGet(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @deprecated This method is deprecated and it will be removed in the next release.
     * Please use verifiedGet instead.
     */
    public byte[] safeGet(byte[] key) throws VerificationException {
        Entry entry = verifiedGet(key);
        return entry.kv.getValue();
    }

    public Entry verifiedGet(String key) throws VerificationException {
        return verifiedGet(key.getBytes(StandardCharsets.UTF_8));
    }

    public Entry verifiedGet(byte[] key) throws VerificationException {

        ImmuState state = state();
        ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .build();
        return verifiedGet(keyReq, state);
    }

    private Entry verifiedGet(ImmudbProto.KeyRequest keyReq, ImmuState state) throws VerificationException {

        ImmudbProto.VerifiableGetRequest vGetReq = ImmudbProto.VerifiableGetRequest.newBuilder()
                .setKeyRequest(keyReq)
                .setProveSinceTx(state.txId)
                .build();
        ImmudbProto.VerifiableEntry vEntry = getStub().verifiableGet(vGetReq);
        InclusionProof inclusionProof = InclusionProof.valueOf(vEntry.getInclusionProof());
        DualProof dualProof = DualProof.valueOf(vEntry.getVerifiableTx().getDualProof());

        byte[] eh;
        long sourceId, targetId;
        byte[] sourceAlh;
        byte[] targetAlh;
        long vTx;
        KV kv;

        ImmudbProto.Entry entry = vEntry.getEntry();

        if (!entry.hasReferencedBy()) {
            vTx = entry.getTx();
            kv = CryptoUtils.encodeKV(vGetReq.getKeyRequest().getKey().toByteArray(), entry.getValue().toByteArray());
        } else {
            ImmudbProto.Reference entryRefBy = entry.getReferencedBy();
            vTx = entryRefBy.getTx();
            kv = CryptoUtils.encodeReference(
                    entryRefBy.getKey().toByteArray(),
                    entry.getKey().toByteArray(),
                    entryRefBy.getAtTx());
        }

        if (state.txId <= vTx) {
            byte[] digest = vEntry.getVerifiableTx().getDualProof().getTargetTxMetadata().getEH().toByteArray();
            eh = CryptoUtils.digestFrom(digest);

            sourceId = state.txId;
            sourceAlh = CryptoUtils.digestFrom(state.txHash);
            targetId = vTx;
            targetAlh = dualProof.targetTxMetadata.alh();
        } else {
            byte[] digest = vEntry.getVerifiableTx().getDualProof().getSourceTxMetadata().getEH().toByteArray();
            eh = CryptoUtils.digestFrom(digest);

            sourceId = vTx;
            sourceAlh = dualProof.sourceTxMetadata.alh();
            targetId = state.txId;
            targetAlh = CryptoUtils.digestFrom(state.txHash);
        }

        if (!CryptoUtils.verifyInclusion(inclusionProof, kv, eh)) {
            throw new VerificationException("Inclusion verification failed.");
        }

        if (state.txId > 0) {
            if (!CryptoUtils.verifyDualProof(
                    dualProof,
                    sourceId,
                    targetId,
                    sourceAlh,
                    targetAlh
            )) {
                throw new VerificationException("Dual proof verification failed.");
            }
        }

        ImmuState newState = new ImmuState(
                currentDb,
                targetId,
                targetAlh,
                vEntry.getVerifiableTx().getSignature().toByteArray());

        // TODO: to-be-implemented (see pkg/client/client.go:620, newState.CheckSignature(c.serverSigningPubKey))
        // if (serverSigningPubKey != null) { }

        stateHolder.setState(currentServerUuid, newState);

        return Entry.valueOf(vEntry.getEntry());
    }

    public Entry verifiedGetAt(byte[] key, long txId) throws VerificationException {
        ImmuState state = state();
        ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setAtTx(txId)
                .build();
        return verifiedGet(keyReq, state);
    }

    public Entry verifiedGetSince(byte[] key, long txId) throws VerificationException {
        ImmuState state = state();
        ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setSinceTx(txId)
                .build();
        return verifiedGet(keyReq, state);
    }

    //
    // ========== HISTORY ==========
    //


    public List<KV> history(String key, int limit, long offset, boolean desc) {
        return history(key.getBytes(StandardCharsets.UTF_8), limit, offset, desc);
    }

    public List<KV> history(byte[] key, int limit, long offset, boolean desc) {
        return history(key, limit, offset, desc, 1);
    }

    public List<KV> history(String key, int limit, long offset, boolean desc, long sinceTxId) {
        return history(key.getBytes(StandardCharsets.UTF_8), limit, offset, desc, sinceTxId);
    }

    public List<KV> history(byte[] key, int limit, long offset, boolean desc, long sinceTxId) {
        ImmudbProto.Entries entries;
        try {
            entries = getStub().history(ImmudbProto.HistoryRequest.newBuilder()
                    .setKey(ByteString.copyFrom(key))
                    .setLimit(limit)
                    .setOffset(offset)
                    .setDesc(desc)
                    .setSinceTx(sinceTxId)
                    .build()
            );
        } catch (StatusRuntimeException e) {
            return new ArrayList<>(0);
        }
        return buildList(entries);
    }

    //
    // ========== SCAN ==========
    //

    public List<KV> scan(String prefix) {
        return scan(ByteString.copyFrom(prefix, StandardCharsets.UTF_8).toByteArray());
    }

    public List<KV> scan(String prefix, long sinceTxId, long limit, boolean desc) {
        return scan(ByteString.copyFrom(prefix, StandardCharsets.UTF_8).toByteArray(), sinceTxId, limit, desc);
    }

    public List<KV> scan(String prefix, String seekKey, long sinceTxId, long limit, boolean desc) {
        return scan(
                ByteString.copyFrom(prefix, StandardCharsets.UTF_8).toByteArray(),
                ByteString.copyFrom(seekKey, StandardCharsets.UTF_8).toByteArray(),
                sinceTxId, limit, desc);
    }

    public List<KV> scan(byte[] prefix) {
        ScanRequest req = ScanRequest.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();
        ImmudbProto.Entries entries = getStub().scan(req);
        return buildList(entries);
    }

    public List<KV> scan(byte[] prefix, long sinceTxId, long limit, boolean desc) {
        ScanRequest req = ScanRequest.newBuilder()
                .setPrefix(ByteString.copyFrom(prefix))
                .setLimit(limit)
                .setSinceTx(sinceTxId)
                .setDesc(desc)
                .build();
        ImmudbProto.Entries entries = getStub().scan(req);
        return buildList(entries);
    }

    public List<KV> scan(byte[] prefix, byte[] seekKey, long sinceTxId, long limit, boolean desc) {
        ScanRequest req = ScanRequest.newBuilder()
                .setPrefix(ByteString.copyFrom(prefix))
                .setLimit(limit)
                .setSeekKey(ByteString.copyFrom(seekKey))
                .setSinceTx(sinceTxId)
                .setDesc(desc)
                .build();
        ImmudbProto.Entries entries = getStub().scan(req);
        return buildList(entries);
    }

    //
    // ========== SET ==========
    //

    public TxMetadata set(String key, byte[] value) throws CorruptedDataException {
        return set(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public TxMetadata set(byte[] key, byte[] value) throws CorruptedDataException {
        ImmudbProto.KeyValue kv = ImmudbProto.KeyValue
                .newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build();
        ImmudbProto.SetRequest req = ImmudbProto.SetRequest.newBuilder().addKVs(kv).build();
        ImmudbProto.TxMetadata txMd = getStub().set(req);
        if (txMd.getNentries() != 1) {
            throw new CorruptedDataException();
        }
        return TxMetadata.valueOf(txMd);
    }

    public TxMetadata setAll(KVList kvList) throws CorruptedDataException {
        ImmudbProto.SetRequest.Builder reqBuilder = ImmudbProto.SetRequest.newBuilder();
        for (KV kv : kvList.entries()) {
            ImmudbProto.KeyValue schemaKV = ImmudbProto.KeyValue
                    .newBuilder()
                    .setKey(ByteString.copyFrom(kv.getKey()))
                    .setValue(ByteString.copyFrom(kv.getValue()))
                    .build();
            reqBuilder.addKVs(schemaKV);
        }
        ImmudbProto.TxMetadata txMd = getStub().set(reqBuilder.build());
        if (txMd.getNentries() != kvList.entries().size()) {
            throw new CorruptedDataException();
        }
        return TxMetadata.valueOf(txMd);
    }

    public TxMetadata setReference(byte[] key, byte[] referencedKey) throws CorruptedDataException {
        return setReferenceAt(key, referencedKey, 0);
    }

    public TxMetadata setReferenceAt(byte[] key, byte[] referencedKey, long atTx) throws CorruptedDataException {
        ImmudbProto.ReferenceRequest req = ImmudbProto.ReferenceRequest.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setReferencedKey(ByteString.copyFrom(referencedKey))
                .setAtTx(atTx)
                .setBoundRef(atTx > 0)
                .build();
        ImmudbProto.TxMetadata txMd = getStub().setReference(req);
        if (txMd.getNentries() != 1) {
            throw new CorruptedDataException();
        }
        return TxMetadata.valueOf(txMd);
    }

    /**
     * @deprecated This method is deprecated and it will be removed in the next release.
     * Please use verifiedSet instead.
     */
    public void safeSet(String key, byte[] value) throws VerificationException {
        safeSet(key.getBytes(StandardCharsets.UTF_8), value);
    }

    /**
     * @deprecated This method is deprecated and it will be removed in the next release.
     * Please use verifiedSet instead.
     */
    public void safeSet(byte[] key, byte[] value) throws VerificationException {
        verifiedSet(key, value);
    }

    public TxMetadata verifiedSet(String key, byte[] value) throws VerificationException {
        return verifiedSet(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public TxMetadata verifiedSet(byte[] key, byte[] value) throws VerificationException {

        ImmuState state = state();
        ImmudbProto.KeyValue kv = ImmudbProto.KeyValue.newBuilder().setKey(ByteString.copyFrom(key)).setValue(ByteString.copyFrom(value)).build();
        ImmudbProto.VerifiableSetRequest vSetReq = ImmudbProto.VerifiableSetRequest.newBuilder()
                .setSetRequest(ImmudbProto.SetRequest.newBuilder().addKVs(kv).build())
                .setProveSinceTx(state.txId)
                .build();
        ImmudbProto.VerifiableTx vtx = getStub().verifiableSet(vSetReq);
        int ne = vtx.getTx().getMetadata().getNentries();
        if (ne != 1) {
            throw new VerificationException(
                    String.format("Got back %d entries (in tx metadata) instead of 1.", ne)
            );
        }
        Tx tx;
        InclusionProof inclusionProof;
        try {
            tx = Tx.valueOf(vtx.getTx());
        } catch (Exception e) {
            throw new VerificationException("Failed to extract the transaction.", e);
        }

        try {
            inclusionProof = tx.proof(CryptoUtils.encodeKey(key));
        } catch (NoSuchElementException | IllegalArgumentException e) {
            throw new VerificationException("Failed to create the inclusion proof.", e);
        }

        if (!CryptoUtils.verifyInclusion(inclusionProof, CryptoUtils.encodeKV(key, value), tx.eh())) {
            throw new VerificationException("Data is corrupted (verify inclusion failed)");
        }

        ImmuState newState = verifyDualProof(vtx, tx, state);

        // TODO: to-be-implemented (see pkg/client/client.go:803 newState.CheckSignature ...)
        // if (serverSigningPubKey != null) { ... }

        stateHolder.setState(currentServerUuid, newState);

        return TxMetadata.valueOf(vtx.getTx().getMetadata());
    }


    public TxMetadata verifiedSetReference(byte[] key, byte[] referencedKey) throws VerificationException {
        return verifiedSetReferenceAt(key, referencedKey, 0);
    }

    public TxMetadata verifiedSetReferenceAt(byte[] key, byte[] referencedKey, long atTx) throws VerificationException {

        ImmuState state = state();
        ImmudbProto.ReferenceRequest refReq = ImmudbProto.ReferenceRequest.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setReferencedKey(ByteString.copyFrom(referencedKey))
                .setAtTx(atTx)
                .setBoundRef(atTx > 0)
                .build();
        ImmudbProto.VerifiableReferenceRequest vRefReq = ImmudbProto.VerifiableReferenceRequest.newBuilder()
                .setReferenceRequest(refReq)
                .setProveSinceTx(state.txId)
                .build();
        ImmudbProto.VerifiableTx vtx = getStub().verifiableSetReference(vRefReq);
        int vtxNentries = vtx.getTx().getMetadata().getNentries();
        if (vtxNentries != 1) {
            throw new VerificationException(String.format("Data is corrupted (verifTx has %d Nentries instead of 1).",
                    vtxNentries));
        }
        Tx tx;
        try {
            tx = Tx.valueOf(vtx.getTx());
        } catch (NoSuchAlgorithmException e) {
            throw new VerificationException("No such algorithm error.", e);
        } catch (MaxWidthExceededException e) {
            throw new VerificationException("Max width exceeded.", e);
        }

        InclusionProof inclusionProof = tx.proof(CryptoUtils.encodeKey(key));
        if (!CryptoUtils.verifyInclusion(inclusionProof, CryptoUtils.encodeReference(key, referencedKey, atTx), tx.eh())) {
            throw new VerificationException("Data is corrupted (inclusion verification failed).");
        }

        if (Arrays.equals(tx.eh(), CryptoUtils.digestFrom(vtx.getDualProof().getTargetTxMetadata().getEH().toByteArray()))) {
            throw new VerificationException("Data is corrupted (different digests).");
        }

        ImmuState newState = verifyDualProof(vtx, tx, state);

        // TODO: to-be-implemented (see pkg/client/client.go:1122 newState.CheckSignature ...)
        // if (serverSigningPubKey != null) { ... }

        stateHolder.setState(currentServerUuid, newState);

        return TxMetadata.valueOf(vtx.getTx().getMetadata());
    }

    private ImmuState verifyDualProof(ImmudbProto.VerifiableTx vtx, Tx tx, ImmuState state) throws VerificationException {
        long sourceId = state.txId;
        long targetId = tx.getId();
        byte[] sourceAlh = CryptoUtils.digestFrom(state.txHash);
        byte[] targetAlh = tx.getAlh();

        if (state.txId > 0) {
            if (!CryptoUtils.verifyDualProof(
                    DualProof.valueOf(vtx.getDualProof()),
                    sourceId,
                    targetId,
                    sourceAlh,
                    targetAlh
            )) {
                throw new VerificationException("Data is corrupted (dual proof verification failed).");
            }
        }
        // If all good, return the new state to be locally stored.
        return new ImmuState(currentDb, targetId, targetAlh, vtx.getSignature().getSignature().toByteArray());
    }

    //
    // ========== Z ==========
    //

    public TxMetadata zAdd(String set, double score, String key) throws CorruptedDataException {
        return zAddAt(set, score, key, 0);
    }

    public TxMetadata zAddAt(String set, double score, String key, long atTxId)
            throws CorruptedDataException {
        ImmudbProto.TxMetadata txMd = getStub().zAdd(
                ImmudbProto.ZAddRequest.newBuilder()
                        .setSet(ByteString.copyFrom(set, StandardCharsets.UTF_8))
                        .setKey(ByteString.copyFrom(key, StandardCharsets.UTF_8))
                        .setScore(score)
                        .setAtTx(atTxId)
                        .setBoundRef(atTxId > 0)
                        .build()
        );
        if (txMd.getNentries() != 1) {
            throw new CorruptedDataException();
        }
        return TxMetadata.valueOf(txMd);
    }


    public TxMetadata verifiedZAdd(String set, double score, String key) throws VerificationException {
        return verifiedZAddAt(set.getBytes(StandardCharsets.UTF_8), score, key.getBytes(StandardCharsets.UTF_8), 0);
    }

    public TxMetadata verifiedZAdd(byte[] set, double score, byte[] key) throws VerificationException {
        return verifiedZAddAt(set, score, key, 0);
    }

    public TxMetadata verifiedZAddAt(String set, double score, String key, long atTx) throws VerificationException {
        return verifiedZAddAt(set.getBytes(StandardCharsets.UTF_8), score, key.getBytes(StandardCharsets.UTF_8), atTx);
    }

    public TxMetadata verifiedZAddAt(byte[] set, double score, byte[] key, long atTx) throws VerificationException {

        ImmuState state = state();
        ImmudbProto.ZAddRequest zAddReq = ImmudbProto.ZAddRequest.newBuilder()
                .setSet(ByteString.copyFrom(set))
                .setScore(score)
                .setKey(ByteString.copyFrom(key))
                .setAtTx(atTx)
                .build();
        ImmudbProto.VerifiableZAddRequest vZAddReq = ImmudbProto.VerifiableZAddRequest.newBuilder()
                .setZAddRequest(zAddReq)
                .setProveSinceTx(state.txId)
                .build();
        ImmudbProto.VerifiableTx vtx = getStub().verifiableZAdd(vZAddReq);

        if (vtx.getTx().getMetadata().getNentries() != 1) {
            throw new VerificationException("Data is corrupted.");
        }

        Tx tx;
        try {
            tx = Tx.valueOf(vtx.getTx());
        } catch (NoSuchAlgorithmException | MaxWidthExceededException e) {
            throw new VerificationException("Failed to extract the transaction.", e);
        }

        KV ekv = CryptoUtils.encodeZAdd(set, score, CryptoUtils.encodeKey(key), atTx);

        InclusionProof inclusionProof = tx.proof(ekv.getKey());

        if (!CryptoUtils.verifyInclusion(inclusionProof, ekv, tx.eh())) {
            throw new VerificationException("Data is corrupted (inclusion verification failed).");
        }

        if (!Arrays.equals(tx.eh(), CryptoUtils.digestFrom(vtx.getDualProof().getTargetTxMetadata().getEH().toByteArray()))) {
            throw new VerificationException("Data is corrupted (different digests).");
        }

        ImmuState newState = verifyDualProof(vtx, tx, state);

        // TODO: to-be-implemented (see pkg/client/client.go:803 newState.CheckSignature ...)
        // if (serverSigningPubKey != null) { ... }

        stateHolder.setState(currentServerUuid, newState);

        return TxMetadata.valueOf(vtx.getTx().getMetadata());
    }

    public List<KV> zScan(String set, long limit, boolean reverse) {
        return zScan(set.getBytes(StandardCharsets.UTF_8), 1, limit, reverse);
    }

    public List<KV> zScan(String set, long sinceTxId, long limit, boolean reverse) {
        return zScan(set.getBytes(StandardCharsets.UTF_8), sinceTxId, limit, reverse);
    }

    public List<KV> zScan(byte[] set, long limit, boolean reverse) {
        return zScan(set, 1, limit, reverse);
    }

    public List<KV> zScan(byte[] set, long sinceTxId, long limit, boolean reverse) {
        ImmudbProto.ZScanRequest req = ImmudbProto.ZScanRequest
                .newBuilder()
                .setSet(ByteString.copyFrom(set))
                .setLimit(limit)
                .setSinceTx(sinceTxId)
                .setDesc(reverse)
                .build();
        ImmudbProto.ZEntries zEntries = getStub().zScan(req);
        return buildList(zEntries);
    }

    //
    // ========== TX ==========
    //

    public Tx txById(long txId) throws MaxWidthExceededException, NoSuchAlgorithmException {
        ImmudbProto.Tx tx = getStub().txById(ImmudbProto.TxRequest.newBuilder().setTx(txId).build());
        return Tx.valueOf(tx);
    }

    public Tx verifiedTxById(long txId) throws VerificationException {

        ImmuState state = state();
        ImmudbProto.VerifiableTxRequest vTxReq = ImmudbProto.VerifiableTxRequest.newBuilder()
                .setTx(txId)
                .setProveSinceTx(state.txId)
                .build();
        ImmudbProto.VerifiableTx vtx = getStub().verifiableTxById(vTxReq);

        DualProof dualProof = DualProof.valueOf(vtx.getDualProof());

        long sourceId;
        long targetId;
        byte[] sourceAlh;
        byte[] targetAlh;

        if (state.txId <= vtx.getTx().getMetadata().getId()) {
            sourceId = state.txId;
            sourceAlh = CryptoUtils.digestFrom(state.txHash);
            targetId = vtx.getTx().getMetadata().getId();
            targetAlh = dualProof.targetTxMetadata.alh();
        } else {
            sourceId = vtx.getTx().getMetadata().getId();
            sourceAlh = dualProof.sourceTxMetadata.alh();
            targetId = state.txId;
            targetAlh = CryptoUtils.digestFrom(state.txHash);
        }

        if (state.txId > 0) {
            if (!CryptoUtils.verifyDualProof(
                    DualProof.valueOf(vtx.getDualProof()),
                    sourceId,
                    targetId,
                    sourceAlh,
                    targetAlh
            )) {
                throw new VerificationException("Data is corrupted (dual proof verification failed).");
            }
        }

        ImmuState newState = new ImmuState(currentDb, targetId, targetAlh, vtx.getSignature().getSignature().toByteArray());

        // TODO: to-be-implemented (see pkg/client/client.go:803 newState.CheckSignature ...)
        // if (serverSigningPubKey != null) { ... }

        stateHolder.setState(currentServerUuid, newState);

        Tx tx = null;
        try {
            tx = Tx.valueOfWithDecodedEntries(vtx.getTx());
        } catch (NoSuchAlgorithmException | MaxWidthExceededException e) {
            throw new VerificationException("Failed to extract the transaction.", e);
        }
        return tx;
    }

    public List<Tx> txScan(long initialTxId) {
        ImmudbProto.TxScanRequest req = ImmudbProto.TxScanRequest.newBuilder().setInitialTx(initialTxId).build();
        ImmudbProto.TxList txList = getStub().txScan(req);
        return buildList(txList);
    }

    public List<Tx> txScan(long initialTxId, int limit, boolean desc) {
        ImmudbProto.TxScanRequest req = ImmudbProto.TxScanRequest
                .newBuilder()
                .setInitialTx(initialTxId)
                .setLimit(limit)
                .setDesc(desc)
                .build();
        ImmudbProto.TxList txList = getStub().txScan(req);
        return buildList(txList);
    }

    //
    // ========== COUNT ==========
    // Temporary disabled (for the sake of code coverage) since it's not yet implemented on the server (immudb) side.
    //

//    public long count(String prefix) {
//        return count(prefix.getBytes(StandardCharsets.UTF_8));
//    }
//
//    public long count(byte[] prefix) {
//        return getStub()
//                .count(ImmudbProto.KeyPrefix.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build())
//                .getCount();
//    }
//
//    public long countAll() {
//        return getStub().countAll(Empty.getDefaultInstance()).getCount();
//    }

    //
    // ========== HEALTH ==========
    //

    public boolean healthCheck() {
        return getStub().health(Empty.getDefaultInstance()).getStatus();
    }

    public boolean isConnected() {
        return channel != null;
    }

    //
    // ========== USER MGMT ==========
    //

    public List<User> listUsers() {
        ImmudbProto.UserList userList = getStub().listUsers(Empty.getDefaultInstance());

        return userList.getUsersList()
                .stream()
                .map(u -> User.getBuilder()
                        .setUser(u.getUser().toString(StandardCharsets.UTF_8))
                        .setActive(u.getActive())
                        .setCreatedAt(u.getCreatedat())
                        .setCreatedBy(u.getCreatedby())
                        .setPermissions(buildPermissions(u.getPermissionsList()))
                        .build()
                )
                .collect(Collectors.toList());
    }

    private List<Permission> buildPermissions(List<ImmudbProto.Permission> permissionsList) {
        return permissionsList
                .stream()
                .map(p -> Permission.valueOfPermissionCode(p.getPermission()))
                .collect(Collectors.toList());
    }

    public void createUser(String user, String password, Permission permission, String database) {
        ImmudbProto.CreateUserRequest createUserRequest = ImmudbProto.CreateUserRequest.newBuilder()
                .setUser(ByteString.copyFrom(user, StandardCharsets.UTF_8))
                .setPassword(ByteString.copyFrom(password, StandardCharsets.UTF_8))
                .setPermission(permission.permissionCode)
                .setDatabase(database)
                .build();

        // noinspection ResultOfMethodCallIgnored
        getStub().createUser(createUserRequest);
    }

    public void changePassword(String user, String oldPassword, String newPassword) {
        ImmudbProto.ChangePasswordRequest changePasswordRequest = ImmudbProto.ChangePasswordRequest.newBuilder()
                .setUser(ByteString.copyFrom(user, StandardCharsets.UTF_8))
                .setOldPassword(ByteString.copyFrom(oldPassword, StandardCharsets.UTF_8))
                .setNewPassword(ByteString.copyFrom(newPassword, StandardCharsets.UTF_8))
                .build();

        // noinspection ResultOfMethodCallIgnored
        getStub().changePassword(changePasswordRequest);
    }

    //
    // ========== USER MGMT ==========
    //

    public void cleanIndex() {
        //noinspection ResultOfMethodCallIgnored
        getStub().cleanIndex(Empty.getDefaultInstance());
    }

    //
    // ========== INTERNAL UTILS ==========
    //

    private List<KV> buildList(ImmudbProto.Entries entries) {
        List<KV> result = new ArrayList<>(entries.getEntriesCount());
        entries.getEntriesList()
                .forEach(entry -> result.add(KVPair.from(entry)));
        return result;
    }

    private List<KV> buildList(ImmudbProto.ZEntries entries) {
        List<KV> result = new ArrayList<>(entries.getEntriesCount());
        entries.getEntriesList()
                .forEach(entry -> result.add(KVPair.from(entry)));
        return result;
    }

    private List<Tx> buildList(ImmudbProto.TxList txList) {
        List<Tx> result = new ArrayList<>(txList.getTxsCount());
        txList.getTxsList().forEach(tx -> {
            try {
                result.add(Tx.valueOf(tx));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        return result;
    }

    //
    // ========== BUILDER ==========
    //

    public static class Builder {

        private String serverUrl;

        private int serverPort;

        private PublicKey serverSigningKey;

        private boolean withAuth;

        private ImmuStateHolder stateHolder;

        private Builder() {
            this.serverUrl = "localhost";
            this.serverPort = 3322;
            this.stateHolder = new SerializableImmuStateHolder();
            this.withAuth = true;
        }

        public ImmuClient build() {
            return new ImmuClient(this);
        }

        public String getServerUrl() {
            return this.serverUrl;
        }

        public Builder withServerUrl(String serverUrl) {
            this.serverUrl = serverUrl;
            return this;
        }

        public int getServerPort() {
            return serverPort;
        }

        public Builder withServerPort(int serverPort) {
            this.serverPort = serverPort;
            return this;
        }

        public PublicKey getServerSigningKey() {
            return serverSigningKey;
        }

        /**
         * Specify the public key file name (a DER file) that will
         * be used for verifying the state signature received from the server.
         */
        public Builder withServerSigningKey(String publicKeyFilename) throws Exception {
            this.serverSigningKey = CryptoUtils.getDERPublicKey(publicKeyFilename);
            return this;
        }

        public boolean isWithAuth() {
            return withAuth;
        }

        public Builder withAuth(boolean withAuth) {
            this.withAuth = withAuth;
            return this;
        }

        public ImmuStateHolder getStateHolder() {
            return stateHolder;
        }

        public Builder withStateHolder(ImmuStateHolder stateHolder) {
            this.stateHolder = stateHolder;
            return this;
        }

    }

}
