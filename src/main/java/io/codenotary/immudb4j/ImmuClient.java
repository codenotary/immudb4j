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

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.codenotary.immudb.ImmuServiceGrpc;
import io.codenotary.immudb.ImmudbProto;
import io.codenotary.immudb.ImmudbProto.ScanRequest;
import io.codenotary.immudb4j.crypto.CryptoUtils;
import io.codenotary.immudb4j.crypto.DualProof;
import io.codenotary.immudb4j.crypto.InclusionProof;
import io.codenotary.immudb4j.exceptions.*;
import io.codenotary.immudb4j.user.Permission;
import io.codenotary.immudb4j.user.User;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.ConnectivityState;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The official immudb Java Client.
 *
 * @author Jeronimo Irazabal
 * @author Marius Ileana
 */
public class ImmuClient {

    private final PublicKey serverSigningKey;
    private final ImmuStateHolder stateHolder;

    private ManagedChannel channel;

    private final ImmuServiceGrpc.ImmuServiceBlockingStub stub;

    private Session session;

    public ImmuClient(Builder builder) {
        this.stateHolder = builder.getStateHolder();
        this.serverSigningKey = builder.getServerSigningKey();
        this.stub = createStubFrom(builder);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private ImmuServiceGrpc.ImmuServiceBlockingStub createStubFrom(Builder builder) {
        channel = ManagedChannelBuilder.forAddress(builder.getServerUrl(), builder.getServerPort())
                .usePlaintext()
                .intercept(new ImmudbAuthRequestInterceptor(this))
                .build();

        return ImmuServiceGrpc.newBlockingStub(channel);
    }

    public synchronized void shutdown() {
        if (channel == null) {
            return;
        }

        if (session != null) {
            closeSession();
        }

        try {
            channel.shutdown();
            if (!channel.isShutdown()) {
                try {
                    channel.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    channel.shutdownNow();
                }
            }
        } finally {
            channel = null;
        }
    }

    Session getSession() {
        return this.session;
    }

    public synchronized void openSession(String username, String password, String database) {
        if (this.session != null) {
            throw new IllegalStateException("session already opened");
        }

        final ImmudbProto.OpenSessionRequest req = ImmudbProto.OpenSessionRequest
                .newBuilder()
                .setUsername(Utils.toByteString(username))
                .setPassword(Utils.toByteString(password))
                .setDatabaseName(database)
                .build();

        final ImmudbProto.OpenSessionResponse resp = this.stub.openSession(req);

        this.session = new Session(resp.getSessionID(), username, database);
    }

    public synchronized void closeSession() {
        if (this.session == null) {
            throw new IllegalStateException("no open session");
        }

        try {
            this.stub.closeSession(Empty.getDefaultInstance());
        } finally {
            this.session = null;
        }
    }

    /**
     * Get the locally saved state of the current database.
     * If nothing exists already, it is fetched from the server and save it locally.
     */
    private ImmuState state() throws VerificationException {
        if (this.session == null) {
            throw new IllegalStateException("no open session");
        }

        ImmuState state = this.stateHolder.getState(this.session.getDatabase());

        if (state == null) {
            state = this.currentState();
            stateHolder.setState(state);
        }

        return state;
    }

    /**
     * Get the current database state that exists on the server.
     * It may throw VerificationException if server's state signature verification
     * fails
     * (if this feature is enabled on the client side, at least).
     * Note: local state is not updated because this is not a verified operation
     */
    public synchronized ImmuState currentState() throws VerificationException {
        if (this.session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.ImmutableState state = this.stub.currentState(Empty.getDefaultInstance());

        final ImmuState immuState = ImmuState.valueOf(state);

        if (!this.session.getDatabase().equals(immuState.getDatabase())) {
            throw new VerificationException("database mismatch");
        }

        if (!immuState.checkSignature(this.serverSigningKey)) {
            throw new VerificationException("State signature verification failed");
        }

        return immuState;
    }

    //
    // ========== DATABASE ==========
    //

    public synchronized void createDatabase(String database) {
        if (this.session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.CreateDatabaseRequest req = ImmudbProto.CreateDatabaseRequest.newBuilder()
                .setName(database)
                .build();

        this.stub.createDatabaseV2(req);
    }

    public synchronized List<String> databases() {
        if (this.session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.DatabaseListRequestV2 req = ImmudbProto.DatabaseListRequestV2.newBuilder().build();
        final ImmudbProto.DatabaseListResponseV2 resp = this.stub.databaseListV2(req);

        final List<String> list = new ArrayList<>(resp.getDatabasesCount());

        for (ImmudbProto.DatabaseWithSettings db : resp.getDatabasesList()) {
            list.add(db.getName());
        }

        return list;
    }

    //
    // ========== GET ==========
    //

    public Entry get(String key) throws KeyNotFoundException {
        return get(Utils.toByteArray(key));
    }

    public Entry get(byte[] key) throws KeyNotFoundException {
        return getAtTx(key, 0);
    }

    public Entry getAtTx(String key, long tx) throws KeyNotFoundException {
        return getAtTx(Utils.toByteArray(key), tx);
    }

    public synchronized Entry getAtTx(byte[] key, long tx) throws KeyNotFoundException {
        final ImmudbProto.KeyRequest req = ImmudbProto.KeyRequest.newBuilder()
                .setKey(Utils.toByteString(key))
                .setAtTx(tx)
                .build();

        try {
            return Entry.valueOf(this.stub.get(req));
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("key not found")) {
                throw new KeyNotFoundException();
            }

            throw e;
        }
    }

    public Entry getSinceTx(String key, long tx) throws KeyNotFoundException {
        return getSinceTx(Utils.toByteArray(key), tx);
    }

    public synchronized Entry getSinceTx(byte[] key, long tx) throws KeyNotFoundException {
        final ImmudbProto.KeyRequest req = ImmudbProto.KeyRequest.newBuilder()
                .setKey(Utils.toByteString(key))
                .setSinceTx(tx)
                .build();

        try {
            return Entry.valueOf(this.stub.get(req));
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("key not found")) {
                throw new KeyNotFoundException();
            }

            throw e;
        }
    }

    public Entry getAtRevision(String key, long rev) throws KeyNotFoundException {
        return getAtRevision(Utils.toByteArray(key), rev);
    }

    public synchronized Entry getAtRevision(byte[] key, long rev) throws KeyNotFoundException {
        final ImmudbProto.KeyRequest req = ImmudbProto.KeyRequest.newBuilder()
                .setKey(Utils.toByteString(key))
                .setAtRevision(rev)
                .build();

        try {
            return Entry.valueOf(this.stub.get(req));
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("key not found")) {
                throw new KeyNotFoundException();
            }

            throw e;
        }
    }

    public synchronized List<Entry> getAll(List<String> keys) {
        final List<ByteString> keysBS = new ArrayList<>(keys.size());

        for (String key : keys) {
            keysBS.add(Utils.toByteString(key));
        }

        final ImmudbProto.KeyListRequest req = ImmudbProto.KeyListRequest.newBuilder().addAllKeys(keysBS).build();
        final ImmudbProto.Entries entries = this.stub.getAll(req);

        final List<Entry> result = new ArrayList<>(entries.getEntriesCount());

        for (ImmudbProto.Entry entry : entries.getEntriesList()) {
            result.add(Entry.valueOf(entry));
        }

        return result;
    }

    public Entry verifiedGet(String key) throws KeyNotFoundException, VerificationException {
        return verifiedGetAtTx(key, 0);
    }

    public Entry verifiedGet(byte[] key) throws KeyNotFoundException, VerificationException {
        return verifiedGetAtTx(key, 0);
    }

    public Entry verifiedGetAtTx(String key, long tx) throws KeyNotFoundException, VerificationException {
        return verifiedGetAtTx(Utils.toByteArray(key), tx);
    }

    public synchronized Entry verifiedGetAtTx(byte[] key, long tx) throws KeyNotFoundException, VerificationException {
        final ImmuState state = this.state();

        final ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
                .setKey(Utils.toByteString(key))
                .setAtTx(tx)
                .build();

        return verifiedGet(keyReq, state);
    }

    public Entry verifiedGetSinceTx(String key, long tx) throws KeyNotFoundException, VerificationException {
        return verifiedGetSinceTx(Utils.toByteArray(key), tx);
    }

    public synchronized Entry verifiedGetSinceTx(byte[] key, long tx)
            throws KeyNotFoundException, VerificationException {

        final ImmuState state = this.state();

        final ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
                .setKey(Utils.toByteString(key))
                .setSinceTx(tx)
                .build();

        return verifiedGet(keyReq, state);
    }

    public Entry verifiedGetAtRevision(String key, long rev) throws KeyNotFoundException, VerificationException {
        return verifiedGetAtRevision(Utils.toByteArray(key), rev);
    }

    public synchronized Entry verifiedGetAtRevision(byte[] key, long rev)
            throws KeyNotFoundException, VerificationException {

        final ImmuState state = this.state();

        final ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
                .setKey(Utils.toByteString(key))
                .setAtRevision(rev)
                .build();

        return verifiedGet(keyReq, state);
    }

    private Entry verifiedGet(ImmudbProto.KeyRequest keyReq, ImmuState state)
            throws KeyNotFoundException, VerificationException {
        final ImmudbProto.VerifiableGetRequest vGetReq = ImmudbProto.VerifiableGetRequest.newBuilder()
                .setKeyRequest(keyReq)
                .setProveSinceTx(state.getTxId())
                .build();

        final ImmudbProto.VerifiableEntry vEntry;

        try {
            vEntry = this.stub.verifiableGet(vGetReq);
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("key not found")) {
                throw new KeyNotFoundException();
            }

            throw e;
        }

        final InclusionProof inclusionProof = InclusionProof.valueOf(vEntry.getInclusionProof());
        final DualProof dualProof = DualProof.valueOf(vEntry.getVerifiableTx().getDualProof());

        byte[] eh;
        long sourceId, targetId;
        byte[] sourceAlh;
        byte[] targetAlh;

        final Entry entry = Entry.valueOf(vEntry.getEntry());

        if (entry.getReferenceBy() == null && !Arrays.equals(keyReq.getKey().toByteArray(), entry.getKey())) {
            throw new VerificationException("Data is corrupted: entry does not belong to specified key");
        }

        if (entry.getReferenceBy() != null
                && !Arrays.equals(keyReq.getKey().toByteArray(), entry.getReferenceBy().getKey())) {
            throw new VerificationException("Data is corrupted: entry does not belong to specified key");
        }

        if (entry.getMetadata() != null && entry.getMetadata().deleted()) {
            throw new VerificationException("Data is corrupted: entry is marked as deleted");
        }

        if (keyReq.getAtTx() != 0 && entry.getTx() != keyReq.getAtTx()) {
            throw new VerificationException("Data is corrupted: entry does not belong to specified tx");
        }

        if (state.getTxId() <= entry.getTx()) {
            final byte[] digest = vEntry.getVerifiableTx().getDualProof().getTargetTxHeader().getEH().toByteArray();
            eh = CryptoUtils.digestFrom(digest);

            sourceId = state.getTxId();
            sourceAlh = CryptoUtils.digestFrom(state.getTxHash());
            targetId = entry.getTx();
            targetAlh = dualProof.targetTxHeader.alh();
        } else {
            final byte[] digest = vEntry.getVerifiableTx().getDualProof().getSourceTxHeader().getEH().toByteArray();
            eh = CryptoUtils.digestFrom(digest);

            sourceId = entry.getTx();
            sourceAlh = dualProof.sourceTxHeader.alh();
            targetId = state.getTxId();
            targetAlh = CryptoUtils.digestFrom(state.getTxHash());
        }

        final byte[] kvDigest = entry.digestFor(vEntry.getVerifiableTx().getTx().getHeader().getVersion());

        if (!CryptoUtils.verifyInclusion(inclusionProof, kvDigest, eh)) {
            throw new VerificationException("Inclusion verification failed.");
        }

        if (state.getTxId() > 0) {
            if (!CryptoUtils.verifyDualProof(
                    dualProof,
                    sourceId,
                    targetId,
                    sourceAlh,
                    targetAlh)) {
                throw new VerificationException("Dual proof verification failed.");
            }
        }

        final ImmuState newState = new ImmuState(
                this.session.getDatabase(),
                targetId,
                targetAlh,
                vEntry.getVerifiableTx().getSignature().toByteArray());

        if (!newState.checkSignature(serverSigningKey)) {
            throw new VerificationException("State signature verification failed");
        }

        this.stateHolder.setState(newState);

        return Entry.valueOf(vEntry.getEntry());
    }

    //
    // ========== DELETE ==========
    //

    public TxHeader delete(String key) throws KeyNotFoundException {
        return delete(Utils.toByteArray(key));
    }

    public synchronized TxHeader delete(byte[] key) throws KeyNotFoundException {
        try {
            final ImmudbProto.DeleteKeysRequest req = ImmudbProto.DeleteKeysRequest.newBuilder()
                    .addKeys(Utils.toByteString(key))
                    .build();

            return TxHeader.valueOf(this.stub.delete(req));
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("key not found")) {
                throw new KeyNotFoundException();
            }

            throw e;
        }
    }

    //
    // ========== HISTORY ==========
    //

    public List<Entry> history(String key, int limit, long offset, boolean desc) throws KeyNotFoundException {
        return history(Utils.toByteArray(key), limit, offset, desc);
    }

    public synchronized List<Entry> history(byte[] key, int limit, long offset, boolean desc)
            throws KeyNotFoundException {
        try {
            ImmudbProto.Entries entries = this.stub.history(ImmudbProto.HistoryRequest.newBuilder()
                    .setKey(Utils.toByteString(key))
                    .setLimit(limit)
                    .setOffset(offset)
                    .setDesc(desc)
                    .build());

            return buildList(entries);
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("key not found")) {
                throw new KeyNotFoundException();
            }

            throw e;
        }
    }

    //
    // ========== SCAN ==========
    //

    public List<Entry> scan(String prefix) {
        return scan(Utils.toByteArray(prefix));
    }

    public List<Entry> scan(byte[] prefix) {
        return scan(prefix, 0, false);
    }

    public List<Entry> scan(String prefix, long limit, boolean desc) {
        return scan(Utils.toByteArray(prefix), limit, desc);
    }

    public List<Entry> scan(byte[] prefix, long limit, boolean desc) {
        return scan(prefix, null, limit, desc);
    }

    public List<Entry> scan(String prefix, String seekKey, long limit, boolean desc) {
        return scan(Utils.toByteArray(prefix), Utils.toByteArray(seekKey), limit, desc);
    }

    public List<Entry> scan(String prefix, String seekKey, String endKey, long limit, boolean desc) {
        return scan(Utils.toByteArray(prefix), Utils.toByteArray(seekKey), Utils.toByteArray(endKey), limit, desc);
    }

    public List<Entry> scan(byte[] prefix, byte[] seekKey, long limit, boolean desc) {
        return scan(prefix, seekKey, null, limit, desc);
    }

    public List<Entry> scan(byte[] prefix, byte[] seekKey, byte[] endKey, long limit, boolean desc) {
        return scan(prefix, seekKey, endKey, false, false, limit, desc);
    }

    public synchronized List<Entry> scan(byte[] prefix, byte[] seekKey, byte[] endKey, boolean inclusiveSeek,
            boolean inclusiveEnd,
            long limit, boolean desc) {
        final ImmudbProto.ScanRequest req = ScanRequest.newBuilder()
                .setPrefix(Utils.toByteString(prefix))
                .setSeekKey(Utils.toByteString(seekKey))
                .setEndKey(Utils.toByteString(endKey))
                .setInclusiveSeek(inclusiveSeek)
                .setInclusiveEnd(inclusiveEnd)
                .setLimit(limit)
                .setDesc(desc)
                .build();

        final ImmudbProto.Entries entries = this.stub.scan(req);
        return buildList(entries);
    }

    //
    // ========== SET ==========
    //

    public TxHeader set(String key, byte[] value) throws CorruptedDataException {
        return set(Utils.toByteArray(key), value);
    }

    public synchronized TxHeader set(byte[] key, byte[] value) throws CorruptedDataException {
        final ImmudbProto.KeyValue kv = ImmudbProto.KeyValue.newBuilder()
                .setKey(Utils.toByteString(key))
                .setValue(Utils.toByteString(value))
                .build();

        final ImmudbProto.SetRequest req = ImmudbProto.SetRequest.newBuilder().addKVs(kv).build();
        final ImmudbProto.TxHeader txHdr = this.stub.set(req);

        if (txHdr.getNentries() != 1) {
            throw new CorruptedDataException();
        }

        return TxHeader.valueOf(txHdr);
    }

    public synchronized TxHeader setAll(List<KVPair> kvList) throws CorruptedDataException {
        final ImmudbProto.SetRequest.Builder reqBuilder = ImmudbProto.SetRequest.newBuilder();

        for (KVPair kv : kvList) {
            ImmudbProto.KeyValue.Builder kvBuilder = ImmudbProto.KeyValue.newBuilder();

            kvBuilder.setKey(Utils.toByteString(kv.getKey()));
            kvBuilder.setValue(Utils.toByteString(kv.getValue()));

            reqBuilder.addKVs(kvBuilder.build());
        }

        final ImmudbProto.TxHeader txHdr = this.stub.set(reqBuilder.build());

        if (txHdr.getNentries() != kvList.size()) {
            throw new CorruptedDataException();
        }

        return TxHeader.valueOf(txHdr);
    }

    public TxHeader setReference(String key, String referencedKey) throws CorruptedDataException {
        return setReference(Utils.toByteArray(key), Utils.toByteArray(referencedKey));
    }

    public TxHeader setReference(byte[] key, byte[] referencedKey) throws CorruptedDataException {
        return setReference(key, referencedKey, 0);
    }

    public TxHeader setReference(String key, String referencedKey, long atTx) throws CorruptedDataException {
        return setReference(Utils.toByteArray(key), Utils.toByteArray(referencedKey), atTx);
    }

    public synchronized TxHeader setReference(byte[] key, byte[] referencedKey, long atTx)
            throws CorruptedDataException {
        final ImmudbProto.ReferenceRequest req = ImmudbProto.ReferenceRequest.newBuilder()
                .setKey(Utils.toByteString(key))
                .setReferencedKey(Utils.toByteString(referencedKey))
                .setAtTx(atTx)
                .setBoundRef(atTx > 0)
                .build();

        final ImmudbProto.TxHeader txHdr = this.stub.setReference(req);

        if (txHdr.getNentries() != 1) {
            throw new CorruptedDataException();
        }

        return TxHeader.valueOf(txHdr);
    }

    public TxHeader verifiedSet(String key, byte[] value) throws VerificationException {
        return verifiedSet(Utils.toByteArray(key), value);
    }

    public synchronized TxHeader verifiedSet(byte[] key, byte[] value) throws VerificationException {
        final ImmuState state = this.state();

        final ImmudbProto.KeyValue kv = ImmudbProto.KeyValue.newBuilder()
                .setKey(Utils.toByteString(key))
                .setValue(Utils.toByteString(value))
                .build();

        final ImmudbProto.VerifiableSetRequest vSetReq = ImmudbProto.VerifiableSetRequest.newBuilder()
                .setSetRequest(ImmudbProto.SetRequest.newBuilder().addKVs(kv).build())
                .setProveSinceTx(state.getTxId())
                .build();

        final ImmudbProto.VerifiableTx vtx = this.stub.verifiableSet(vSetReq);

        final int ne = vtx.getTx().getHeader().getNentries();

        if (ne != 1 || vtx.getTx().getEntriesList().size() != 1) {
            throw new VerificationException(
                    String.format("Got back %d entries (in tx metadata) instead of 1.", ne));
        }

        final Tx tx;

        try {
            tx = Tx.valueOf(vtx.getTx());
        } catch (Exception e) {
            throw new VerificationException("Failed to extract the transaction.", e);
        }

        final TxHeader txHeader = tx.getHeader();

        final Entry entry = Entry.valueOf(ImmudbProto.Entry.newBuilder()
                .setKey(Utils.toByteString(key))
                .setValue(Utils.toByteString(value))
                .build());

        final InclusionProof inclusionProof = tx.proof(entry.getEncodedKey());

        if (!CryptoUtils.verifyInclusion(inclusionProof, entry.digestFor(txHeader.getVersion()), txHeader.getEh())) {
            throw new VerificationException("Data is corrupted (verify inclusion failed)");
        }

        final ImmuState newState = verifyDualProof(vtx, tx, state);

        if (!newState.checkSignature(serverSigningKey)) {
            throw new VerificationException("State signature verification failed");
        }

        this.stateHolder.setState(newState);

        return TxHeader.valueOf(vtx.getTx().getHeader());
    }

    public TxHeader verifiedSetReference(byte[] key, byte[] referencedKey) throws VerificationException {
        return verifiedSetReference(key, referencedKey, 0);
    }

    public synchronized TxHeader verifiedSetReference(byte[] key, byte[] referencedKey, long atTx)
            throws VerificationException {

        final ImmuState state = this.state();

        final ImmudbProto.ReferenceRequest refReq = ImmudbProto.ReferenceRequest.newBuilder()
                .setKey(Utils.toByteString(key))
                .setReferencedKey(Utils.toByteString(referencedKey))
                .setAtTx(atTx)
                .setBoundRef(atTx > 0)
                .build();

        final ImmudbProto.VerifiableReferenceRequest vRefReq = ImmudbProto.VerifiableReferenceRequest.newBuilder()
                .setReferenceRequest(refReq)
                .setProveSinceTx(state.getTxId())
                .build();

        final ImmudbProto.VerifiableTx vtx = this.stub.verifiableSetReference(vRefReq);

        final int vtxNentries = vtx.getTx().getHeader().getNentries();
        if (vtxNentries != 1) {
            throw new VerificationException(String.format("Data is corrupted (verifTx has %d Nentries instead of 1).",
                    vtxNentries));
        }

        final Tx tx;
        try {
            tx = Tx.valueOf(vtx.getTx());
        } catch (Exception e) {
            throw new VerificationException("Failed to extract the transaction.", e);
        }

        final TxHeader txHeader = tx.getHeader();

        final Entry entry = Entry.valueOf(ImmudbProto.Entry.newBuilder()
                .setKey(Utils.toByteString(referencedKey))
                .setReferencedBy(ImmudbProto.Reference.newBuilder()
                        .setKey(Utils.toByteString(key))
                        .setAtTx(atTx)
                        .build())
                .build());

        final InclusionProof inclusionProof = tx.proof(entry.getEncodedKey());

        if (!CryptoUtils.verifyInclusion(inclusionProof, entry.digestFor(txHeader.getVersion()), txHeader.getEh())) {
            throw new VerificationException("Data is corrupted (inclusion verification failed).");
        }

        if (Arrays.equals(txHeader.getEh(),
                CryptoUtils.digestFrom(vtx.getDualProof().getTargetTxHeader().getEH().toByteArray()))) {
            throw new VerificationException("Data is corrupted (different digests).");
        }

        final ImmuState newState = verifyDualProof(vtx, tx, state);

        if (!newState.checkSignature(serverSigningKey)) {
            throw new VerificationException("State signature verification failed");
        }

        this.stateHolder.setState(newState);

        return TxHeader.valueOf(vtx.getTx().getHeader());
    }

    private ImmuState verifyDualProof(ImmudbProto.VerifiableTx vtx, Tx tx, ImmuState state)
            throws VerificationException {

        final long sourceId = state.getTxId();
        final long targetId = tx.getHeader().getId();
        final byte[] sourceAlh = CryptoUtils.digestFrom(state.getTxHash());
        final byte[] targetAlh = tx.getHeader().alh();

        if (state.getTxId() > 0) {
            if (!CryptoUtils.verifyDualProof(
                    DualProof.valueOf(vtx.getDualProof()),
                    sourceId,
                    targetId,
                    sourceAlh,
                    targetAlh)) {
                throw new VerificationException("Data is corrupted (dual proof verification failed).");
            }
        }

        return new ImmuState(state.getDatabase(), targetId, targetAlh, vtx.getSignature().getSignature().toByteArray());
    }

    //
    // ========== Z ==========
    //

    public TxHeader zAdd(String set, String key, double score) throws CorruptedDataException {
        return zAdd(Utils.toByteArray(set), Utils.toByteArray(key), score);
    }

    public TxHeader zAdd(byte[] set, byte[] key, double score) throws CorruptedDataException {
        return zAdd(set, key, 0, score);
    }

    public synchronized TxHeader zAdd(byte[] set, byte[] key, long atTx, double score) throws CorruptedDataException {
        final ImmudbProto.TxHeader txHdr = this.stub.zAdd(
                ImmudbProto.ZAddRequest.newBuilder()
                        .setSet(Utils.toByteString(set))
                        .setKey(Utils.toByteString(key))
                        .setAtTx(atTx)
                        .setScore(score)
                        .setBoundRef(atTx > 0)
                        .build());

        if (txHdr.getNentries() != 1) {
            throw new CorruptedDataException();
        }

        return TxHeader.valueOf(txHdr);
    }

    public TxHeader verifiedZAdd(String set, String key, double score) throws VerificationException {
        return verifiedZAdd(Utils.toByteArray(set), Utils.toByteArray(key), score);
    }

    public TxHeader verifiedZAdd(byte[] set, byte[] key, double score) throws VerificationException {
        return verifiedZAdd(set, key, 0, score);
    }

    public TxHeader verifiedZAdd(String set, String key, long atTx, double score) throws VerificationException {
        return verifiedZAdd(Utils.toByteArray(set), Utils.toByteArray(key), atTx, score);
    }

    public synchronized TxHeader verifiedZAdd(byte[] set, byte[] key, long atTx, double score)
            throws VerificationException {

        final ImmuState state = this.state();

        final ImmudbProto.ZAddRequest zAddReq = ImmudbProto.ZAddRequest.newBuilder()
                .setSet(Utils.toByteString(set))
                .setKey(Utils.toByteString(key))
                .setAtTx(atTx)
                .setBoundRef(atTx > 0)
                .setScore(score)
                .build();

        final ImmudbProto.VerifiableZAddRequest vZAddReq = ImmudbProto.VerifiableZAddRequest.newBuilder()
                .setZAddRequest(zAddReq)
                .setProveSinceTx(state.getTxId())
                .build();

        final ImmudbProto.VerifiableTx vtx = this.stub.verifiableZAdd(vZAddReq);

        if (vtx.getTx().getHeader().getNentries() != 1) {
            throw new VerificationException("Data is corrupted.");
        }

        final Tx tx;
        try {
            tx = Tx.valueOf(vtx.getTx());
        } catch (Exception e) {
            throw new VerificationException("Failed to extract the transaction.", e);
        }

        final TxHeader txHeader = tx.getHeader();

        final ZEntry entry = ZEntry.valueOf(ImmudbProto.ZEntry.newBuilder()
                .setSet(Utils.toByteString(set))
                .setKey(Utils.toByteString(key))
                .setAtTx(atTx)
                .setScore(score)
                .build());

        InclusionProof inclusionProof = tx.proof(entry.getEncodedKey());

        if (!CryptoUtils.verifyInclusion(inclusionProof, entry.digestFor(txHeader.getVersion()), txHeader.getEh())) {
            throw new VerificationException("Data is corrupted (inclusion verification failed).");
        }

        if (!Arrays.equals(txHeader.getEh(),
                CryptoUtils.digestFrom(vtx.getDualProof().getTargetTxHeader().getEH().toByteArray()))) {
            throw new VerificationException("Data is corrupted (different digests).");
        }

        final ImmuState newState = verifyDualProof(vtx, tx, state);

        if (!newState.checkSignature(serverSigningKey)) {
            throw new VerificationException("State signature verification failed");
        }

        this.stateHolder.setState(newState);

        return TxHeader.valueOf(vtx.getTx().getHeader());
    }

    public List<ZEntry> zScan(String set, long limit, boolean reverse) {
        return zScan(Utils.toByteArray(set), limit, reverse);
    }

    public synchronized List<ZEntry> zScan(byte[] set, long limit, boolean reverse) {
        final ImmudbProto.ZScanRequest req = ImmudbProto.ZScanRequest
                .newBuilder()
                .setSet(Utils.toByteString(set))
                .setLimit(limit)
                .setDesc(reverse)
                .build();

        final ImmudbProto.ZEntries zEntries = this.stub.zScan(req);

        return buildList(zEntries);
    }

    //
    // ========== TX ==========
    //

    public synchronized Tx txById(long txId) throws TxNotFoundException, NoSuchAlgorithmException {
        try {
            final ImmudbProto.Tx tx = this.stub.txById(ImmudbProto.TxRequest.newBuilder().setTx(txId).build());
            return Tx.valueOf(tx);
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("tx not found")) {
                throw new TxNotFoundException();
            }

            throw e;
        }
    }

    public synchronized Tx verifiedTxById(long txId) throws TxNotFoundException, VerificationException {
        final ImmuState state = this.state();

        final ImmudbProto.VerifiableTxRequest vTxReq = ImmudbProto.VerifiableTxRequest.newBuilder()
                .setTx(txId)
                .setProveSinceTx(state.getTxId())
                .build();

        final ImmudbProto.VerifiableTx vtx;

        try {
            vtx = this.stub.verifiableTxById(vTxReq);
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("tx not found")) {
                throw new TxNotFoundException();
            }

            throw e;
        }

        final DualProof dualProof = DualProof.valueOf(vtx.getDualProof());

        long sourceId;
        long targetId;
        byte[] sourceAlh;
        byte[] targetAlh;

        if (state.getTxId() <= txId) {
            sourceId = state.getTxId();
            sourceAlh = CryptoUtils.digestFrom(state.getTxHash());
            targetId = txId;
            targetAlh = dualProof.targetTxHeader.alh();
        } else {
            sourceId = txId;
            sourceAlh = dualProof.sourceTxHeader.alh();
            targetId = state.getTxId();
            targetAlh = CryptoUtils.digestFrom(state.getTxHash());
        }

        if (state.getTxId() > 0) {
            if (!CryptoUtils.verifyDualProof(
                    DualProof.valueOf(vtx.getDualProof()),
                    sourceId,
                    targetId,
                    sourceAlh,
                    targetAlh)) {
                throw new VerificationException("Data is corrupted (dual proof verification failed).");
            }
        }

        final Tx tx;
        try {
            tx = Tx.valueOf(vtx.getTx());
        } catch (Exception e) {
            throw new VerificationException("Failed to extract the transaction.", e);
        }

        final ImmuState newState = new ImmuState(state.getDatabase(), targetId, targetAlh,
                vtx.getSignature().getSignature().toByteArray());

        if (!newState.checkSignature(serverSigningKey)) {
            throw new VerificationException("State signature verification failed");
        }

        stateHolder.setState(newState);

        return tx;
    }

    public synchronized List<Tx> txScan(long initialTxId) {
        final ImmudbProto.TxScanRequest req = ImmudbProto.TxScanRequest.newBuilder().setInitialTx(initialTxId).build();
        final ImmudbProto.TxList txList = this.stub.txScan(req);
        return buildList(txList);
    }

    public synchronized List<Tx> txScan(long initialTxId, int limit, boolean desc) {
        final ImmudbProto.TxScanRequest req = ImmudbProto.TxScanRequest
                .newBuilder()
                .setInitialTx(initialTxId)
                .setLimit(limit)
                .setDesc(desc)
                .build();
        final ImmudbProto.TxList txList = this.stub.txScan(req);
        return buildList(txList);
    }

    //
    // ========== HEALTH ==========
    //

    public boolean isConnected() {
        return channel != null && channel.getState(false) == ConnectivityState.READY;
    }

    public boolean isShutdown() {
        return channel != null && channel.isShutdown();
    }

    public synchronized boolean healthCheck() {
        return this.stub.serverInfo(ImmudbProto.ServerInfoRequest.getDefaultInstance()) != null;
    }

    //
    // ========== USER MGMT ==========
    //

    public synchronized List<User> listUsers() {
        final ImmudbProto.UserList userList = this.stub.listUsers(Empty.getDefaultInstance());

        return userList.getUsersList()
                .stream()
                .map(u -> User.getBuilder()
                        .setUser(u.getUser().toString(StandardCharsets.UTF_8))
                        .setActive(u.getActive())
                        .setCreatedAt(u.getCreatedat())
                        .setCreatedBy(u.getCreatedby())
                        .setPermissions(buildPermissions(u.getPermissionsList()))
                        .build())
                .collect(Collectors.toList());
    }

    private List<Permission> buildPermissions(List<ImmudbProto.Permission> permissionsList) {
        return permissionsList
                .stream()
                .map(p -> Permission.valueOfPermissionCode(p.getPermission()))
                .collect(Collectors.toList());
    }

    public synchronized void createUser(String user, String password, Permission permission, String database) {
        final ImmudbProto.CreateUserRequest createUserRequest = ImmudbProto.CreateUserRequest.newBuilder()
                .setUser(Utils.toByteString(user))
                .setPassword(Utils.toByteString(password))
                .setPermission(permission.permissionCode)
                .setDatabase(database)
                .build();

        // noinspection ResultOfMethodCallIgnored
        this.stub.createUser(createUserRequest);
    }

    public synchronized void changePassword(String user, String oldPassword, String newPassword) {
        final ImmudbProto.ChangePasswordRequest changePasswordRequest = ImmudbProto.ChangePasswordRequest.newBuilder()
                .setUser(Utils.toByteString(user))
                .setOldPassword(Utils.toByteString(oldPassword))
                .setNewPassword(Utils.toByteString(newPassword))
                .build();

        // noinspection ResultOfMethodCallIgnored
        this.stub.changePassword(changePasswordRequest);
    }

    //
    // ========== INDEX MGMT ==========
    //

    public synchronized void flushIndex(float cleanupPercentage, boolean synced) {
        ImmudbProto.FlushIndexRequest req = ImmudbProto.FlushIndexRequest.newBuilder()
                .setCleanupPercentage(cleanupPercentage)
                .setSynced(synced)
                .build();

        this.stub.flushIndex(req);
    }

    public synchronized void compactIndex() {
        this.stub.compactIndex(Empty.getDefaultInstance());
    }

    //
    // ========== INTERNAL UTILS ==========
    //

    private List<Entry> buildList(ImmudbProto.Entries entries) {
        final List<Entry> result = new ArrayList<>(entries.getEntriesCount());
        entries.getEntriesList()
                .forEach(entry -> result.add(Entry.valueOf(entry)));
        return result;
    }

    private List<ZEntry> buildList(ImmudbProto.ZEntries entries) {
        final List<ZEntry> result = new ArrayList<>(entries.getEntriesCount());
        entries.getEntriesList()
                .forEach(entry -> result.add(ZEntry.valueOf(entry)));
        return result;
    }

    private List<Tx> buildList(ImmudbProto.TxList txList) {
        final List<Tx> result = new ArrayList<>(txList.getTxsCount());
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
