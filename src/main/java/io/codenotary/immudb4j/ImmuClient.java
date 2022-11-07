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
import io.codenotary.immudb.ImmudbProto.Chunk;
import io.codenotary.immudb.ImmudbProto.ScanRequest;
import io.codenotary.immudb4j.basics.LatchHolder;
import io.codenotary.immudb4j.crypto.CryptoUtils;
import io.codenotary.immudb4j.crypto.DualProof;
import io.codenotary.immudb4j.crypto.InclusionProof;
import io.codenotary.immudb4j.exceptions.*;
import io.codenotary.immudb4j.user.Permission;
import io.codenotary.immudb4j.user.User;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.grpc.ConnectivityState;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
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
    private long keepAlivePeriod;
    private int chunkSize;

    private ManagedChannel channel;

    private final ImmuServiceGrpc.ImmuServiceBlockingStub blockingStub;
    private final ImmuServiceGrpc.ImmuServiceStub nonBlockingStub;

    private Session session;
    private Timer sessionHeartBeat;
    
    public ImmuClient(Builder builder) {
        stateHolder = builder.getStateHolder();
        serverSigningKey = builder.getServerSigningKey();
        keepAlivePeriod = builder.getKeepAlivePeriod();
        chunkSize = builder.getChunkSize();

        channel = ManagedChannelBuilder
                    .forAddress(builder.getServerUrl(), builder.getServerPort())
                    .usePlaintext()
                    .intercept(new ImmudbAuthRequestInterceptor(this))
                    .build();

        blockingStub = ImmuServiceGrpc.newBlockingStub(channel);
        nonBlockingStub = ImmuServiceGrpc.newStub(channel);
    }

    public static Builder newBuilder() {
        return new Builder();
    }
    
    public synchronized void shutdown() throws InterruptedException {
        if (channel == null) {
            return;
        }

        if (session != null) {
            closeSession();
        }

        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } finally {
            channel = null;
        }
    }

    protected synchronized Session getSession() {
        return session;
    }

    public synchronized void openSession(String database) {
        openSession(database, "", "");
    }

    public synchronized void openSession(String database, String username, String password) {
        if (session != null) {
            throw new IllegalStateException("session already opened");
        }

        final ImmudbProto.OpenSessionRequest req = ImmudbProto.OpenSessionRequest
                .newBuilder()
                .setDatabaseName(database)
                .setUsername(Utils.toByteString(username))
                .setPassword(Utils.toByteString(password))
                .build();

        final ImmudbProto.OpenSessionResponse resp = this.blockingStub.openSession(req);

        session = new Session(resp.getSessionID(), database);

        sessionHeartBeat = new Timer();

        sessionHeartBeat.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    blockingStub.keepAlive(Empty.getDefaultInstance());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 0, keepAlivePeriod);
    }

    public synchronized void closeSession() {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        sessionHeartBeat.cancel();

        try {
            blockingStub.closeSession(Empty.getDefaultInstance());
        } finally {
            session = null;
        }
    }

    /**
     * Get the locally saved state of the current database.
     * If nothing exists already, it is fetched from the server and save it locally.
     */
    private ImmuState state() throws VerificationException {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        ImmuState state = stateHolder.getState(session.getDatabase());

        if (state == null) {
            state = currentState();
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
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.ImmutableState state = blockingStub.currentState(Empty.getDefaultInstance());

        final ImmuState immuState = ImmuState.valueOf(state);

        if (!session.getDatabase().equals(immuState.getDatabase())) {
            throw new VerificationException("database mismatch");
        }

        if (!immuState.checkSignature(serverSigningKey)) {
            throw new VerificationException("State signature verification failed");
        }

        return immuState;
    }

    //
    // ========== DATABASE ==========
    //

    public synchronized void createDatabase(String database) {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.CreateDatabaseRequest req = ImmudbProto.CreateDatabaseRequest.newBuilder()
                .setName(database)
                .build();

        blockingStub.createDatabaseV2(req);
    }

    // LoadDatabase loads database on the server. A database is not loaded
    // if it has AutoLoad setting set to false or if it failed to load during
    // immudb startup.
    //
    // This call requires SysAdmin permission level or admin permission to the database.
    public synchronized void loadDatabase(String database) {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.LoadDatabaseRequest req = ImmudbProto.LoadDatabaseRequest.newBuilder()
                .setDatabase(database)
                .build();

        blockingStub.loadDatabase(req);
    }

    // UnloadDatabase unloads database on the server. Such database becomes inaccessible
    // by the client and server frees internal resources allocated for that database.
    //
    // This call requires SysAdmin permission level or admin permission to the database.
    public synchronized void unloadDatabase(String database) {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.UnloadDatabaseRequest req = ImmudbProto.UnloadDatabaseRequest.newBuilder()
                .setDatabase(database)
                .build();

        blockingStub.unloadDatabase(req);
    }

    // DeleteDatabase removes an unloaded database.
    // This also removes locally stored files used by the database.
    //
    // This call requires SysAdmin permission level or admin permission to the database.
    public synchronized void deleteDatabase(String database) {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.DeleteDatabaseRequest req = ImmudbProto.DeleteDatabaseRequest.newBuilder()
                .setDatabase(database)
                .build();

        blockingStub.deleteDatabase(req);
    }

    public synchronized List<String> databases() {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.DatabaseListRequestV2 req = ImmudbProto.DatabaseListRequestV2.newBuilder().build();
        final ImmudbProto.DatabaseListResponseV2 resp = blockingStub.databaseListV2(req);

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
            return Entry.valueOf(blockingStub.get(req));
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
            return Entry.valueOf(blockingStub.get(req));
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
            return Entry.valueOf(blockingStub.get(req));
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
        final ImmudbProto.Entries entries = blockingStub.getAll(req);

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
        final ImmuState state = state();

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

        final ImmuState state = state();

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

        final ImmuState state = state();

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
            vEntry = blockingStub.verifiableGet(vGetReq);
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
                session.getDatabase(),
                targetId,
                targetAlh,
                vEntry.getVerifiableTx().getSignature().toByteArray());

        if (!newState.checkSignature(serverSigningKey)) {
            throw new VerificationException("State signature verification failed");
        }

        stateHolder.setState(newState);

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

            return TxHeader.valueOf(blockingStub.delete(req));
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
            ImmudbProto.Entries entries = blockingStub.history(ImmudbProto.HistoryRequest.newBuilder()
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

        final ImmudbProto.Entries entries = blockingStub.scan(req);
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
        final ImmudbProto.TxHeader txHdr = blockingStub.set(req);

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

        final ImmudbProto.TxHeader txHdr = blockingStub.set(reqBuilder.build());

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

        final ImmudbProto.TxHeader txHdr = blockingStub.setReference(req);

        if (txHdr.getNentries() != 1) {
            throw new CorruptedDataException();
        }

        return TxHeader.valueOf(txHdr);
    }

    public TxHeader verifiedSet(String key, byte[] value) throws VerificationException {
        return verifiedSet(Utils.toByteArray(key), value);
    }

    public synchronized TxHeader verifiedSet(byte[] key, byte[] value) throws VerificationException {
        final ImmuState state = state();

        final ImmudbProto.KeyValue kv = ImmudbProto.KeyValue.newBuilder()
                .setKey(Utils.toByteString(key))
                .setValue(Utils.toByteString(value))
                .build();

        final ImmudbProto.VerifiableSetRequest vSetReq = ImmudbProto.VerifiableSetRequest.newBuilder()
                .setSetRequest(ImmudbProto.SetRequest.newBuilder().addKVs(kv).build())
                .setProveSinceTx(state.getTxId())
                .build();

        final ImmudbProto.VerifiableTx vtx = blockingStub.verifiableSet(vSetReq);

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

        stateHolder.setState(newState);

        return TxHeader.valueOf(vtx.getTx().getHeader());
    }

    public TxHeader verifiedSetReference(byte[] key, byte[] referencedKey) throws VerificationException {
        return verifiedSetReference(key, referencedKey, 0);
    }

    public synchronized TxHeader verifiedSetReference(byte[] key, byte[] referencedKey, long atTx)
            throws VerificationException {

        final ImmuState state = state();

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

        final ImmudbProto.VerifiableTx vtx = blockingStub.verifiableSetReference(vRefReq);

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

        stateHolder.setState(newState);

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
        final ImmudbProto.TxHeader txHdr = blockingStub.zAdd(
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

        final ImmuState state = state();

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

        final ImmudbProto.VerifiableTx vtx = blockingStub.verifiableZAdd(vZAddReq);

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

        stateHolder.setState(newState);

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

        final ImmudbProto.ZEntries zEntries = blockingStub.zScan(req);

        return buildList(zEntries);
    }

    //
    // ========== TX ==========
    //

    public synchronized Tx txById(long txId) throws TxNotFoundException, NoSuchAlgorithmException {
        try {
            final ImmudbProto.Tx tx = blockingStub.txById(ImmudbProto.TxRequest.newBuilder().setTx(txId).build());
            return Tx.valueOf(tx);
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("tx not found")) {
                throw new TxNotFoundException();
            }

            throw e;
        }
    }

    public synchronized Tx verifiedTxById(long txId) throws TxNotFoundException, VerificationException {
        final ImmuState state = state();

        final ImmudbProto.VerifiableTxRequest vTxReq = ImmudbProto.VerifiableTxRequest.newBuilder()
                .setTx(txId)
                .setProveSinceTx(state.getTxId())
                .build();

        final ImmudbProto.VerifiableTx vtx;

        try {
            vtx = blockingStub.verifiableTxById(vTxReq);
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
        final ImmudbProto.TxList txList = blockingStub.txScan(req);
        return buildList(txList);
    }

    public synchronized List<Tx> txScan(long initialTxId, int limit, boolean desc) {
        final ImmudbProto.TxScanRequest req = ImmudbProto.TxScanRequest
                .newBuilder()
                .setInitialTx(initialTxId)
                .setLimit(limit)
                .setDesc(desc)
                .build();
        final ImmudbProto.TxList txList = blockingStub.txScan(req);
        return buildList(txList);
    }

    //
    // ========== STREAMS ==========
    //

    private StreamObserver<ImmudbProto.TxHeader> txHeaderStreamObserver(LatchHolder<ImmudbProto.TxHeader> latchHolder) {
        return new StreamObserver<ImmudbProto.TxHeader>() {
            @Override
            public void onCompleted() {
            }
    
            @Override
            public void onError(Throwable cause) {
                throw new RuntimeException(cause);
            }
    
            @Override
            public void onNext(ImmudbProto.TxHeader hdr) {
                latchHolder.doneWith(hdr);
            }
        };
    }

    private void chunkIt(byte[] bs, StreamObserver<Chunk> streamObserver) {
        final ByteBuffer buf = ByteBuffer.allocate(Long.BYTES+bs.length).order(ByteOrder.BIG_ENDIAN);

        buf.putLong(bs.length);
        buf.put(bs);

        final Chunk chunk = Chunk.newBuilder().setContent(Utils.toByteString(buf.array())).build();

        streamObserver.onNext(chunk);
    }

    private byte[] dechunkIt(Iterator<Chunk> chunks) {
        final Chunk firstChunk = chunks.next();
        final byte[] firstChunkContent = firstChunk.getContent().toByteArray();

        if (firstChunkContent.length < Long.BYTES) {
            throw new RuntimeException("invalid chunk");
        }

        final ByteBuffer b = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
        b.put(firstChunkContent, 0, Long.BYTES);
        
        int payloadSize = (int) b.getLong(0);

        final ByteBuffer buf = ByteBuffer.allocate(payloadSize);
        buf.put(firstChunkContent, Long.BYTES, firstChunkContent.length-Long.BYTES);

        while (buf.position() < payloadSize) {
            Chunk chunk = chunks.next();
            buf.put(chunk.getContent().toByteArray());
        }

        return buf.array();
    }

    private Iterator<Entry> entryIterator(Iterator<Chunk> chunks) {
        return new Iterator<Entry>() {

            @Override
            public boolean hasNext() {
                return chunks.hasNext();
            }

            @Override
            public Entry next() {
                return new Entry(dechunkIt(chunks), dechunkIt(chunks));
            }

        };
    }

    private Iterator<ZEntry> zentryIterator(Iterator<Chunk> chunks) {
        return new Iterator<ZEntry>() {

            @Override
            public boolean hasNext() {
                return chunks.hasNext();
            }

            @Override
            public ZEntry next() {
                final byte[] set = dechunkIt(chunks);
                final byte[] key = dechunkIt(chunks);

                final ByteBuffer b = ByteBuffer.allocate(Double.BYTES + Long.BYTES).order(ByteOrder.BIG_ENDIAN);
                b.put(dechunkIt(chunks)); // score
                b.put(dechunkIt(chunks)); // atTx

                double score = b.getDouble(0);
                long atTx = b.getLong(Double.BYTES);

                final byte[] value = dechunkIt(chunks);

                final Entry entry = new Entry(key, value);

                return new ZEntry(set, key, score, atTx, entry);
            }

        };
    }

    //
    // ========== STREAM SET ==========
    //

    public TxHeader streamSet(String key, byte[] value) throws InterruptedException {
        return streamSet(Utils.toByteArray(key), value);
    }

    public synchronized TxHeader streamSet(byte[] key, byte[] value) throws InterruptedException {
        final LatchHolder<ImmudbProto.TxHeader> latchHolder = new LatchHolder<>();
        final StreamObserver<Chunk> streamObserver = nonBlockingStub.streamSet(txHeaderStreamObserver(latchHolder));

        chunkIt(key, streamObserver);
        chunkIt(value, streamObserver);
        
        streamObserver.onCompleted();

        return TxHeader.valueOf(latchHolder.awaitValue());
    }

    public synchronized TxHeader streamSetAll(List<KVPair> kvList) throws InterruptedException {
        final LatchHolder<ImmudbProto.TxHeader> latchHolder = new LatchHolder<>();
        final StreamObserver<Chunk> streamObserver = nonBlockingStub.streamSet(txHeaderStreamObserver(latchHolder));

        for (KVPair kv : kvList) {
            chunkIt(kv.getKey(), streamObserver);
            chunkIt(kv.getValue(), streamObserver);
        }

        streamObserver.onCompleted();

        return TxHeader.valueOf(latchHolder.awaitValue());
    }

    //
    // ========== STREAM GET ==========
    //

    public Entry streamGet(String key) throws KeyNotFoundException {
        return streamGet(Utils.toByteArray(key));
    }

    public synchronized Entry streamGet(byte[] key) throws KeyNotFoundException {
        final ImmudbProto.KeyRequest req = ImmudbProto.KeyRequest.newBuilder()
                .setKey(Utils.toByteString(key))
                .build();

        try {
            final Iterator<Chunk> chunks = blockingStub.streamGet(req);
            return new Entry(dechunkIt(chunks), dechunkIt(chunks));
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("key not found")) {
                throw new KeyNotFoundException();
            }

            throw e;
        }
    }

    //
    // ========== STREAM SCAN ==========
    //
    
    public Iterator<Entry> streamScan(String prefix) {
        return streamScan(Utils.toByteArray(prefix));
    }

    public Iterator<Entry> streamScan(byte[] prefix) {
        return streamScan(prefix, 0, false);
    }

    public Iterator<Entry> streamScan(String prefix, long limit, boolean desc) {
        return streamScan(Utils.toByteArray(prefix), limit, desc);
    }

    public Iterator<Entry> streamScan(byte[] prefix, long limit, boolean desc) {
        return streamScan(prefix, null, limit, desc);
    }

    public Iterator<Entry> streamScan(String prefix, String seekKey, long limit, boolean desc) {
        return streamScan(Utils.toByteArray(prefix), Utils.toByteArray(seekKey), limit, desc);
    }

    public Iterator<Entry> streamScan(String prefix, String seekKey, String endKey, long limit, boolean desc) {
        return streamScan(Utils.toByteArray(prefix), Utils.toByteArray(seekKey), Utils.toByteArray(endKey), limit, desc);
    }

    public Iterator<Entry> streamScan(byte[] prefix, byte[] seekKey, long limit, boolean desc) {
        return streamScan(prefix, seekKey, null, limit, desc);
    }

    public Iterator<Entry> streamScan(byte[] prefix, byte[] seekKey, byte[] endKey, long limit, boolean desc) {
        return streamScan(prefix, seekKey, endKey, false, false, limit, desc);
    }

    public synchronized Iterator<Entry> streamScan(byte[] prefix, byte[] seekKey, byte[] endKey, boolean inclusiveSeek,
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

        final Iterator<Chunk> chunks = blockingStub.streamScan(req);

        return entryIterator(chunks);
    }

    //
    // ========== STREAM ZSCAN ==========
    //

    public Iterator<ZEntry> streamZScan(String set, long limit, boolean reverse) {
        return streamZScan(Utils.toByteArray(set), limit, reverse);
    }

    public synchronized Iterator<ZEntry> streamZScan(byte[] set, long limit, boolean reverse) {
        final ImmudbProto.ZScanRequest req = ImmudbProto.ZScanRequest
                .newBuilder()
                .setSet(Utils.toByteString(set))
                .setLimit(limit)
                .setDesc(reverse)
                .build();

        final Iterator<Chunk> chunks = blockingStub.streamZScan(req);

        return zentryIterator(chunks);
    }
 
    //
    // ========== STREAM HISTORY ==========
    //

    public Iterator<Entry> streamHistory(String key, int limit, long offset, boolean desc) throws KeyNotFoundException {
        return streamHistory(Utils.toByteArray(key), limit, offset, desc);
    }

    public synchronized Iterator<Entry> streamHistory(byte[] key, int limit, long offset, boolean desc)
            throws KeyNotFoundException {
        try {
            ImmudbProto.HistoryRequest req = ImmudbProto.HistoryRequest.newBuilder()
                    .setKey(Utils.toByteString(key))
                    .setLimit(limit)
                    .setOffset(offset)
                    .setDesc(desc)
                    .build();

            final Iterator<Chunk> chunks = blockingStub.streamHistory(req);

            return entryIterator(chunks);
        } catch (StatusRuntimeException e) {
            if (e.getMessage().contains("key not found")) {
                throw new KeyNotFoundException();
            }

            throw e;
        }
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
        return blockingStub.serverInfo(ImmudbProto.ServerInfoRequest.getDefaultInstance()) != null;
    }

    //
    // ========== USER MGMT ==========
    //

    public synchronized List<User> listUsers() {
        final ImmudbProto.UserList userList = blockingStub.listUsers(Empty.getDefaultInstance());

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
        blockingStub.createUser(createUserRequest);
    }

    public synchronized void activateUser(String user, boolean active) {
        final ImmudbProto.SetActiveUserRequest req = ImmudbProto.SetActiveUserRequest.newBuilder()
                .setUsername(user)
                .setActive(active)
                .build();

        // noinspection ResultOfMethodCallIgnored
        blockingStub.setActiveUser(req);
    }

    public synchronized void changePassword(String user, String oldPassword, String newPassword) {
        final ImmudbProto.ChangePasswordRequest changePasswordRequest = ImmudbProto.ChangePasswordRequest.newBuilder()
                .setUser(Utils.toByteString(user))
                .setOldPassword(Utils.toByteString(oldPassword))
                .setNewPassword(Utils.toByteString(newPassword))
                .build();

        // noinspection ResultOfMethodCallIgnored
        blockingStub.changePassword(changePasswordRequest);
    }

    public synchronized void grantPermission(String user, String database, int permissions) {
        final ImmudbProto.ChangePermissionRequest req = ImmudbProto.ChangePermissionRequest.newBuilder()
                .setUsername(user)
                .setAction(ImmudbProto.PermissionAction.GRANT)
                .setDatabase(database)
                .setPermission(permissions)
                .build();

        // noinspection ResultOfMethodCallIgnored
        blockingStub.changePermission(req);
    }

    public synchronized void revokePermission(String user, String database, int permissions) {
        final ImmudbProto.ChangePermissionRequest req = ImmudbProto.ChangePermissionRequest.newBuilder()
                .setUsername(user)
                .setAction(ImmudbProto.PermissionAction.REVOKE)
                .setDatabase(database)
                .setPermission(permissions)
                .build();

        // noinspection ResultOfMethodCallIgnored
        blockingStub.changePermission(req);
    }

    //
    // ========== INDEX MGMT ==========
    //

    public synchronized void flushIndex(float cleanupPercentage, boolean synced) {
        ImmudbProto.FlushIndexRequest req = ImmudbProto.FlushIndexRequest.newBuilder()
                .setCleanupPercentage(cleanupPercentage)
                .setSynced(synced)
                .build();

        blockingStub.flushIndex(req);
    }

    public synchronized void compactIndex() {
        blockingStub.compactIndex(Empty.getDefaultInstance());
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

        private long keepAlivePeriod;

        private int chunkSize;

        private ImmuStateHolder stateHolder;

        private Builder() {
            serverUrl = "localhost";
            serverPort = 3322;
            stateHolder = new SerializableImmuStateHolder();
            keepAlivePeriod = 60 * 1000; // 1 minute
            chunkSize = 64 * 1024; // 64 * 1024 64 KiB
        }

        public ImmuClient build() {
            return new ImmuClient(this);
        }

        public String getServerUrl() {
            return serverUrl;
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
            serverSigningKey = CryptoUtils.getDERPublicKey(publicKeyFilename);
            return this;
        }

        public long getKeepAlivePeriod() {
            return keepAlivePeriod;
        }

        public Builder withKeepAlivePeriod(long keepAlivePeriod) {
            this.keepAlivePeriod = keepAlivePeriod;
            return this;
        }

        public ImmuStateHolder getStateHolder() {
            return stateHolder;
        }

        public Builder withStateHolder(ImmuStateHolder stateHolder) {
            this.stateHolder = stateHolder;
            return this;
        }

        public Builder withChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        public int getChunkSize() {
            return chunkSize;
        }
    }

}
