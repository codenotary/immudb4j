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

    private static final String AUTH_HEADER = "authorization";
    private final ImmuServiceGrpc.ImmuServiceBlockingStub stub;
    private final boolean withAuth;
    private final ImmuStateHolder stateHolder;
    private ManagedChannel channel;
    private String authToken;
    private String currentServerUuid;
    private String currentDb = "defaultdb";
    private final PublicKey serverSigningKey;

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

    private boolean checkSignature(ImmuState state) {
        if (serverSigningKey != null) {
            return state.checkSignature(serverSigningKey);
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
        ImmudbProto.CreateDatabaseRequest db = ImmudbProto.CreateDatabaseRequest.newBuilder().setName(database).build();
        getStub().createDatabaseV2(db);
    }

    public synchronized void useDatabase(String database) {
        ImmudbProto.Database db = ImmudbProto.Database.newBuilder().setDatabaseName(database).build();
        ImmudbProto.UseDatabaseReply response = getStub().useDatabase(db);
        authToken = response.getToken();
        currentDb = database;
    }

    public List<String> databases() {
        ImmudbProto.DatabaseListRequestV2 req = ImmudbProto.DatabaseListRequestV2.newBuilder().build();
        ImmudbProto.DatabaseListResponseV2 res = getStub().databaseListV2(req);
        
        List<String> list = new ArrayList<>(res.getDatabasesCount());
        
        for (ImmudbProto.DatabaseWithSettings db : res.getDatabasesList()) {
            list.add(db.getName());
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
    public TxHeader execAll(
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

        return TxHeader.valueOf(getStub().execAll(reqb.build()));
    }

    //
    // ========== GET ==========
    //

    public byte[] get(String key) {
        return get(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] get(byte[] key) {
        ImmudbProto.KeyRequest req = ImmudbProto.KeyRequest.newBuilder().setKey(ByteString.copyFrom(key)).build();
        ImmudbProto.Entry entry = getStub().get(req);

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

    public Entry verifiedGet(String key) throws VerificationException {
        return verifiedGet(key.getBytes(StandardCharsets.UTF_8));
    }

    public Entry verifiedGet(byte[] key) throws VerificationException {
        final ImmuState state = state();
        final ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .build();

        return verifiedGet(keyReq, state);
    }

    private Entry verifiedGet(ImmudbProto.KeyRequest keyReq, ImmuState state) throws VerificationException {
        final ImmudbProto.VerifiableGetRequest vGetReq = ImmudbProto.VerifiableGetRequest.newBuilder()
                .setKeyRequest(keyReq)
                .setProveSinceTx(state.txId)
                .build();
        
        final ImmudbProto.VerifiableEntry vEntry = getStub().verifiableGet(vGetReq);

        final InclusionProof inclusionProof = InclusionProof.valueOf(vEntry.getInclusionProof());
        final DualProof dualProof = DualProof.valueOf(vEntry.getVerifiableTx().getDualProof());

        byte[] eh;
        long sourceId, targetId;
        byte[] sourceAlh;
        byte[] targetAlh;
        
        long vTx = keyReq.getAtTx();

        final ImmudbProto.Entry entry = vEntry.getEntry();

        KV kv;
        KVMetadata md = null;

        if (entry.hasMetadata()) {
            md = Utils.kvMetadataFromProto(entry.getMetadata());
        }

        if (md != null && md.deleted()) {
		    throw new RuntimeException("Data is corrupted");
	    }

        if (!entry.hasReferencedBy()) {
            if (vTx == 0) {
                vTx = entry.getTx();
            }

            kv = CryptoUtils.encodeKV(
                    vGetReq.getKeyRequest().getKey().toByteArray(),
                    md,
                    entry.getValue().toByteArray());
        } else {
            ImmudbProto.Reference entryRefBy = entry.getReferencedBy();

            if (vTx == 0) {
                vTx = entryRefBy.getTx();
            }

            kv = CryptoUtils.encodeReference(
                    vGetReq.getKeyRequest().getKey().toByteArray(),
                    md,
                    entry.getKey().toByteArray(),
                    entryRefBy.getAtTx());
        }

        if (state.txId <= vTx) {
            byte[] digest = vEntry.getVerifiableTx().getDualProof().getTargetTxHeader().getEH().toByteArray();
            eh = CryptoUtils.digestFrom(digest);

            sourceId = state.txId;
            sourceAlh = CryptoUtils.digestFrom(state.txHash);
            targetId = vTx;
            targetAlh = dualProof.targetTxHeader.alh();
        } else {
            byte[] digest = vEntry.getVerifiableTx().getDualProof().getSourceTxHeader().getEH().toByteArray();
            eh = CryptoUtils.digestFrom(digest);

            sourceId = vTx;
            sourceAlh = dualProof.sourceTxHeader.alh();
            targetId = state.txId;
            targetAlh = CryptoUtils.digestFrom(state.txHash);
        }

        byte[] kvDigest = kv.digestFor(vEntry.getVerifiableTx().getTx().getHeader().getVersion());

        if (!CryptoUtils.verifyInclusion(inclusionProof, kvDigest, eh)) {
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

        final ImmuState newState = new ImmuState(
                currentDb,
                targetId,
                targetAlh,
                vEntry.getVerifiableTx().getSignature().toByteArray());

        if (!checkSignature(state)) {
            throw new RuntimeException("State signature verification failed");
        }

        stateHolder.setState(currentServerUuid, newState);

        return Entry.valueOf(vEntry.getEntry());
    }

    public Entry verifiedGetAt(byte[] key, long txId) throws VerificationException {
        final ImmuState state = state();
        final ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setAtTx(txId)
                .build();

        return verifiedGet(keyReq, state);
    }

    public Entry verifiedGetSince(byte[] key, long txId) throws VerificationException {
        final ImmuState state = state();
        final ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
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
        final ScanRequest req = ScanRequest.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build();
        final ImmudbProto.Entries entries = getStub().scan(req);

        return buildList(entries);
    }

    public List<KV> scan(byte[] prefix, long sinceTxId, long limit, boolean desc) {
        final ScanRequest req = ScanRequest.newBuilder()
                .setPrefix(ByteString.copyFrom(prefix))
                .setLimit(limit)
                .setSinceTx(sinceTxId)
                .setDesc(desc)
                .build();

        final ImmudbProto.Entries entries = getStub().scan(req);

        return buildList(entries);
    }

    public List<KV> scan(byte[] prefix, byte[] seekKey, long sinceTxId, long limit, boolean desc) {
        final ScanRequest req = ScanRequest.newBuilder()
                .setPrefix(ByteString.copyFrom(prefix))
                .setLimit(limit)
                .setSeekKey(ByteString.copyFrom(seekKey))
                .setSinceTx(sinceTxId)
                .setDesc(desc)
                .build();

        final ImmudbProto.Entries entries = getStub().scan(req);

        return buildList(entries);
    }

    //
    // ========== SET ==========
    //

    public TxHeader set(String key, byte[] value) throws CorruptedDataException {
        return set(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public TxHeader set(byte[] key, byte[] value) throws CorruptedDataException {
        final ImmudbProto.KeyValue kv = ImmudbProto.KeyValue
                .newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build();
        
        final ImmudbProto.SetRequest req = ImmudbProto.SetRequest.newBuilder().addKVs(kv).build();
        final ImmudbProto.TxHeader txHdr = getStub().set(req);
        
        if (txHdr.getNentries() != 1) {
            throw new CorruptedDataException();
        }

        return TxHeader.valueOf(txHdr);
    }

    public TxHeader setAll(KVList kvList) throws CorruptedDataException {
        final ImmudbProto.SetRequest.Builder reqBuilder = ImmudbProto.SetRequest.newBuilder();
        
        for (KV kv : kvList.entries()) {
            ImmudbProto.KeyValue schemaKV = ImmudbProto.KeyValue
                    .newBuilder()
                    .setKey(ByteString.copyFrom(kv.getKey()))
                    .setValue(ByteString.copyFrom(kv.getValue()))
                    .build();

            reqBuilder.addKVs(schemaKV);
        }

        final ImmudbProto.TxHeader txHdr = getStub().set(reqBuilder.build());

        if (txHdr.getNentries() != kvList.entries().size()) {
            throw new CorruptedDataException();
        }

        return TxHeader.valueOf(txHdr);
    }

    public TxHeader setReference(byte[] key, byte[] referencedKey) throws CorruptedDataException {
        return setReferenceAt(key, referencedKey, 0);
    }

    public TxHeader setReferenceAt(byte[] key, byte[] referencedKey, long atTx) throws CorruptedDataException {
        final ImmudbProto.ReferenceRequest req = ImmudbProto.ReferenceRequest.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setReferencedKey(ByteString.copyFrom(referencedKey))
                .setAtTx(atTx)
                .setBoundRef(atTx > 0)
                .build();

        final ImmudbProto.TxHeader txHdr = getStub().setReference(req);
        
        if (txHdr.getNentries() != 1) {
            throw new CorruptedDataException();
        }

        return TxHeader.valueOf(txHdr);
    }

    public TxHeader  verifiedSet(String key, byte[] value) throws VerificationException {
        return verifiedSet(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public TxHeader verifiedSet(byte[] key, byte[] value) throws VerificationException {
        final ImmuState state = state();
        final ImmudbProto.KeyValue kv = ImmudbProto.KeyValue.newBuilder()
                                    .setKey(ByteString.copyFrom(key))
                                    .setValue(ByteString.copyFrom(value))
                                    .build();
        
        final ImmudbProto.VerifiableSetRequest vSetReq = ImmudbProto.VerifiableSetRequest.newBuilder()
                .setSetRequest(ImmudbProto.SetRequest.newBuilder().addKVs(kv).build())
                .setProveSinceTx(state.txId)
                .build();
        
        final ImmudbProto.VerifiableTx vtx = getStub().verifiableSet(vSetReq);
        
        final int ne = vtx.getTx().getHeader().getNentries();
        
        if (ne != 1 || vtx.getTx().getEntriesList().size() != 1) {
            throw new VerificationException(
                    String.format("Got back %d entries (in tx metadata) instead of 1.", ne)
            );
        }
        
        Tx tx;
        
        try {
            tx = Tx.valueOf(vtx.getTx());
        } catch (Exception e) {
            throw new VerificationException("Failed to extract the transaction.", e);
        }

        final TxHeader txHeader = tx.getHeader();

        final KV kvEntry = CryptoUtils.encodeKV(key, null, value);

        final InclusionProof inclusionProof = tx.proof(kvEntry.getKey());

        if (!CryptoUtils.verifyInclusion(inclusionProof, kvEntry.digestFor(txHeader.getVersion()), txHeader.getEh())) {
            throw new VerificationException("Data is corrupted (verify inclusion failed)");
        }

        final ImmuState newState = verifyDualProof(vtx, tx, state);

        if (!checkSignature(state)) {
            throw new RuntimeException("State signature verification failed");
        }

        stateHolder.setState(currentServerUuid, newState);

        return TxHeader.valueOf(vtx.getTx().getHeader());
    }


    public TxHeader verifiedSetReference(byte[] key, byte[] referencedKey) throws VerificationException {
        return verifiedSetReferenceAt(key, referencedKey, 0);
    }

    public TxHeader verifiedSetReferenceAt(byte[] key, byte[] referencedKey, long atTx) throws VerificationException {
        final ImmuState state = state();

        final ImmudbProto.ReferenceRequest refReq = ImmudbProto.ReferenceRequest.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setReferencedKey(ByteString.copyFrom(referencedKey))
                .setAtTx(atTx)
                .setBoundRef(atTx > 0)
                .build();

        final ImmudbProto.VerifiableReferenceRequest vRefReq = ImmudbProto.VerifiableReferenceRequest.newBuilder()
                .setReferenceRequest(refReq)
                .setProveSinceTx(state.txId)
                .build();

        final ImmudbProto.VerifiableTx vtx = getStub().verifiableSetReference(vRefReq);
       
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

        final KV kvEntry = CryptoUtils.encodeReference(key, null, referencedKey, atTx);

        final InclusionProof inclusionProof = tx.proof(kvEntry.getKey());
        
        if (!CryptoUtils.verifyInclusion(inclusionProof, kvEntry.digestFor(txHeader.getVersion()), txHeader.getEh())) {
            throw new VerificationException("Data is corrupted (inclusion verification failed).");
        }

        if (Arrays.equals(txHeader.getEh(), CryptoUtils.digestFrom(vtx.getDualProof().getTargetTxHeader().getEH().toByteArray()))) {
            throw new VerificationException("Data is corrupted (different digests).");
        }

        final ImmuState newState = verifyDualProof(vtx, tx, state);

        if (!checkSignature(state)) {
            throw new RuntimeException("State signature verification failed");
        }

        stateHolder.setState(currentServerUuid, newState);

        return TxHeader.valueOf(vtx.getTx().getHeader());
    }

    private ImmuState verifyDualProof(ImmudbProto.VerifiableTx vtx, Tx tx, ImmuState state) throws VerificationException {
        long sourceId = state.txId;
        long targetId = tx.getHeader().getId();
        byte[] sourceAlh = CryptoUtils.digestFrom(state.txHash);
        byte[] targetAlh = tx.getHeader().alh();

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

        return new ImmuState(currentDb, targetId, targetAlh, vtx.getSignature().getSignature().toByteArray());
    }

    //
    // ========== Z ==========
    //

    public TxHeader zAdd(String set, double score, String key) throws CorruptedDataException {
        return zAddAt(set, score, key, 0);
    }

    public TxHeader zAddAt(String set, double score, String key, long atTxId) throws CorruptedDataException {
        final ImmudbProto.TxHeader txHdr = getStub().zAdd(
                ImmudbProto.ZAddRequest.newBuilder()
                        .setSet(ByteString.copyFrom(set, StandardCharsets.UTF_8))
                        .setKey(ByteString.copyFrom(key, StandardCharsets.UTF_8))
                        .setScore(score)
                        .setAtTx(atTxId)
                        .setBoundRef(atTxId > 0)
                        .build()
        );
        
        if (txHdr.getNentries() != 1) {
            throw new CorruptedDataException();
        }

        return TxHeader.valueOf(txHdr);
    }


    public TxHeader verifiedZAdd(String set, double score, String key) throws VerificationException {
        return verifiedZAddAt(set.getBytes(StandardCharsets.UTF_8), score, key.getBytes(StandardCharsets.UTF_8), 0);
    }

    public TxHeader verifiedZAdd(byte[] set, double score, byte[] key) throws VerificationException {
        return verifiedZAddAt(set, score, key, 0);
    }

    public TxHeader verifiedZAddAt(String set, double score, String key, long atTx) throws VerificationException {
        return verifiedZAddAt(set.getBytes(StandardCharsets.UTF_8), score, key.getBytes(StandardCharsets.UTF_8), atTx);
    }

    public TxHeader verifiedZAddAt(byte[] set, double score, byte[] key, long atTx) throws VerificationException {
        final ImmuState state = state();

        final ImmudbProto.ZAddRequest zAddReq = ImmudbProto.ZAddRequest.newBuilder()
                .setSet(ByteString.copyFrom(set))
                .setScore(score)
                .setKey(ByteString.copyFrom(key))
                .setAtTx(atTx)
                .build();
        
        final ImmudbProto.VerifiableZAddRequest vZAddReq = ImmudbProto.VerifiableZAddRequest.newBuilder()
                .setZAddRequest(zAddReq)
                .setProveSinceTx(state.txId)
                .build();
        
        final ImmudbProto.VerifiableTx vtx = getStub().verifiableZAdd(vZAddReq);

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

        final KV kvEntry = CryptoUtils.encodeZAdd(set, score, CryptoUtils.encodeKey(key), atTx);

        InclusionProof inclusionProof = tx.proof(kvEntry.getKey());

        if (!CryptoUtils.verifyInclusion(inclusionProof, kvEntry.digestFor(txHeader.getVersion()), txHeader.getEh())) {
            throw new VerificationException("Data is corrupted (inclusion verification failed).");
        }

        if (!Arrays.equals(txHeader.getEh(), CryptoUtils.digestFrom(vtx.getDualProof().getTargetTxHeader().getEH().toByteArray()))) {
            throw new VerificationException("Data is corrupted (different digests).");
        }

        final ImmuState newState = verifyDualProof(vtx, tx, state);

        if (!checkSignature(state)) {
            throw new RuntimeException("State signature verification failed");
        }

        stateHolder.setState(currentServerUuid, newState);

        return TxHeader.valueOf(vtx.getTx().getHeader());
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
        final ImmudbProto.ZScanRequest req = ImmudbProto.ZScanRequest
                .newBuilder()
                .setSet(ByteString.copyFrom(set))
                .setLimit(limit)
                .setSinceTx(sinceTxId)
                .setDesc(reverse)
                .build();

        final ImmudbProto.ZEntries zEntries = getStub().zScan(req);

        return buildList(zEntries);
    }

    //
    // ========== TX ==========
    //

    public Tx txById(long txId) throws MaxWidthExceededException, NoSuchAlgorithmException {
        final ImmudbProto.Tx tx = getStub().txById(ImmudbProto.TxRequest.newBuilder().setTx(txId).build());
        return Tx.valueOf(tx);
    }

    public Tx verifiedTxById(long txId) throws VerificationException {
        final ImmuState state = state();
        final ImmudbProto.VerifiableTxRequest vTxReq = ImmudbProto.VerifiableTxRequest.newBuilder()
                .setTx(txId)
                .setProveSinceTx(state.txId)
                .build();
        
        final ImmudbProto.VerifiableTx vtx = getStub().verifiableTxById(vTxReq);

        final DualProof dualProof = DualProof.valueOf(vtx.getDualProof());

        long sourceId;
        long targetId;
        byte[] sourceAlh;
        byte[] targetAlh;

        if (state.txId <= txId) {
            sourceId = state.txId;
            sourceAlh = CryptoUtils.digestFrom(state.txHash);
            targetId = txId;
            targetAlh = dualProof.targetTxHeader.alh();
        } else {
            sourceId = txId;
            sourceAlh = dualProof.sourceTxHeader.alh();
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

        Tx tx;
        try {
            tx = Tx.valueOf(vtx.getTx());
        } catch (Exception e) {
            throw new VerificationException("Failed to extract the transaction.", e);
        }

        final ImmuState newState = new ImmuState(currentDb, targetId, targetAlh, vtx.getSignature().getSignature().toByteArray());

        if (!checkSignature(state)) {
            throw new RuntimeException("State signature verification failed");
        }

        stateHolder.setState(currentServerUuid, newState);

        return tx;
    }

    public List<Tx> txScan(long initialTxId) {
        final ImmudbProto.TxScanRequest req = ImmudbProto.TxScanRequest.newBuilder().setInitialTx(initialTxId).build();
        final ImmudbProto.TxList txList = getStub().txScan(req);
        return buildList(txList);
    }

    public List<Tx> txScan(long initialTxId, int limit, boolean desc) {
        final ImmudbProto.TxScanRequest req = ImmudbProto.TxScanRequest
                .newBuilder()
                .setInitialTx(initialTxId)
                .setLimit(limit)
                .setDesc(desc)
                .build();
        final ImmudbProto.TxList txList = getStub().txScan(req);
        return buildList(txList);
    }

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
        final ImmudbProto.UserList userList = getStub().listUsers(Empty.getDefaultInstance());

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
        final ImmudbProto.CreateUserRequest createUserRequest = ImmudbProto.CreateUserRequest.newBuilder()
                .setUser(ByteString.copyFrom(user, StandardCharsets.UTF_8))
                .setPassword(ByteString.copyFrom(password, StandardCharsets.UTF_8))
                .setPermission(permission.permissionCode)
                .setDatabase(database)
                .build();

        // noinspection ResultOfMethodCallIgnored
        getStub().createUser(createUserRequest);
    }

    public void changePassword(String user, String oldPassword, String newPassword) {
        final ImmudbProto.ChangePasswordRequest changePasswordRequest = ImmudbProto.ChangePasswordRequest.newBuilder()
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

    public void compactIndex() {
        //noinspection ResultOfMethodCallIgnored
        getStub().compactIndex(Empty.getDefaultInstance());
    }

    //
    // ========== INTERNAL UTILS ==========
    //

    private List<KV> buildList(ImmudbProto.Entries entries) {
        final List<KV> result = new ArrayList<>(entries.getEntriesCount());
        entries.getEntriesList()
                .forEach(entry -> result.add(KVPair.from(entry)));
        return result;
    }

    private List<KV> buildList(ImmudbProto.ZEntries entries) {
        final List<KV> result = new ArrayList<>(entries.getEntriesCount());
        entries.getEntriesList()
                .forEach(entry -> result.add(KVPair.from(entry)));
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
