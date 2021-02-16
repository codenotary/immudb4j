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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import io.codenotary.immudb.ImmuServiceGrpc;
import io.codenotary.immudb.ImmudbProto;
import io.codenotary.immudb.ImmudbProto.ScanRequest;
import io.codenotary.immudb4j.crypto.*;
import io.codenotary.immudb4j.exceptions.CorruptedDataException;
import io.codenotary.immudb4j.exceptions.VerificationException;
import io.codenotary.immudb4j.user.Permission;
import io.codenotary.immudb4j.user.User;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;

import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * immudb client using grpc.
 *
 * @author Jeronimo Irazabal
 */
public class ImmuClient {

    private static final String AUTH_HEADER = "authorization";
    private final ImmuServiceGrpc.ImmuServiceBlockingStub stub;
    private final boolean withAuthToken;
    private final ImmuStateHolder stateHolder;
    private ManagedChannel channel;
    private String authToken;
    private String currentDb = "defaultdb";

    /**
     * ECDSA Public Key of the server, used for signing the state.
     */
    private PublicKey serverSigningPubKey;

    public ImmuClient(Builder builder) {
        this.stub = createStubFrom(builder);
        this.withAuthToken = builder.isWithAuthToken();
        this.stateHolder = builder.getStateHolder();
        this.serverSigningPubKey = builder.getServerSigningPubKey();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private ImmuServiceGrpc.ImmuServiceBlockingStub createStubFrom(Builder builder) {
        channel = ManagedChannelBuilder.forAddress(builder.getServerUrl(), builder.getServerPort())
                .usePlaintext()
                .build();
        return ImmuServiceGrpc.newBlockingStub(channel);
    }

    public synchronized void shutdown() {
        channel.shutdown();
        channel = null;
    }

    public synchronized boolean isShutdown() {
        return channel == null;
    }

    private ImmuServiceGrpc.ImmuServiceBlockingStub getStub() {
        if (!withAuthToken || authToken == null) {
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
        ImmuState state = stateHolder.getState(currentDb);
        if (state == null) {
            state = currentState();
            stateHolder.setState(state);
        }
        return state;
    }

    /**
     * Get the current database state that exists on the server.
     */
    public ImmuState currentState() {
        Empty empty = com.google.protobuf.Empty.getDefaultInstance();
        ImmudbProto.ImmutableState state = getStub().currentState(empty);
        return new ImmuState(
                currentDb,
                state.getTxId(),
                state.getTxHash().toByteArray(),
                state.getSignature().toByteArray()
        );
    }

    // ========== DATABASE ==========

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

    // ========== GET ==========

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

    public List<KV> getAllBytes(List<byte[]> keys) {
        List<ByteString> keysBS = new ArrayList<>(keys.size());
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

    public KV getAt(byte[] key, int txId) {
        ImmudbProto.Entry entry = getStub()
                .get(ImmudbProto.KeyRequest.newBuilder().setKey(ByteString.copyFrom(key)).setAtTx(txId).build());
        return KVPair.from(entry);
    }

    public KV getSince(byte[] key, int txId) {
        ImmudbProto.Entry entry = getStub()
                .get(ImmudbProto.KeyRequest.newBuilder().setKey(ByteString.copyFrom(key)).setSinceTx(txId).build());
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
        // TODO: TBD Anything like a Go's stateService.cacheLock()

        ImmuState state = state();

        ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .build();
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
            kv = CryptoUtils.encodeKV(keyReq.getKey().toByteArray(), entry.getValue().toByteArray());
        } else {
            ImmudbProto.Reference entryRefBy = entry.getReferencedBy();
            vTx = entryRefBy.getTx();
            kv = CryptoUtils.encodeReference(entryRefBy.getKey().toByteArray(), entry.getKey().toByteArray(), entryRefBy.getAtTx());
        }

        if (state.txId <= vTx) {
            eh = CryptoUtils.digestFrom(vEntry.getVerifiableTx().getDualProof().getTargetTxMetadata().getEH().toByteArray());

            sourceId = state.txId;
            sourceAlh = CryptoUtils.digestFrom(state.txHash);
            targetId = vTx;
            targetAlh = dualProof.targetTxMetadata.alh();
        } else {
            eh = CryptoUtils.digestFrom(vEntry.getVerifiableTx().getDualProof().getSourceTxMetadata().getEH().toByteArray());

            sourceId = vTx;
            sourceAlh = dualProof.sourceTxMetadata.alh();
            targetId = state.txId;
            targetAlh = CryptoUtils.digestFrom(state.txHash);
        }

        if (!CryptoUtils.verifyInclusion(inclusionProof, kv, eh)) {
            throw new VerificationException("inclusion verification failed");
        }

        if (state.txId > 0) {
            if (!CryptoUtils.verifyDualProof(
                    dualProof,
                    sourceId,
                    targetId,
                    sourceAlh,
                    targetAlh
            )) {
                throw new VerificationException("dual proof verification failed");
            }
        }

        ImmuState newState = new ImmuState(
                currentDb,
                targetId,
                targetAlh,
                vEntry.getVerifiableTx().getSignature().toByteArray());

        if (serverSigningPubKey != null) {
            // TODO: to-be-implemented, see pkg/client/client.go:620
        }

        stateHolder.setState(newState);

        return Entry.valueOf(vEntry.getEntry());
    }

    // ========== HISTORY ==========

    public List<KV> history(String key, int limit, long offset, boolean reverse) {
        return history(key.getBytes(StandardCharsets.UTF_8), limit, offset, reverse);
    }

    public List<KV> history(byte[] key, int limit, long offset, boolean reverse) {
        ImmudbProto.Entries entries;
        try {
            entries = getStub().history(ImmudbProto.HistoryRequest
                    .newBuilder()
                    .setKey(ByteString.copyFrom(key))
                    .setLimit(limit)
                    .setOffset(offset)
                    .setDesc(reverse)
                    .build()
            );
        } catch (StatusRuntimeException e) {
            return new ArrayList<>(0);
        }
        return buildList(entries);
    }

    // ========== SCAN ==========

    public List<KV> scan(String key) {
        return scan(ByteString.copyFrom(key, StandardCharsets.UTF_8).toByteArray());
    }

    public List<KV> scan(String key, long sinceTxId, long limit, boolean reverse) {
        return scan(ByteString.copyFrom(key, StandardCharsets.UTF_8).toByteArray(), sinceTxId, limit, reverse);
    }

    public List<KV> scan(byte[] key) {
        ScanRequest req = ScanRequest.newBuilder().setPrefix(ByteString.copyFrom(key)).build();
        ImmudbProto.Entries entries = getStub().scan(req);
        return buildList(entries);
    }

    public List<KV> scan(byte[] key, long sinceTxId, long limit, boolean reverse) {
        ScanRequest req = ScanRequest
                .newBuilder()
                .setPrefix(ByteString.copyFrom(key))
                .setLimit(limit)
                .setSinceTx(sinceTxId)
                .setDesc(reverse)
                .build();
        ImmudbProto.Entries entries = getStub().scan(req);
        return buildList(entries);
    }

    // ========== SET ==========

    public void set(String key, byte[] value) {
        set(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public void set(byte[] key, byte[] value) {
        ImmudbProto.KeyValue kv = ImmudbProto.KeyValue
                .newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build();
        ImmudbProto.SetRequest req = ImmudbProto.SetRequest.newBuilder().addKVs(kv).build();
        // noinspection ResultOfMethodCallIgnored
        getStub().set(req);
    }

    public void setAll(KVList kvList) {
        ImmudbProto.SetRequest.Builder reqBuilder = ImmudbProto.SetRequest.newBuilder();
        for (KV kv : kvList.entries()) {
            ImmudbProto.KeyValue schemaKV = ImmudbProto.KeyValue
                    .newBuilder()
                    .setKey(ByteString.copyFrom(kv.getKey()))
                    .setValue(ByteString.copyFrom(kv.getValue()))
                    .build();
            reqBuilder.addKVs(schemaKV);
        }
        // noinspection ResultOfMethodCallIgnored
        getStub().set(reqBuilder.build());
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
        ImmudbProto.Entry entry = ImmudbProto.Entry
                .newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build();
        // safeRawSet(key, entry.toByteArray(), this.state());
    }

    public void verifiedSet(String key, byte[] value) throws VerificationException {
        verifiedSet(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public void verifiedSet(byte[] key, byte[] value) throws VerificationException {

        ImmuState state = state();
        ImmudbProto.KeyValue kv = ImmudbProto.KeyValue.newBuilder().setKey(ByteString.copyFrom(key)).setValue(ByteString.copyFrom(value)).build();
        ImmudbProto.VerifiableSetRequest vSetReq = ImmudbProto.VerifiableSetRequest.newBuilder()
                .setSetRequest(ImmudbProto.SetRequest.newBuilder().addKVs(kv).build())
                .setProveSinceTx(state.txId)
                // TBD grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD),
                .build();
        ImmudbProto.VerifiableTx vtx = getStub().verifiableSet(vSetReq);
        int ne = vtx.getTx().getMetadata().getNentries();
        if (ne != 1) {
            throw new VerificationException(
                    String.format("Got back %d entries (in tx metadata) instead of 1.", ne)
            );
        }

    }

    // ========== Z ==========

    public ImmudbProto.TxMetadata zAdd(String set, String key, double score) throws CorruptedDataException {
        return zAddAt(set, key, score, 0);
    }

    public ImmudbProto.TxMetadata zAddAt(String set, String key, double score, long atTxId)
            throws CorruptedDataException {
        ImmudbProto.TxMetadata txMd = getStub()
                .zAdd(
                        ImmudbProto.ZAddRequest
                                .newBuilder()
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
        return txMd;
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

    // ========== TX ==========

    public Tx txById(long txId) {
        ImmudbProto.Tx tx = getStub().txById(ImmudbProto.TxRequest.newBuilder().setTx(txId).build());
        return Tx.valueOf(tx);
    }

    public List<Tx> txScan(long initialTxId) {
        ImmudbProto.TxScanRequest req = ImmudbProto.TxScanRequest.newBuilder().setInitialTx(initialTxId).build();
        ImmudbProto.TxList txList = getStub().txScan(req);
        return buildList(txList);
    }

    public List<Tx> txScan(long initialTxId, int limit, boolean reverse) {
        ImmudbProto.TxScanRequest req = ImmudbProto.TxScanRequest
                .newBuilder()
                .setInitialTx(initialTxId)
                .setLimit(limit)
                .setDesc(reverse)
                .build();
        ImmudbProto.TxList txList = getStub().txScan(req);
        return buildList(txList);
    }

    // ========== COUNT ==========

    public long count(String prefix) {
        return count(prefix.getBytes(StandardCharsets.UTF_8));
    }

    public long count(byte[] prefix) {
        return getStub()
                .count(ImmudbProto.KeyPrefix.newBuilder().setPrefix(ByteString.copyFrom(prefix)).build())
                .getCount();
    }

    public long countAll() {
        return getStub().countAll(Empty.getDefaultInstance()).getCount();
    }

    // ========== HEALTH ==========

    public boolean healthCheck() {
        return getStub().health(Empty.getDefaultInstance()).getStatus();
    }

    public boolean isConnected() {
        return channel != null;
    }

    // ========== USER MGMT ==========

    public List<User> listUsers() {
        ImmudbProto.UserList userList = getStub().listUsers(Empty.getDefaultInstance());

        return userList
                .getUsersList()
                .stream()
                .map(
                        u ->
                                User
                                        .getBuilder()
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
        ImmudbProto.CreateUserRequest createUserRequest = ImmudbProto.CreateUserRequest
                .newBuilder()
                .setUser(ByteString.copyFrom(user, StandardCharsets.UTF_8))
                .setPassword(ByteString.copyFrom(password, StandardCharsets.UTF_8))
                .setPermission(permission.permissionCode)
                .setDatabase(database)
                .build();

        // noinspection ResultOfMethodCallIgnored
        getStub().createUser(createUserRequest);
    }

    public void changePassword(String user, String oldPassword, String newPassword) {
        ImmudbProto.ChangePasswordRequest changePasswordRequest = ImmudbProto.ChangePasswordRequest
                .newBuilder()
                .setUser(ByteString.copyFrom(user, StandardCharsets.UTF_8))
                .setOldPassword(ByteString.copyFrom(oldPassword, StandardCharsets.UTF_8))
                .setNewPassword(ByteString.copyFrom(newPassword, StandardCharsets.UTF_8))
                .build();

        // noinspection ResultOfMethodCallIgnored
        getStub().changePassword(changePasswordRequest);
    }

    // ========== INTERNAL UTILS ==========

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
        txList.getTxsList().forEach(tx -> result.add(Tx.valueOf(tx)));
        return result;
    }

    // ========== BUILDER ==========

    public static class Builder {

        private String serverUrl;

        private int serverPort;

        private PublicKey serverSigningPubKey;

        private boolean withAuthToken;

        private ImmuStateHolder stateHolder;

        private Builder() {
            this.serverUrl = "localhost";
            this.serverPort = 3322;
            this.stateHolder = new SerializableImmuStateHolder();
            this.withAuthToken = true;
        }

        public ImmuClient build() {
            return new ImmuClient(this);
        }

        public String getServerUrl() {
            return this.serverUrl;
        }

        public Builder setServerUrl(String serverUrl) {
            this.serverUrl = serverUrl;
            return this;
        }

        public int getServerPort() {
            return serverPort;
        }

        public Builder setServerPort(int serverPort) {
            this.serverPort = serverPort;
            return this;
        }

        public PublicKey getServerSigningPubKey() {
            return serverSigningPubKey;
        }

        public Builder setServerSigningPubKey(PublicKey serverSigningPubKey) {
            this.serverSigningPubKey = serverSigningPubKey;
            return this;
        }

        public boolean isWithAuthToken() {
            return withAuthToken;
        }

        public Builder setWithAuthToken(boolean withAuthToken) {
            this.withAuthToken = withAuthToken;
            return this;
        }

        public ImmuStateHolder getStateHolder() {
            return stateHolder;
        }

        public Builder setStateHolder(ImmuStateHolder stateHolder) {
            this.stateHolder = stateHolder;
            return this;
        }
    }
}
