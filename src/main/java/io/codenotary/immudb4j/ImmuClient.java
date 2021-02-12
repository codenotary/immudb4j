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
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import io.codenotary.immudb.ImmuServiceGrpc;
import io.codenotary.immudb.ImmudbProto;
import io.codenotary.immudb.ImmudbProto.ScanRequest;
import io.codenotary.immudb4j.crypto.ImmutableState;
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
    private final ImmutableStateHolder stateHolder;
    private ManagedChannel channel;
    private String authToken;
    private String activeDatabase = "defaultdb";

    public ImmuClient(Builder builder) {
        this.stub = createStubFrom(builder);
        this.withAuthToken = builder.isWithAuthToken();
        this.stateHolder = builder.getStateHolder();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    private ImmuServiceGrpc.ImmuServiceBlockingStub createStubFrom(Builder builder) {
        channel =
            ManagedChannelBuilder.forAddress(builder.getServerUrl(), builder.getServerPort()).usePlaintext().build();
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

    public ImmutableState state() {
        if (stateHolder.getState(activeDatabase) == null) {
            Empty empty = com.google.protobuf.Empty.getDefaultInstance();
            ImmudbProto.ImmutableState state = getStub().currentState(empty);
            ImmutableState currState = new ImmutableState(
                activeDatabase,
                state.getTxId(),
                state.getTxHash().toByteArray()
            );
            stateHolder.setState(currState);
        }
        return stateHolder.getState(activeDatabase);
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
        activeDatabase = database;
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

    public byte[] safeGet(String key) throws VerificationException {
        return safeGet(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] safeGet(byte[] key) throws VerificationException {
        // try {
        // ImmudbProto.Entry entry = ImmudbProto.Entry.parseFrom(safeRawGet(key,
        // this.state()));
        // return entry.toByteArray();
        // } catch (InvalidProtocolBufferException e) {
        // throw new RuntimeException(e);
        // }
        return Empty.getDefaultInstance().toByteArray();
    }

    // public byte[] rawGet(String key) {
    // return rawGet(key.getBytes(StandardCharsets.UTF_8));
    // }

    public byte[] verifiedGet(byte[] key, ImmutableState state)
        throws VerificationException, InvalidProtocolBufferException {
        // ImmudbProto.Index index =
        // ImmudbProto.Index.newBuilder().setIndex(state.getIndex()).build();
        //
        // ImmudbProto.SafeGetOptions sOpts =
        // ImmudbProto.SafeGetOptions.newBuilder()
        // .setKey(ByteString.copyFrom(key))
        // .setRootIndex(index)
        // .build();
        //
        // ImmudbProto.SafeItem safeItem = getStub().safeGet(sOpts);
        // ImmudbProto.Proof proof = safeItem.getProof();
        // CryptoUtils.verify(proof, safeItem.getItem(), state);
        //
        // stateHolder.setRoot(new Root(activeDatabase, proof.getAt(),
        // proof.getRoot().toByteArray()));
        //
        // return safeItem.getItem().getValue().toByteArray();

        ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest
            .newBuilder()
            .setKey(ByteString.copyFrom(key))
            .setSinceTx(state.getTxId())
            .build();
        ImmudbProto.VerifiableGetRequest verifGetReq = ImmudbProto.VerifiableGetRequest
            .newBuilder()
            .setKeyRequest(keyReq)
            .setProveSinceTx(state.getTxId())
            .build();
        ImmudbProto.VerifiableEntry vEntry = getStub().verifiableGet(verifGetReq);

        ImmudbProto.InclusionProof inclProof = vEntry.getInclusionProof();

        // Schema.InclusionProof schInclProof =
        // Schema.InclusionProof.parseFrom(inclProof.toByteArray());
        // Schema.DualProof dualProof =
        // Schema.DualProof.parseFrom(vEntry.getVerifiableTx().getDualProof().toByteArray());

        // TODO:
        // CryptoUtils.verify(inclProof, verifEntry.getEntry(), state);
        // stateHolder.setState(new ImmutableState(activeDatabase, inclProof.getLeaf(),
        // inclProof...));

        return vEntry.getEntry().getValue().toByteArray();
    }

    // ========== HISTORY ==========

    public List<KV> history(String key, int limit, long offset, boolean reverse) {
        return history(key.getBytes(StandardCharsets.UTF_8), limit, offset, reverse);
    }

    public List<KV> history(byte[] key, int limit, long offset, boolean reverse) {
        ImmudbProto.Entries entries;
        try {
            entries =
                getStub()
                    .history(
                        ImmudbProto.HistoryRequest
                            .newBuilder()
                            .setKey(ByteString.copyFrom(key))
                            .setLimit(limit)
                            .setOffset(offset)
                            .setDesc(reverse)
                            .build()
                    );
        } catch (StatusRuntimeException e) {
            return new ArrayList<KV>(0);
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

    public void safeSet(String key, byte[] value) throws VerificationException {
        safeSet(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public void safeSet(byte[] key, byte[] value) throws VerificationException {
        ImmudbProto.Entry entry = ImmudbProto.Entry
            .newBuilder()
            .setKey(ByteString.copyFrom(key))
            .setValue(ByteString.copyFrom(value))
            .build();
        // safeRawSet(key, entry.toByteArray(), this.state());
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
        return zScan(set, limit, reverse);
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

    public Transaction txById(long txId) {
        ImmudbProto.Tx tx = getStub().txById(ImmudbProto.TxRequest.newBuilder().setTx(txId).build());
        return Transaction.valueOf(tx);
    }

    public List<Transaction> txScan(long initialTxId) {
        ImmudbProto.TxScanRequest req = ImmudbProto.TxScanRequest.newBuilder().setInitialTx(initialTxId).build();
        ImmudbProto.TxList txList = getStub().txScan(req);
        return buildList(txList);
    }

    public List<Transaction> txScan(long initialTxId, int limit, boolean reverse) {
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
        entries
            .getEntriesList()
            .forEach(
                entry -> {
                    result.add(KVPair.from(entry));
                }
            );
        return result;
    }

    private List<KV> buildList(ImmudbProto.ZEntries entries) {
        List<KV> result = new ArrayList<>(entries.getEntriesCount());
        entries
            .getEntriesList()
            .forEach(
                entry -> {
                    result.add(KVPair.from(entry));
                }
            );
        return result;
    }

    private List<Transaction> buildList(ImmudbProto.TxList txList) {
        List<Transaction> result = new ArrayList<>(txList.getTxsCount());
        txList.getTxsList().forEach(tx -> result.add(Transaction.valueOf(tx)));
        return result;
    }

    // ========== BUILDER ==========

    public static class Builder {

        private String serverUrl;

        private int serverPort;

        private boolean withAuthToken;

        private ImmutableStateHolder stateHolder;

        private Builder() {
            this.serverUrl = "localhost";
            this.serverPort = 3322;
            this.stateHolder = new SerializableImmutableStateHolder();
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

        public boolean isWithAuthToken() {
            return withAuthToken;
        }

        public Builder setWithAuthToken(boolean withAuthToken) {
            this.withAuthToken = withAuthToken;
            return this;
        }

        public ImmutableStateHolder getStateHolder() {
            return stateHolder;
        }

        public Builder setStateHolder(ImmutableStateHolder stateHolder) {
            this.stateHolder = stateHolder;
            return this;
        }
    }
}
