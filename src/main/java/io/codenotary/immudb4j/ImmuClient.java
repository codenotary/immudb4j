/*
Copyright 2019-2020 vChain, Inc.

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
import io.codenotary.immudb4j.crypto.CryptoUtils;
import io.codenotary.immudb4j.crypto.ImmutableState;
import io.codenotary.immudb4j.crypto.VerificationException;
import io.codenotary.immudb4j.user.Permission;
import io.codenotary.immudb4j.user.User;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
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
                ManagedChannelBuilder.forAddress(builder.getServerUrl(), builder.getServerPort())
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
        ImmudbProto.LoginRequest loginRequest =
                ImmudbProto.LoginRequest.newBuilder()
                        .setUser(ByteString.copyFrom(username, StandardCharsets.UTF_8))
                        .setPassword(ByteString.copyFrom(password, StandardCharsets.UTF_8))
                        .build();

        ImmudbProto.LoginResponse loginResponse = getStub().login(loginRequest);
        authToken = loginResponse.getToken();
    }

    public synchronized void logout() {
        //noinspection ResultOfMethodCallIgnored
        getStub().logout(com.google.protobuf.Empty.getDefaultInstance());
        authToken = null;
    }

    public ImmutableState state() {
        if (stateHolder.getState(activeDatabase) == null) {
            Empty empty = com.google.protobuf.Empty.getDefaultInstance();
            ImmudbProto.ImmutableState state = getStub().currentState(empty);
            ImmutableState currState = new ImmutableState(activeDatabase, state.getTxId(), state.getTxHash().toByteArray());
            stateHolder.setState(currState);
        }
        return stateHolder.getState(activeDatabase);
    }

    public void createDatabase(String database) {
        ImmudbProto.Database db = ImmudbProto.Database.newBuilder()
                .setDatabasename(database).build();
        //noinspection ResultOfMethodCallIgnored
        getStub().createDatabase(db);
    }

    public synchronized void useDatabase(String database) {
        ImmudbProto.Database db = ImmudbProto.Database.newBuilder()
                .setDatabasename(database).build();
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

    public void set(String key, byte[] value) {
        set(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public void set(byte[] key, byte[] value) {
        ImmudbProto.Entry entry = ImmudbProto.Entry.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                // TODO
                // .setTx(...)
                .build();
        rawSet(key, entry.toByteArray());
    }

    public byte[] get(String key) {
        return get(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] get(byte[] key) {
        try {
            ImmudbProto.Entry entry = ImmudbProto.Entry.parseFrom(rawGet(key));
            return entry.toByteArray();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] safeGet(String key) throws VerificationException {
        return safeGet(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] safeGet(byte[] key) throws VerificationException {
        try {
            ImmudbProto.Entry entry = ImmudbProto.Entry.parseFrom(safeRawGet(key, this.state()));
            return entry.toByteArray();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public void safeSet(String key, byte[] value) throws VerificationException {
        safeSet(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public void safeSet(byte[] key, byte[] value) throws VerificationException {
        ImmudbProto.Entry entry = ImmudbProto.Entry.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build();
        safeRawSet(key, entry.toByteArray(), this.state());
    }

    public void rawSet(String key, byte[] value) {
        rawSet(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public void rawSet(byte[] key, byte[] value) {
        ImmudbProto.KeyValue kv = ImmudbProto.KeyValue.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .build();
        ImmudbProto.SetRequest req = ImmudbProto.SetRequest.newBuilder().addKVs(kv).build();
        //noinspection ResultOfMethodCallIgnored
        getStub().set(req);
    }

    public byte[] rawGet(String key) {
        return rawGet(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] rawGet(byte[] key) {
        ImmudbProto.KeyRequest req = ImmudbProto.KeyRequest.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .build();
        ImmudbProto.Entry entry = getStub().get(req);
        return entry.getValue().toByteArray();
    }

    public byte[] safeRawGet(String key) throws VerificationException {
        return safeRawGet(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] safeRawGet(byte[] key) throws VerificationException {
        return safeRawGet(key, this.state());
    }

    public byte[] safeRawGet(byte[] key, ImmutableState state) throws VerificationException {

//        ImmudbProto.Index index = ImmudbProto.Index.newBuilder().setIndex(state.getIndex()).build();
//
//        ImmudbProto.SafeGetOptions sOpts =
//                ImmudbProto.SafeGetOptions.newBuilder()
//                        .setKey(ByteString.copyFrom(key))
//                        .setRootIndex(index)
//                        .build();
//
//        ImmudbProto.SafeItem safeItem = getStub().safeGet(sOpts);
//
//        ImmudbProto.Proof proof = safeItem.getProof();
//
//        CryptoUtils.verify(proof, safeItem.getItem(), state);
//
//        stateHolder.setRoot(new Root(activeDatabase, proof.getAt(), proof.getRoot().toByteArray()));
//
//        return safeItem.getItem().getValue().toByteArray();

        ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setSinceTx(state.getTxId())
                .build();
        ImmudbProto.VerifiableGetRequest verifGetReq = ImmudbProto.VerifiableGetRequest.newBuilder()
                .setKeyRequest(keyReq)
                .setProveSinceTx(state.getTxId())
                .build();
        ImmudbProto.VerifiableEntry verifEntry = getStub().verifiableGet(verifGetReq);

        ImmudbProto.InclusionProof inclProof = verifEntry.getInclusionProof();

        // TODO:
        // CryptoUtils.verify(inclProof, verifEntry.getEntry(), state);
        // stateHolder.setState(new ImmutableState(activeDatabase, inclProof.getLeaf(), inclProof...));

        return verifEntry.getEntry().getValue().toByteArray();
    }

    public void safeRawSet(String key, byte[] value) throws VerificationException {
        safeRawSet(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public void safeRawSet(byte[] key, byte[] value) throws VerificationException {
        safeRawSet(key, value, this.state());
    }

    public void safeRawSet(byte[] key, byte[] value, ImmutableState state) throws VerificationException {
        ImmudbProto.KeyValue kv =
                ImmudbProto.KeyValue.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(value))
                        .build();

        ImmudbProto.SafeSetOptions sOpts =
                ImmudbProto.SafeSetOptions.newBuilder()
                        .setKv(kv)
                        .setRootIndex(ImmudbProto.Index.newBuilder().setIndex(state.getIndex()).build())
                        .build();

        ImmudbProto.Proof proof = getStub().safeSet(sOpts);

        ImmudbProto.Item item =
                ImmudbProto.Item.newBuilder()
                        .setIndex(proof.getIndex())
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(value))
                        .build();

        CryptoUtils.verify(proof, item, state);

        stateHolder.setRoot(new Root(activeDatabase, proof.getAt(), proof.getRoot().toByteArray()));
    }

    public void setAll(KVList kvList) {
        KVList.KVListBuilder svListBuilder = KVList.newBuilder();

        for (KV kv : kvList.entries()) {
            ImmudbProto.Content content = ImmudbProto.Content.newBuilder()
                    .setTimestamp(System.currentTimeMillis() / 1000L)
                    .setPayload(ByteString.copyFrom(kv.getValue()))
                    .build();
            svListBuilder.add(kv.getKey(), content.toByteArray());

            ImmudbProto.Entry entry = ImmudbProto.Entry.newBuilder()
                    .setKey()
        }

        rawSetAll(svListBuilder.build());
    }

    public void rawSetAll(KVList kvList) {
        ImmudbProto.KVList.Builder builder = ImmudbProto.KVList.newBuilder();

        for (KV kv : kvList.entries()) {
            ImmudbProto.KeyValue skv =
                    ImmudbProto.KeyValue.newBuilder()
                            .setKey(ByteString.copyFrom(kv.getKey()))
                            .setValue(ByteString.copyFrom(kv.getValue()))
                            .build();

            builder.addKVs(skv);
        }

        //noinspection ResultOfMethodCallIgnored
        getStub().setBatch(builder.build());
    }

    public List<KV> getAll(List<?> keyList) {
        List<KV> rawKVs = rawGetAll(keyList);

        return convertToStructuredKVList(rawKVs);
    }

    private List<KV> convertToStructuredKVList(List<KV> rawKVs) {
        assert rawKVs != null;
        List<KV> kvs = new ArrayList<>(rawKVs.size());

        for (KV rawKV : rawKVs) {
            try {
                ImmudbProto.Content content = ImmudbProto.Content.parseFrom(rawKV.getValue());
                kvs.add(new KVPair(rawKV.getKey(), content.getPayload().toByteArray()));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
        return kvs;
    }

    public List<KV> rawGetAll(List<?> keyList) {
        if (keyList == null) {
            throw new RuntimeException("Illegal argument");
        }

        if (keyList.isEmpty()) {
            return new ArrayList<>();
        }

        if (keyList.get(0) instanceof String) {
            List<byte[]> kList = new ArrayList<>(keyList.size());

            for (Object key : keyList) {
                kList.add(((String) key).getBytes(StandardCharsets.UTF_8));
            }

            return rawGetAllFrom(kList);
        }

        if (keyList.get(0) instanceof byte[]) {
            //noinspection unchecked
            return rawGetAllFrom((List<byte[]>) keyList);
        }

        throw new RuntimeException("Illegal argument");
    }

    private List<KV> rawGetAllFrom(List<byte[]> keyList) {

        ImmudbProto.KeyList.Builder builder = ImmudbProto.KeyList.newBuilder();

        for (byte[] key : keyList) {
            ImmudbProto.Key k = ImmudbProto.Key.newBuilder().setKey(ByteString.copyFrom(key)).build();
            builder.addKeys(k);
        }

        ImmudbProto.ItemList res = getStub().getBatch(builder.build());

        return buildKVList(res);
    }

    public List<KV> history(String key, long limit, long offset, boolean reverse) {
        return history(key.getBytes(StandardCharsets.UTF_8), limit, offset, reverse);
    }

    public List<KV> history(byte[] key, long limit, long offset, boolean reverse) {
        return convertToStructuredKVList(rawHistory(key, limit, offset, reverse));
    }

    public List<KV> rawHistory(byte[] key, long limit, long offset, boolean reverse) {
        ImmudbProto.HistoryOptions h = ImmudbProto.HistoryOptions.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setLimit(limit)
                .setOffset(offset)
                .setReverse(reverse)
                .build();
        ImmudbProto.ItemList res = getStub().history(h);

        return buildKVList(res);
    }

    private List<KV> buildKVList(ImmudbProto.ItemList itemList) {
        List<KV> result = new ArrayList<>(itemList.getItemsCount());

        for (ImmudbProto.Item item : itemList.getItemsList()) {
            KV kv = new KVPair(item.getKey().toByteArray(), item.getValue().toByteArray());
            result.add(kv);
        }

        return result;
    }

    public List<KV> scan(String prefix, String offset, long limit, boolean reverse, boolean deep) {
        return convertToStructuredKVList(rawScan(prefix, offset, limit, reverse, deep));
    }

    public List<KV> scan(byte[] prefix, byte[] offset, long limit, boolean reverse, boolean deep) {
        return convertToStructuredKVList(rawScan(prefix, offset, limit, reverse, deep));
    }

    public List<KV> rawScan(String prefix, String offset, long limit, boolean reverse, boolean deep) {
        return rawScan(prefix.getBytes(StandardCharsets.UTF_8), offset.getBytes(StandardCharsets.UTF_8), limit, reverse, deep);
    }

    public List<KV> rawScan(byte[] prefix, byte[] offset, long limit, boolean reverse, boolean deep) {
        ImmudbProto.ScanOptions request = ImmudbProto.ScanOptions.newBuilder()
                .setPrefix(ByteString.copyFrom(prefix))
                .setOffset(ByteString.copyFrom(offset))
                .setLimit(limit)
                .setReverse(reverse)
                .setDeep(deep)
                .build();

        ImmudbProto.ItemList res = getStub().scan(request);
        return buildKVList(res);
    }

    public List<KV> zScan(String set, String offset, long limit, boolean reverse) {
        return zScan(set.getBytes(StandardCharsets.UTF_8), offset.getBytes(StandardCharsets.UTF_8), limit, reverse);
    }

    public List<KV> zScan(byte[] set, byte[] offset, long limit, boolean reverse) {
        return convertToStructuredKVList(rawZScan(set, offset, limit, reverse));
    }

    public List<KV> rawZScan(byte[] set, byte[] offset, long limit, boolean reverse) {
        ImmudbProto.ZScanOptions request = ImmudbProto.ZScanOptions.newBuilder()
                .setSet(ByteString.copyFrom(set))
                .setOffset(ByteString.copyFrom(offset))
                .setLimit(limit)
                .setReverse(reverse)
                .build();

        ImmudbProto.ZItemList res = getStub().zScan(request);

        return buildKVList(res);
    }

    private List<KV> buildKVList(ImmudbProto.ZItemList zItemList) {
        List<KV> result = new ArrayList<>(zItemList.getItemsCount());

        for (ImmudbProto.ZItem zItem : zItemList.getItemsList()) {
            KV kv = new KVPair(zItem.getItem().getKey().toByteArray(), zItem.getItem().getValue().toByteArray());
            result.add(kv);
        }

        return result;
    }

    public KVPage iScan(long pageNumber, long pageSize) {
        KVPage kvPage = rawIScan(pageNumber, pageSize);
        KVList structuredKvList = KVList.newBuilder()
                .addAll(convertToStructuredKVList(kvPage.getKvList().entries()))
                .build();

        return KVPage.newBuilder()
                .setKvList(structuredKvList)
                .setMore(kvPage.isMore())
                .build();
    }

    public KVPage rawIScan(long pageNumber, long pageSize) {
        ImmudbProto.IScanOptions request = ImmudbProto.IScanOptions.newBuilder()
                .setPageNumber(pageNumber)
                .setPageSize(pageSize)
                .build();

        ImmudbProto.Page page = getStub().iScan(request);
        return KVPage.newBuilder()
                .setKvList(buildKVList(page.getItemsList()))
                .setMore(page.getMore())
                .build();
    }

    private KVList buildKVList(List<ImmudbProto.Item> itemsList) {
        KVList result = KVList.newBuilder().build();

        for (ImmudbProto.Item item : itemsList) {
            KV kv = new KVPair(item.getKey().toByteArray(), item.getValue().toByteArray());
            result.entries().add(kv);
        }

        return result;
    }

    public void zAdd(String set, String key, double score) {
        ImmudbProto.Score scoreObject = ImmudbProto.Score.newBuilder()
                .setScore(score)
                .build();

        ImmudbProto.ZAddOptions options = ImmudbProto.ZAddOptions.newBuilder()
                .setSet(ByteString.copyFrom(set, StandardCharsets.UTF_8))
                .setScore(scoreObject)
                .setKey(ByteString.copyFrom(key, StandardCharsets.UTF_8))
                .build();
        //noinspection ResultOfMethodCallIgnored
        getStub().zAdd(options);
    }

    public void zAdd(String set, String key, double score, long index) {
        ImmudbProto.Score scoreObject = ImmudbProto.Score.newBuilder()
                .setScore(score)
                .build();

        ImmudbProto.ZAddOptions options = ImmudbProto.ZAddOptions.newBuilder()
                .setSet(ByteString.copyFrom(set, StandardCharsets.UTF_8))
                .setScore(scoreObject)
                .setKey(ByteString.copyFrom(key, StandardCharsets.UTF_8))
                .setIndex(ImmudbProto.Index.newBuilder()
                        .setIndex(index)
                        .build())
                .build();
        //noinspection ResultOfMethodCallIgnored
        getStub().zAdd(options);
    }

    public List<User> listUsers() {
        ImmudbProto.UserList userList = getStub().listUsers(Empty.getDefaultInstance());

        return userList.getUsersList().stream().map(u -> User.getBuilder()
                .setUser(u.getUser().toString(StandardCharsets.UTF_8))
                .setActive(u.getActive())
                .setCreatedAt(u.getCreatedat())
                .setCreatedBy(u.getCreatedby())
                .setPermissions(buildPermissions(u.getPermissionsList()))
                .build())
                .collect(Collectors.toList());
    }

    private List<Permission> buildPermissions(List<ImmudbProto.Permission> permissionsList) {
        return permissionsList.stream().map(
                p -> Permission.valueOfPermissionCode(p.getPermission())
        ).collect(Collectors.toList());
    }

    public void createUser(String user, String password, Permission permission, String database) {
        ImmudbProto.CreateUserRequest createUserRequest = ImmudbProto.CreateUserRequest.newBuilder()
                .setUser(ByteString.copyFrom(user, StandardCharsets.UTF_8))
                .setPassword(ByteString.copyFrom(password, StandardCharsets.UTF_8))
                .setPermission(permission.permissionCode)
                .setDatabase(database)
                .build();

        //noinspection ResultOfMethodCallIgnored
        getStub().createUser(createUserRequest);
    }

    public void changePassword(String user, String oldPassword, String newPassword) {
        ImmudbProto.ChangePasswordRequest changePasswordRequest = ImmudbProto.ChangePasswordRequest.newBuilder()
                .setUser(ByteString.copyFrom(user, StandardCharsets.UTF_8))
                .setOldPassword(ByteString.copyFrom(oldPassword, StandardCharsets.UTF_8))
                .setNewPassword(ByteString.copyFrom(newPassword, StandardCharsets.UTF_8))
                .build();

        //noinspection ResultOfMethodCallIgnored
        getStub().changePassword(changePasswordRequest);
    }

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
