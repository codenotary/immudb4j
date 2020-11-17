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
import io.codenotary.immudb4j.crypto.Root;
import io.codenotary.immudb4j.crypto.VerificationException;
import io.codenotary.immudb4j.user.Permission;
import io.codenotary.immudb4j.user.User;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
    private final RootHolder rootHolder;
    private ManagedChannel channel;
    private String authToken;
    private String activeDatabase = "defaultdb";


    public ImmuClient(ImmuClientBuilder builder) {
        this.stub = createStubFrom(builder);
        this.withAuthToken = builder.isWithAuthToken();
        this.rootHolder = builder.getRootHolder();
    }

    public static ImmuClientBuilder newBuilder() {
        return new ImmuClientBuilder();
    }

    private ImmuServiceGrpc.ImmuServiceBlockingStub createStubFrom(ImmuClientBuilder builder) {
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
        metadata.put(
                Metadata.Key.of(AUTH_HEADER, Metadata.ASCII_STRING_MARSHALLER), "Bearer " + authToken);

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

    public Root root() {
        if (rootHolder.getRoot(activeDatabase) == null) {
            Empty empty = com.google.protobuf.Empty.getDefaultInstance();
            ImmudbProto.Root r = getStub().currentRoot(empty);
            Root root = new Root(activeDatabase, r.getPayload().getIndex(), r.getPayload().getRoot().toByteArray());
            rootHolder.setRoot(root);
        }
        return rootHolder.getRoot(activeDatabase);
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
        ImmudbProto.Content content = ImmudbProto.Content.newBuilder()
                .setTimestamp(System.currentTimeMillis() / 1000L)
                .setPayload(ByteString.copyFrom(value))
                .build();
        this.rawSet(key, content.toByteArray());
    }

    public byte[] get(String key) {
        return get(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] get(byte[] key) {
        try {
            ImmudbProto.Content content = ImmudbProto.Content.parseFrom(rawGet(key));
            return content.getPayload().toByteArray();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] safeGet(String key) throws VerificationException {
        return safeGet(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] safeGet(byte[] key) throws VerificationException {
        try {
            ImmudbProto.Content content = ImmudbProto.Content.parseFrom(safeRawGet(key, this.root()));
            return content.getPayload().toByteArray();
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public void safeSet(String key, byte[] value) throws VerificationException {
        safeSet(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public void safeSet(byte[] key, byte[] value) throws VerificationException {
        ImmudbProto.Content content = ImmudbProto.Content.newBuilder()
                .setTimestamp(System.currentTimeMillis() / 1000L)
                .setPayload(ByteString.copyFrom(value))
                .build();

        safeRawSet(key, content.toByteArray(), this.root());
    }

    public void rawSet(String key, byte[] value) {
        rawSet(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public void rawSet(byte[] key, byte[] value) {
        ImmudbProto.KeyValue kv =
                ImmudbProto.KeyValue.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(value))
                        .build();

        //noinspection ResultOfMethodCallIgnored
        getStub().set(kv);
    }

    public byte[] rawGet(String key) {
        return rawGet(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] rawGet(byte[] key) {
        ImmudbProto.Key k = ImmudbProto.Key.newBuilder().setKey(ByteString.copyFrom(key)).build();

        ImmudbProto.Item item = getStub().get(k);
        return item.getValue().toByteArray();
    }

    public byte[] safeRawGet(String key) throws VerificationException {
        return safeRawGet(key.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] safeRawGet(byte[] key) throws VerificationException {
        return safeRawGet(key, this.root());
    }

    public byte[] safeRawGet(byte[] key, Root root) throws VerificationException {
        ImmudbProto.Index index = ImmudbProto.Index.newBuilder().setIndex(root.getIndex()).build();

        ImmudbProto.SafeGetOptions sOpts =
                ImmudbProto.SafeGetOptions.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .setRootIndex(index)
                        .build();

        ImmudbProto.SafeItem safeItem = getStub().safeGet(sOpts);

        ImmudbProto.Proof proof = safeItem.getProof();

        CryptoUtils.verify(proof, safeItem.getItem(), root);

        rootHolder.setRoot(new Root(activeDatabase, proof.getAt(), proof.getRoot().toByteArray()));

        return safeItem.getItem().getValue().toByteArray();
    }

    public void safeRawSet(String key, byte[] value) throws VerificationException {
        safeRawSet(key.getBytes(StandardCharsets.UTF_8), value);
    }

    public void safeRawSet(byte[] key, byte[] value) throws VerificationException {
        safeRawSet(key, value, this.root());
    }

    public void safeRawSet(byte[] key, byte[] value, Root root) throws VerificationException {
        ImmudbProto.KeyValue kv =
                ImmudbProto.KeyValue.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(value))
                        .build();

        ImmudbProto.SafeSetOptions sOpts =
                ImmudbProto.SafeSetOptions.newBuilder()
                        .setKv(kv)
                        .setRootIndex(ImmudbProto.Index.newBuilder().setIndex(root.getIndex()).build())
                        .build();

        ImmudbProto.Proof proof = getStub().safeSet(sOpts);

        ImmudbProto.Item item =
                ImmudbProto.Item.newBuilder()
                        .setIndex(proof.getIndex())
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(value))
                        .build();

        CryptoUtils.verify(proof, item, root);

        rootHolder.setRoot(new Root(activeDatabase, proof.getAt(), proof.getRoot().toByteArray()));
    }

    public void setAll(KVList kvList) {
        KVList.KVListBuilder svListBuilder = KVList.newBuilder();

        for (KV kv : kvList.entries()) {
            ImmudbProto.Content content = ImmudbProto.Content.newBuilder()
                    .setTimestamp(System.currentTimeMillis() / 1000L)
                    .setPayload(ByteString.copyFrom(kv.getValue()))
                    .build();
            svListBuilder.add(kv.getKey(), content.toByteArray());
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

    public List<KV> history(String key) {
        return history(key.getBytes(StandardCharsets.UTF_8));
    }

    public List<KV> history(byte[] key) {
        return convertToStructuredKVList(rawHistory(key));
    }

    public List<KV> rawHistory(byte[] key) {
        ImmudbProto.Key k = ImmudbProto.Key.newBuilder().setKey(ByteString.copyFrom(key)).build();
        ImmudbProto.ItemList res = getStub().history(k);

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

        ImmudbProto.ItemList res = getStub().zScan(request);

        return buildKVList(res);
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

    public void zAdd(String set, String key, long score) {
        ImmudbProto.ZAddOptions options = ImmudbProto.ZAddOptions.newBuilder()
                .setSet(ByteString.copyFrom(set, StandardCharsets.UTF_8))
                .setScore(score)
                .setKey(ByteString.copyFrom(key, StandardCharsets.UTF_8))
                .setIndex(ImmudbProto.Index.newBuilder()
                        .setIndex(root().getIndex())
                        .build())
                .build();
        //noinspection ResultOfMethodCallIgnored
        getStub().zAdd(options);
    }

    public void safeZAdd(String set, String key, long score) throws VerificationException {
        safeZAdd(set.getBytes(StandardCharsets.UTF_8), key.getBytes(StandardCharsets.UTF_8), score);
    }

    public void safeZAdd(byte[] set, byte[] key, long score) throws VerificationException {
        ImmudbProto.SafeZAddOptions options = ImmudbProto.SafeZAddOptions.newBuilder()
                .setRootIndex(ImmudbProto.Index.newBuilder()
                        .setIndex(root().getIndex())
                        .build())
                .setZopts(ImmudbProto.ZAddOptions.newBuilder()
                        .setSet(ByteString.copyFrom(set))
                        .setScore(score)
                        .setKey(ByteString.copyFrom(key))
                        .setIndex(ImmudbProto.Index.newBuilder()
                                .setIndex(root().getIndex())
                                .build())
                        .build())
                .build();
        ImmudbProto.Proof proof = getStub().safeZAdd(options);

        ImmudbProto.Item item =
                ImmudbProto.Item.newBuilder()
                        .setIndex(proof.getIndex())
                        .setKey(ByteString.copyFrom(buildKeySet(key, set, score)))
                        .setValue(ByteString.copyFrom(key))
                        .build();

        CryptoUtils.verify(proof, item, root());
    }

    private byte[] buildKeySet(byte[] key, byte[] set, long score) {
        ByteBuffer buffer = ByteBuffer.allocate(set.length + key.length + 8);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.put(set);
        buffer.putLong(score);
        buffer.put(key);
        return buffer.array();
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

    public static class ImmuClientBuilder {

        private String serverUrl;

        private int serverPort;

        private boolean withAuthToken;

        private RootHolder rootHolder;

        private ImmuClientBuilder() {
            this.serverUrl = "localhost";
            this.serverPort = 3322;
            this.rootHolder = new SerializableRootHolder();
            this.withAuthToken = true;
        }

        public ImmuClient build() {
            return new ImmuClient(this);
        }

        public String getServerUrl() {
            return this.serverUrl;
        }

        public ImmuClientBuilder setServerUrl(String serverUrl) {
            this.serverUrl = serverUrl;
            return this;
        }

        public int getServerPort() {
            return serverPort;
        }

        public ImmuClientBuilder setServerPort(int serverPort) {
            this.serverPort = serverPort;
            return this;
        }

        public boolean isWithAuthToken() {
            return withAuthToken;
        }

        public ImmuClientBuilder setWithAuthToken(boolean withAuthToken) {
            this.withAuthToken = withAuthToken;
            return this;
        }

        public RootHolder getRootHolder() {
            return rootHolder;
        }

        public ImmuClientBuilder setRootHolder(RootHolder rootHolder) {
            this.rootHolder = rootHolder;
            return this;
        }

    }
}
