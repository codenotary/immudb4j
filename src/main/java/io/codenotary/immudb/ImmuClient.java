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
package io.codenotary.immudb;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.codenotary.immudb.crypto.CryptoUtils;
import io.codenotary.immudb.crypto.Root;
import io.codenotary.immudb.crypto.VerificationException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * immudb client using grpc.
 *
 * @author Jeronimo Irazabal
 */
public class ImmuClient {

  private static final String AUTH_HEADER = "authorization";

  private ImmuServiceGrpc.ImmuServiceBlockingStub stub;

  private boolean withAuthToken;
  private String authToken;

  private RootHolder rootHolder;

  private String activeDatabase = "defaultdb";

  public ImmuClient(ImmuClientBuilder builder) throws NoSuchAlgorithmException {
    this.stub = createStubFrom(builder);
    this.withAuthToken = builder.isWithAuthToken();
    this.rootHolder = builder.getRootHolder();
  }

  public static ImmuClientBuilder newBuilder() {
    return new ImmuClientBuilder();
  }

  private ImmuServiceGrpc.ImmuServiceBlockingStub createStubFrom(ImmuClientBuilder builder) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(builder.getServerUrl(), builder.getServerPort())
            .usePlaintext()
            .build();
    return ImmuServiceGrpc.newBlockingStub(channel);
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

  public static class ImmuClientBuilder {

    private String serverUrl;

    private int serverPort;

    private boolean withAuthToken;

    private RootHolder rootHolder;

    private ImmuClientBuilder() {
      this.serverUrl = "localhost";
      this.serverPort = 3322;
      this.rootHolder = new TransientRootHolder();
      this.withAuthToken = true;
    }

    public ImmuClient build() {
      try {
        return new ImmuClient(this);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }

    public String getServerUrl() {
      return this.serverUrl;
    }

    public int getServerPort() {
      return serverPort;
    }

    public boolean isWithAuthToken() {
      return withAuthToken;
    }

    public RootHolder getRootHolder() {
      return rootHolder;
    }

    public ImmuClientBuilder setServerUrl(String serverUrl) {
      this.serverUrl = serverUrl;
      return this;
    }

    public ImmuClientBuilder setServerPort(int serverPort) {
      this.serverPort = serverPort;
      return this;
    }

    public ImmuClientBuilder setWithAuthToken(boolean withAuthToken) {
      this.withAuthToken = withAuthToken;
      return this;
    }

    public ImmuClientBuilder setRootHolder(RootHolder rootHolder) {
      this.rootHolder = rootHolder;
      return this;
    }

  }

  public void login(String username, String password) {
    ImmudbProto.LoginRequest loginRequest =
        ImmudbProto.LoginRequest.newBuilder()
            .setUser(ByteString.copyFrom(username, Charsets.UTF_8))
            .setPassword(ByteString.copyFrom(password, Charsets.UTF_8))
            .build();

    ImmudbProto.LoginResponse loginResponse = getStub().login(loginRequest);
    authToken = loginResponse.getToken();
  }

  public void logout() {
    getStub().logout(com.google.protobuf.Empty.getDefaultInstance());
    authToken = null;
  }

  public Root root() {
    if (rootHolder.getRoot(activeDatabase) == null) {
      Empty empty = com.google.protobuf.Empty.getDefaultInstance();
      ImmudbProto.Root r = getStub().currentRoot(empty);
      Root root = new Root(activeDatabase, r.getIndex(), r.getRoot().toByteArray());
      rootHolder.SetRoot(root);
    }
    return rootHolder.getRoot(activeDatabase);
  }

  public void createDatabase(String database) {
    ImmudbProto.Database db = ImmudbProto.Database.newBuilder()
            .setDatabasename(database).build();
    getStub().createDatabase(db);
  }

  public void useDatabase(String database) {
    ImmudbProto.Database db = ImmudbProto.Database.newBuilder()
            .setDatabasename(database).build();
    getStub().useDatabase(db);
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
    set(key.getBytes(Charsets.UTF_8), value);
  }

  public void set(byte[] key, byte[] value) {
    ImmudbProto.KeyValue kv =
        ImmudbProto.KeyValue.newBuilder()
            .setKey(ByteString.copyFrom(key))
            .setValue(ByteString.copyFrom(value))
            .build();

    getStub().set(kv);
  }

  public byte[] get(String key) {
    return get(key.getBytes(Charsets.UTF_8));
  }

  public byte[] get(byte[] key) {
    ImmudbProto.Key k = ImmudbProto.Key.newBuilder().setKey(ByteString.copyFrom(key)).build();

    ImmudbProto.Item item = getStub().get(k);
    return item.getValue().toByteArray();
  }

  public byte[] safeGet(String key) throws VerificationException {
    return safeGet(key.getBytes(Charsets.UTF_8));
  }

  public byte[] safeGet(byte[] key) throws VerificationException {
    return safeGet(key, this.root());
  }

  public byte[] safeGet(byte[] key, Root root) throws VerificationException {
    ImmudbProto.Index index = ImmudbProto.Index.newBuilder().setIndex(root.getIndex()).build();

    ImmudbProto.SafeGetOptions sOpts =
        ImmudbProto.SafeGetOptions.newBuilder()
            .setKey(ByteString.copyFrom(key))
            .setRootIndex(index)
            .build();

    ImmudbProto.SafeItem safeItem = getStub().safeGet(sOpts);

    ImmudbProto.Proof proof = safeItem.getProof();

    CryptoUtils.verify(proof, safeItem.getItem(), root);

    rootHolder.SetRoot(new Root(activeDatabase, proof.getAt(), proof.getRoot().toByteArray()));

    return safeItem.getItem().getValue().toByteArray();
  }

  public void safeSet(String key, byte[] value) throws VerificationException {
    safeSet(key.getBytes(Charsets.UTF_8), value);
  }

  public void safeSet(byte[] key, byte[] value) throws VerificationException {
    safeSet(key, value, this.root());
  }

  public void safeSet(byte[] key, byte[] value, Root root) throws VerificationException {
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

    CryptoUtils.verify(proof, item, rootHolder.getRoot(activeDatabase));

    rootHolder.SetRoot(new Root(activeDatabase, proof.getAt(), proof.getRoot().toByteArray()));
  }
}
