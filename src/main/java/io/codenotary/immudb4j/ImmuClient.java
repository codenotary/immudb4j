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
import io.codenotary.immudb.ImmudbProto.NamedParam;
import io.codenotary.immudb.ImmudbProto.NewTxResponse;
import io.codenotary.immudb.ImmudbProto.ScanRequest;
import io.codenotary.immudb.ImmudbProto.Score;
import io.codenotary.immudb.ImmudbProto.TxMode;
import io.codenotary.immudb4j.basics.LatchHolder;
import io.codenotary.immudb4j.crypto.CryptoUtils;
import io.codenotary.immudb4j.crypto.DualProof;
import io.codenotary.immudb4j.crypto.InclusionProof;
import io.codenotary.immudb4j.exceptions.*;
import io.codenotary.immudb4j.sql.SQLException;
import io.codenotary.immudb4j.sql.SQLQueryResult;
import io.codenotary.immudb4j.sql.SQLValue;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    private ImmuClient(Builder builder) {
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

    /**
     * @return ImmuClient builder for chaining client settings
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Releases the resources used by the SDK objects. (e.g. connection resources).
     * This method should be called just before the existing process ends.
     */
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

    /**
     * Establishes a new database session using provided credentials
     * 
     * @param database The name of the database to which the session is established
     * @param username The username is required to get authorization
     * @param password The password is required to get authorization
     */
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
                    synchronized (ImmuClient.this) {
                        if (session != null) {
                            blockingStub.keepAlive(Empty.getDefaultInstance());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 0, keepAlivePeriod);
    }

    /**
     * Closes the open database session
     */
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
    // ========== SQL ==========
    //

    /**
     * Creates a new SQL transaction that can be terminated with the
     * {@link #commitTransaction() commit} or {@link #rollbackTransaction()
     * rollback} methods
     * 
     * @throws SQLException if the cause of the error is sql-related
     */
    public synchronized void beginTransaction() throws SQLException {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        if (session.getTransactionID() != null) {
            throw new IllegalStateException("transaction already initiated");
        }

        final ImmudbProto.NewTxRequest req = ImmudbProto.NewTxRequest.newBuilder()
                .setMode(TxMode.ReadWrite)
                .build();

        final NewTxResponse res = blockingStub.newTx(req);

        session.setTransactionID(res.getTransactionID());
    }

    /**
     * Commits the ongoing SQL transaction
     * 
     * @throws SQLException if the cause of the error is sql-related
     */
    public synchronized void commitTransaction() throws SQLException {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        if (session.getTransactionID() == null) {
            throw new IllegalStateException("no transaction initiated");
        }

        blockingStub.commit(Empty.getDefaultInstance());

        session.setTransactionID(null);
    }

    /**
     * Rollback the ongoing SQL transaction
     * 
     * @throws SQLException if the cause of the error is sql-related
     */
    public synchronized void rollbackTransaction() throws SQLException {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        if (session.getTransactionID() == null) {
            throw new IllegalStateException("no transaction initiated");
        }

        blockingStub.rollback(Empty.getDefaultInstance());

        session.setTransactionID(null);
    }

    /**
     * Executes a SQL statement in the ongoing SQL transaction
     * 
     * @param stmt   the SQL statement to be executed
     * @param params the positional parameters for SQL statement evaluation
     * 
     * @throws SQLException if the cause of the error is sql-related
     */
    public void sqlExec(String stmt, SQLValue... params) throws SQLException {
        sqlExec(stmt, sqlNameParams(params));
    }

    /**
     * Executes a SQL statement in the ongoing SQL transaction
     * 
     * @param stmt   the SQL statement to be executed
     * @param params the named parameters for SQL statement evaluation
     * 
     * @throws SQLException if the cause of the error is sql-related
     */
    public synchronized void sqlExec(String stmt, Map<String, SQLValue> params) throws SQLException {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        if (session.getTransactionID() == null) {
            throw new IllegalStateException("no transaction initiated");
        }

        final ImmudbProto.SQLExecRequest req = ImmudbProto.SQLExecRequest.newBuilder()
                .setSql(stmt)
                .addAllParams(sqlEncodeParams(params))
                .build();

        blockingStub.txSQLExec(req);
    }

    /**
     * Performs a SQL query in the ongoing SQL transaction
     * 
     * @param stmt   the SQL query to be evaluated
     * @param params the positional parameters for SQL statement evaluation
     * 
     * @return the query resultset
     * 
     * @throws SQLException if the cause of the error is sql-related
     */
    public SQLQueryResult sqlQuery(String stmt, SQLValue... params) throws SQLException {
        return sqlQuery(stmt, sqlNameParams(params));
    }

    /**
     * Performs a SQL query in the ongoing SQL transaction
     * 
     * @param stmt   the SQL query to be evaluated
     * @param params the named parameters for SQL statement evaluation
     * 
     * @return the query resultset
     * 
     * @throws SQLException if the cause of the error is sql-related
     */
    public synchronized SQLQueryResult sqlQuery(String stmt, Map<String, SQLValue> params) throws SQLException {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        if (session.getTransactionID() == null) {
            throw new IllegalStateException("no transaction initiated");
        }

        final ImmudbProto.SQLQueryRequest req = ImmudbProto.SQLQueryRequest.newBuilder()
                .setSql(stmt)
                .addAllParams(sqlEncodeParams(params))
                .build();

        return new SQLQueryResult(blockingStub.txSQLQuery(req));
    }

    private Map<String, SQLValue> sqlNameParams(SQLValue... params) {
        final Map<String, SQLValue> nparams = new HashMap<>(params.length);

        for (int i = 1; i <= params.length; i++) {
            nparams.put("param" + i, params[i - 1]);
        }

        return nparams;
    }

    private Iterable<NamedParam> sqlEncodeParams(Map<String, SQLValue> params) {
        List<ImmudbProto.NamedParam> nparams = new ArrayList<>();

        for (Map.Entry<String, SQLValue> p : params.entrySet()) {
            nparams.add(NamedParam.newBuilder()
                    .setName(p.getKey())
                    .setValue(p.getValue().asProtoSQLValue())
                    .build());
        }

        return nparams;
    }

    //
    // ========== DATABASE ==========
    //

    /**
     * Creates a database using default settings
     * 
     * @param database the database name
     */
    public void createDatabase(String database) {
        createDatabase(database, false);
    }

    /**
     * Creates a database using default settings
     * 
     * @param database    the database name
     * @param ifNotExists allow the database to be already created
     */
    public synchronized void createDatabase(String database, boolean ifNotExists) {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.CreateDatabaseRequest req = ImmudbProto.CreateDatabaseRequest.newBuilder()
                .setName(database)
                .setIfNotExists(ifNotExists)
                .build();

        blockingStub.createDatabaseV2(req);
    }

    /**
     * Loads database on the server. A database is not loaded
     * if it has AutoLoad setting set to false or if it failed to load during immudb
     * startup.
     * 
     * This call requires SysAdmin permission level or admin permission to the
     * database.
     * 
     * @param database the database name
     */
    public synchronized void loadDatabase(String database) {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.LoadDatabaseRequest req = ImmudbProto.LoadDatabaseRequest.newBuilder()
                .setDatabase(database)
                .build();

        blockingStub.loadDatabase(req);
    }

    /**
     * Unloads database on the server. Such database becomes inaccessible by the
     * client and server frees internal resources allocated for that database.
     * 
     * This call requires SysAdmin permission level or admin permission to the
     * database.
     * 
     * @param database the database name
     */
    public synchronized void unloadDatabase(String database) {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.UnloadDatabaseRequest req = ImmudbProto.UnloadDatabaseRequest.newBuilder()
                .setDatabase(database)
                .build();

        blockingStub.unloadDatabase(req);
    }

    /**
     * Removes an unloaded database. This also removes locally stored files used by
     * the database.
     * 
     * This call requires SysAdmin permission level or admin permission to the
     * database.
     * 
     * @param database the database name
     */
    public synchronized void deleteDatabase(String database) {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.DeleteDatabaseRequest req = ImmudbProto.DeleteDatabaseRequest.newBuilder()
                .setDatabase(database)
                .build();

        blockingStub.deleteDatabase(req);
    }

    /**
     * @return the list of existing databases
     */
    public synchronized List<Database> databases() {
        if (session == null) {
            throw new IllegalStateException("no open session");
        }

        final ImmudbProto.DatabaseListRequestV2 req = ImmudbProto.DatabaseListRequestV2.newBuilder().build();
        final ImmudbProto.DatabaseListResponseV2 resp = blockingStub.databaseListV2(req);

        final List<Database> list = new ArrayList<>(resp.getDatabasesCount());

        for (ImmudbProto.DatabaseWithSettings db : resp.getDatabasesList()) {
            list.add(Database.valueOf(db));
        }

        return list;
    }

    //
    // ========== GET ==========
    //

    /**
     * @param key the key to look for
     * @return the latest entry associated to the provided key
     */
    public Entry get(String key) throws KeyNotFoundException {
        return get(Utils.toByteArray(key));
    }

    /**
     * @param key the key to look for
     * @return the latest entry associated to the provided key
     */
    public Entry get(byte[] key) throws KeyNotFoundException {
        return getAtTx(key, 0);
    }

    /**
     * @param key the key to look for
     * @param tx  the transaction at which the associated entry is expected to be
     * @return the entry associated to the provided key and transaction
     */
    public Entry getAtTx(String key, long tx) throws KeyNotFoundException {
        return getAtTx(Utils.toByteArray(key), tx);
    }

    /**
     * @param key the key to look for
     * @param tx  the transaction at which the associated entry is expected to be
     * @return the entry associated to the provided key and transaction
     */
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

    /**
     * This method can be used to avoid additional latency while the server
     * waits for the indexer to finish indexing but still guarantees that
     * specific portion of the database history has already been indexed.
     * 
     * @param key the key to look for
     * @param tx  the lowest transaction from which the associated entry should be
     *            retrieved
     * @return the latest indexed entry associated the to provided key. Ensuring the
     *         indexing has already be completed up to the specified transaction.
     */
    public Entry getSinceTx(String key, long tx) throws KeyNotFoundException {
        return getSinceTx(Utils.toByteArray(key), tx);
    }

    /**
     * This method can be used to avoid additional latency while the server
     * waits for the indexer to finish indexing but still guarantees that
     * specific portion of the database history has already been indexed.
     * 
     * @param key the key to look for
     * @param tx  the lowest transaction from which the associated entry should be
     *            retrieved
     * @return the latest indexed entry associated the to provided key. Ensuring the
     *         indexing has already be completed up to the specified transaction.
     */
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

    /**
     * @param key the key to look for
     * @param rev the specific revision
     * @return the specific revision for given key.
     * 
     *         Key revision is an integer value that starts at 1 when
     *         the key is created and then increased by 1 on every update made to
     *         that key.
     * 
     *         The way rev is interpreted depends on the value:
     *         - if rev == 0, returns current value
     *         - if rev &gt; 0, returns nth revision value, e.g. 1 is the first
     *         value,
     *         2 is the second and so on
     *         - if rev &lt; 0, returns nth revision value from the end, e.g. -1 is
     *         the
     *         previous value, -2 is the one before and so on
     */
    public Entry getAtRevision(String key, long rev) throws KeyNotFoundException {
        return getAtRevision(Utils.toByteArray(key), rev);
    }

    /**
     * @param key the key to look for
     * @param rev the specific revision
     * @return the specific revision for given key.
     * 
     *         Key revision is an integer value that starts at 1 when
     *         the key is created and then increased by 1 on every update made to
     *         that key.
     * 
     *         The way rev is interpreted depends on the value:
     *         - if rev == 0, returns current value
     *         - if rev &gt; 0, returns nth revision value, e.g. 1 is the first
     *         value,
     *         2 is the second and so on
     *         - if rev &lt; 0, returns nth revision value from the end, e.g. -1 is
     *         the
     *         previous value, -2 is the one before and so on
     */
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

    /**
     * @param keys the lists of keys to look for
     * @return retrieves multiple entries in a single call
     */
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

    /**
     * @param key the keys to look for
     * @return the latest entry associated to the provided key. Equivalent to
     *         {@link #get(String) get} but with additional
     *         server-provided proof validation.
     */
    public Entry verifiedGet(String key) throws KeyNotFoundException, VerificationException {
        return verifiedGetAtTx(key, 0);
    }

    /**
     * @param key the keys to look for
     * @return the latest entry associated to the provided key. Equivalent to
     *         {@link #get(byte[]) get} but with additional
     *         server-provided proof validation.
     */
    public Entry verifiedGet(byte[] key) throws KeyNotFoundException, VerificationException {
        return verifiedGetAtTx(key, 0);
    }

    /**
     * @param key the key to look for
     * @param tx  the transaction at which the associated entry is expected to be
     * @return the entry associated to the provided key and transaction. Equivalent
     *         to
     *         {@link #getAtTx(String, long) getAtTx} but with additional
     *         server-provided proof validation.
     */
    public Entry verifiedGetAtTx(String key, long tx) throws KeyNotFoundException, VerificationException {
        return verifiedGetAtTx(Utils.toByteArray(key), tx);
    }

    /**
     * @param key the key to look for
     * @param tx  the transaction at which the associated entry is expected to be
     * @return the entry associated to the provided key and transaction. Equivalent
     *         to
     *         {@link #getAtTx(byte[], long) getAtTx} but with additional
     *         server-provided proof validation.
     */
    public synchronized Entry verifiedGetAtTx(byte[] key, long tx) throws KeyNotFoundException, VerificationException {
        final ImmuState state = state();

        final ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
                .setKey(Utils.toByteString(key))
                .setAtTx(tx)
                .build();

        return verifiedGet(keyReq, state);
    }

    /**
     * This method can be used to avoid additional latency while the server
     * waits for the indexer to finish indexing but still guarantees that
     * specific portion of the database history has already been indexed.
     * 
     * @param key the key to look for
     * @param tx  the lowest transaction from which the associated entry should be
     *            retrieved
     * @return the latest indexed entry associated the to provided key. Ensuring the
     *         indexing has already be completed up to the specified transaction.
     *         Equivalent to
     *         {@link #getSinceTx(String, long) getSinceTx} but with additional
     *         server-provided proof validation.
     */
    public Entry verifiedGetSinceTx(String key, long tx) throws KeyNotFoundException, VerificationException {
        return verifiedGetSinceTx(Utils.toByteArray(key), tx);
    }

    /**
     * This method can be used to avoid additional latency while the server
     * waits for the indexer to finish indexing but still guarantees that
     * specific portion of the database history has already been indexed.
     * 
     * @param key the key to look for
     * @param tx  the lowest transaction from which the associated entry should be
     *            retrieved
     * @return the latest indexed entry associated the to provided key. Ensuring the
     *         indexing has already be completed up to the specified transaction.
     *         Equivalent to
     *         {@link #getSinceTx(byte[], long) getSinceTx} but with additional
     *         server-provided proof validation.
     */
    public synchronized Entry verifiedGetSinceTx(byte[] key, long tx)
            throws KeyNotFoundException, VerificationException {

        final ImmuState state = state();

        final ImmudbProto.KeyRequest keyReq = ImmudbProto.KeyRequest.newBuilder()
                .setKey(Utils.toByteString(key))
                .setSinceTx(tx)
                .build();

        return verifiedGet(keyReq, state);
    }

    /**
     * @param key the key to look for
     * @param rev the specific revision
     * @return the specific revision for given key. Equivalent to
     *         {@link #getAtRevision(String, long) getAtRevision} but with
     *         additional
     *         server-provided proof validation.
     */
    public Entry verifiedGetAtRevision(String key, long rev) throws KeyNotFoundException, VerificationException {
        return verifiedGetAtRevision(Utils.toByteArray(key), rev);
    }

    /**
     * @param key the key to look for
     * @param rev the specific revision
     * @return the specific revision for given key. Equivalent to
     *         {@link #getAtRevision(byte[], long) getAtRevision} but with
     *         additional
     *         server-provided proof validation.
     */
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

    /**
     * Performs a logical deletion for key
     * 
     * @param key the key to delete
     */
    public TxHeader delete(String key) throws KeyNotFoundException {
        return delete(Utils.toByteArray(key));
    }

    /**
     * Performs a logical deletion for key
     * 
     * @param key the key to delete
     */
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

    /**
     * @param key    the key to look for
     * @param offset the number of entries to be skipped
     * @param desc   the order in which entries are returned
     * @param limit  the maximum number of entries to be returned
     * @return the list of entries associated to the provided key
     * @throws KeyNotFoundException if the key is not found
     */
    public List<Entry> historyAll(String key, long offset, boolean desc, int limit) throws KeyNotFoundException {
        return historyAll(Utils.toByteArray(key), offset, desc, limit);
    }

    /**
     * @param key    the key to look for
     * @param offset the number of entries to be skipped
     * @param desc   the order in which entries are returned
     * @param limit  the maximum number of entries to be returned
     * @return the list of entries associated to the provided key
     * @throws KeyNotFoundException if the key is not found
     */
    public synchronized List<Entry> historyAll(byte[] key, long offset, boolean desc, int limit)
            throws KeyNotFoundException {
        try {
            ImmudbProto.Entries entries = blockingStub.history(ImmudbProto.HistoryRequest.newBuilder()
                    .setKey(Utils.toByteString(key))
                    .setDesc(desc)
                    .setOffset(offset)
                    .setLimit(limit)
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

    /**
     * @param prefix the prefix used to filter entries
     * @return the list of entries with a maching prefix
     */
    public List<Entry> scanAll(String prefix) {
        return scanAll(Utils.toByteArray(prefix));
    }

    /**
     * @param prefix the prefix used to filter entries
     * @return the list of entries with a maching prefix
     */
    public List<Entry> scanAll(byte[] prefix) {
        return scanAll(prefix, false, 0);
    }

    /**
     * @param prefix the prefix used to filter entries
     * @param desc   the order in which entries are returned
     * @param limit  the maximum number of entries to be returned
     * @return the list of entries with a maching prefix
     */
    public List<Entry> scanAll(String prefix, boolean desc, long limit) {
        return scanAll(Utils.toByteArray(prefix), null, desc, limit);
    }

    /**
     * @param prefix the prefix used to filter entries
     * @param desc   the order in which entries are returned
     * @param limit  the maximum number of entries to be returned
     * @return the list of entries with a maching prefix
     */
    public List<Entry> scanAll(byte[] prefix, boolean desc, long limit) {
        return scanAll(prefix, null, desc, limit);
    }

    /**
     * @param prefix  the prefix used to filter entries
     * @param seekKey the initial key from which the scan begins
     * @param desc    the order in which entries are returned
     * @param limit   the maximum number of entries to be returned
     * @return the list of entries with a maching prefix
     */
    public List<Entry> scanAll(byte[] prefix, byte[] seekKey, boolean desc, long limit) {
        return scanAll(prefix, seekKey, null, desc, limit);
    }

    /**
     * @param prefix  the prefix used to filter entries
     * @param seekKey the initial key from which the scan begins
     * @param endKey  the final key at which the scan ends
     * @param desc    the order in which entries are returned
     * @param limit   the maximum number of entries to be returned
     * @return the list of entries with a maching prefix
     */
    public List<Entry> scanAll(byte[] prefix, byte[] seekKey, byte[] endKey, boolean desc, long limit) {
        return scanAll(prefix, seekKey, endKey, true, true, desc, limit);
    }

    /**
     * @param prefix        the prefix used to filter entries
     * @param seekKey       the initial key from which the scan begins
     * @param endKey        the final key at which the scan ends
     * @param inclusiveSeek used to include/exclude the seekKey from the result
     * @param inclusiveEnd  used to include/exclude the endKey from the result
     * @param desc          the order in which entries are returned
     * @param limit         the maximum number of entries to be returned
     * @return the list of entries with a maching prefix
     */
    public synchronized List<Entry> scanAll(byte[] prefix, byte[] seekKey, byte[] endKey, boolean inclusiveSeek,
            boolean inclusiveEnd,
            boolean desc, long limit) {
        final ImmudbProto.ScanRequest req = ScanRequest.newBuilder()
                .setPrefix(Utils.toByteString(prefix))
                .setSeekKey(Utils.toByteString(seekKey))
                .setEndKey(Utils.toByteString(endKey))
                .setInclusiveSeek(inclusiveSeek)
                .setInclusiveEnd(inclusiveEnd)
                .setDesc(desc)
                .setLimit(limit)
                .build();

        final ImmudbProto.Entries entries = blockingStub.scan(req);
        return buildList(entries);
    }

    //
    // ========== SET ==========
    //

    /**
     * Commits a change of a value for a single key.
     * 
     * @param key   the key to set
     * @param value the value to set
     */
    public TxHeader set(String key, byte[] value) {
        return set(Utils.toByteArray(key), value);
    }

    /**
     * Commits a change of a value for a single key.
     * 
     * @param key   the key to set
     * @param value the value to set
     */
    public synchronized TxHeader set(byte[] key, byte[] value) {
        final ImmudbProto.KeyValue kv = ImmudbProto.KeyValue.newBuilder()
                .setKey(Utils.toByteString(key))
                .setValue(Utils.toByteString(value))
                .build();

        final ImmudbProto.SetRequest req = ImmudbProto.SetRequest.newBuilder().addKVs(kv).build();

        return TxHeader.valueOf(blockingStub.set(req));
    }

    /**
     * Commits multiple entries in a single transaction.
     * 
     * @param kvList the list of key-value pairs to set
     */
    public synchronized TxHeader setAll(List<KVPair> kvList) {
        final ImmudbProto.SetRequest.Builder reqBuilder = ImmudbProto.SetRequest.newBuilder();

        for (KVPair kv : kvList) {
            ImmudbProto.KeyValue.Builder kvBuilder = ImmudbProto.KeyValue.newBuilder();

            kvBuilder.setKey(Utils.toByteString(kv.getKey()));
            kvBuilder.setValue(Utils.toByteString(kv.getValue()));

            reqBuilder.addKVs(kvBuilder.build());
        }

        return TxHeader.valueOf(blockingStub.set(reqBuilder.build()));
    }

    /**
     * 
     * @param key
     * @param referencedKey
     * @return
     */
    public TxHeader setReference(String key, String referencedKey) {
        return setReference(Utils.toByteArray(key), Utils.toByteArray(referencedKey));
    }

    public TxHeader setReference(byte[] key, byte[] referencedKey) {
        return setReference(key, referencedKey, 0);
    }

    public TxHeader setReference(String key, String referencedKey, long atTx) {
        return setReference(Utils.toByteArray(key), Utils.toByteArray(referencedKey), atTx);
    }

    public synchronized TxHeader setReference(byte[] key, byte[] referencedKey, long atTx) {
        final ImmudbProto.ReferenceRequest req = ImmudbProto.ReferenceRequest.newBuilder()
                .setKey(Utils.toByteString(key))
                .setReferencedKey(Utils.toByteString(referencedKey))
                .setAtTx(atTx)
                .setBoundRef(atTx > 0)
                .build();

        return TxHeader.valueOf(blockingStub.setReference(req));
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

    public TxHeader zAdd(String set, String key, double score) {
        return zAdd(Utils.toByteArray(set), Utils.toByteArray(key), score);
    }

    public TxHeader zAdd(byte[] set, byte[] key, double score) {
        return zAdd(set, key, 0, score);
    }

    public synchronized TxHeader zAdd(byte[] set, byte[] key, long atTx, double score) {
        ImmudbProto.ZAddRequest req = ImmudbProto.ZAddRequest.newBuilder()
                .setSet(Utils.toByteString(set))
                .setKey(Utils.toByteString(key))
                .setAtTx(atTx)
                .setScore(score)
                .setBoundRef(atTx > 0)
                .build();

        return TxHeader.valueOf(blockingStub.zAdd(req));
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

    public List<ZEntry> zScanAll(String set) {
        return zScanAll(set, false, 0);
    }

    public List<ZEntry> zScanAll(String set, boolean reverse, long limit) {
        return pzScanAll(Utils.toByteArray(set), null, null, null, null, 0, true, reverse, limit);
    }

    public List<ZEntry> zScanAll(byte[] set, double minScore, double maxScore, boolean reverse, long limit) {
        return pzScanAll(set, minScore, maxScore, null, null, 0, true, false, 0);
    }

    public List<ZEntry> zScanAll(byte[] set, double minScore, double maxScore, double seekScore, byte[] seekKey,
            long seekAtTx, boolean inclusiveSeek, boolean reverse, long limit) {
        return pzScanAll(set, minScore, maxScore, seekScore, seekKey, seekAtTx, inclusiveSeek, reverse, limit);
    }

    private synchronized List<ZEntry> pzScanAll(byte[] set, Double minScore, Double maxScore, Double seekScore,
            byte[] seekKey,
            long seekAtTx, boolean inclusiveSeek, boolean reverse, long limit) {

        final ImmudbProto.ZScanRequest.Builder reqBuilder = ImmudbProto.ZScanRequest.newBuilder();

        reqBuilder.setSet(Utils.toByteString(set))
                .setSeekKey(Utils.toByteString(seekKey))
                .setSeekAtTx(seekAtTx)
                .setInclusiveSeek(inclusiveSeek)
                .setDesc(reverse)
                .setLimit(limit);

        if (seekScore != null) {
            reqBuilder.setSeekScore(seekScore);
        }

        if (minScore != null) {
            reqBuilder.setMinScore(Score.newBuilder().setScore(minScore).build());
        }

        if (maxScore != null) {
            reqBuilder.setMaxScore(Score.newBuilder().setScore(maxScore).build());
        }

        final ImmudbProto.ZEntries zEntries = blockingStub.zScan(reqBuilder.build());

        return buildList(zEntries);
    }

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

    public synchronized List<Tx> txScanAll(long initialTxId) {
        final ImmudbProto.TxScanRequest req = ImmudbProto.TxScanRequest.newBuilder().setInitialTx(initialTxId).build();
        final ImmudbProto.TxList txList = blockingStub.txScan(req);

        return buildList(txList);
    }

    public synchronized List<Tx> txScanAll(long initialTxId, boolean desc, int limit) {
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
        final ByteBuffer buf = ByteBuffer.allocate(chunkSize).order(ByteOrder.BIG_ENDIAN);

        buf.putLong(bs.length);

        int i = 0;

        while (i < bs.length) {
            final int chunkContentLen = Math.min(bs.length, buf.remaining());

            buf.put(bs, i, chunkContentLen);

            buf.flip();

            byte[] chunkContent = new byte[buf.limit()];
            buf.get(chunkContent);

            final Chunk chunk = Chunk.newBuilder().setContent(Utils.toByteString(chunkContent)).build();

            streamObserver.onNext(chunk);

            buf.clear();

            i += chunkContentLen;
        }
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
        buf.put(firstChunkContent, Long.BYTES, firstChunkContent.length - Long.BYTES);

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
                try {
                    return chunks.hasNext();
                } catch (StatusRuntimeException e) {
                    if (e.getMessage().contains("key not found")) {
                        return false;
                    }

                    throw e;
                }
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
                try {
                    return chunks.hasNext();
                } catch (StatusRuntimeException e) {
                    if (e.getMessage().contains("key not found")) {
                        return false;
                    }

                    throw e;
                }
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

    public synchronized TxHeader streamSet(byte[] key, byte[] value)
            throws InterruptedException {
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

    public Iterator<Entry> scan(String prefix) {
        return scan(Utils.toByteArray(prefix));
    }

    public Iterator<Entry> scan(byte[] prefix) {
        return scan(prefix, false, 0);
    }

    public Iterator<Entry> scan(String prefix, boolean desc, long limit) {
        return scan(Utils.toByteArray(prefix), desc, limit);
    }

    public Iterator<Entry> scan(byte[] prefix, boolean desc, long limit) {
        return scan(prefix, null, desc, limit);
    }

    public Iterator<Entry> scan(byte[] prefix, byte[] seekKey, boolean desc, long limit) {
        return scan(prefix, seekKey, null, desc, limit);
    }

    public Iterator<Entry> scan(byte[] prefix, byte[] seekKey, byte[] endKey, boolean desc, long limit) {
        return scan(prefix, seekKey, endKey, true, true, desc, limit);
    }

    public synchronized Iterator<Entry> scan(byte[] prefix, byte[] seekKey, byte[] endKey, boolean inclusiveSeek,
            boolean inclusiveEnd,
            boolean desc, long limit) {

        final ImmudbProto.ScanRequest req = ScanRequest.newBuilder()
                .setPrefix(Utils.toByteString(prefix))
                .setSeekKey(Utils.toByteString(seekKey))
                .setEndKey(Utils.toByteString(endKey))
                .setInclusiveSeek(inclusiveSeek)
                .setInclusiveEnd(inclusiveEnd)
                .setDesc(desc)
                .setLimit(limit)
                .build();

        final Iterator<Chunk> chunks = blockingStub.streamScan(req);

        return entryIterator(chunks);
    }

    //
    // ========== STREAM ZSCAN ==========
    //

    public Iterator<ZEntry> zScan(String set) {
        return zScan(set, false, 0);
    }

    public Iterator<ZEntry> zScan(String set, boolean reverse, long limit) {
        return pzScan(Utils.toByteArray(set), null, null, null, null, 0, true, reverse, limit);
    }

    public Iterator<ZEntry> zScan(byte[] set, double minScore, double maxScore, boolean reverse, long limit) {
        return pzScan(set, minScore, maxScore, null, null, 0, true, false, 0);
    }

    public Iterator<ZEntry> zScan(byte[] set, double minScore, double maxScore, double seekScore, byte[] seekKey,
            long seekAtTx, boolean inclusiveSeek, boolean reverse, long limit) {
        return pzScan(set, minScore, maxScore, seekScore, seekKey, seekAtTx, inclusiveSeek, reverse, limit);
    }

    private synchronized Iterator<ZEntry> pzScan(byte[] set, Double minScore, Double maxScore, Double seekScore,
            byte[] seekKey,
            long seekAtTx, boolean inclusiveSeek, boolean reverse, long limit) {

        final ImmudbProto.ZScanRequest.Builder reqBuilder = ImmudbProto.ZScanRequest.newBuilder();

        reqBuilder.setSet(Utils.toByteString(set))
                .setSeekKey(Utils.toByteString(seekKey))
                .setSeekAtTx(seekAtTx)
                .setInclusiveSeek(inclusiveSeek)
                .setDesc(reverse)
                .setLimit(limit);

        if (seekScore != null) {
            reqBuilder.setSeekScore(seekScore);
        }

        if (minScore != null) {
            reqBuilder.setMinScore(Score.newBuilder().setScore(minScore).build());
        }

        if (maxScore != null) {
            reqBuilder.setMaxScore(Score.newBuilder().setScore(maxScore).build());
        }

        final Iterator<Chunk> chunks = blockingStub.streamZScan(reqBuilder.build());

        return zentryIterator(chunks);
    }

    //
    // ========== STREAM HISTORY ==========
    //

    public Iterator<Entry> history(String key, long offset, boolean desc, int limit) throws KeyNotFoundException {
        return history(Utils.toByteArray(key), offset, desc, limit);
    }

    public synchronized Iterator<Entry> history(byte[] key, long offset, boolean desc, int limit)
            throws KeyNotFoundException {
        try {
            ImmudbProto.HistoryRequest req = ImmudbProto.HistoryRequest.newBuilder()
                    .setKey(Utils.toByteString(key))
                    .setDesc(desc)
                    .setOffset(offset)
                    .setLimit(limit)
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

    public synchronized void grantPermission(String user, String database, Permission permissions) {
        final ImmudbProto.ChangePermissionRequest req = ImmudbProto.ChangePermissionRequest.newBuilder()
                .setUsername(user)
                .setAction(ImmudbProto.PermissionAction.GRANT)
                .setDatabase(database)
                .setPermission(permissions.permissionCode)
                .build();

        // noinspection ResultOfMethodCallIgnored
        blockingStub.changePermission(req);
    }

    public synchronized void revokePermission(String user, String database, Permission permissions) {
        final ImmudbProto.ChangePermissionRequest req = ImmudbProto.ChangePermissionRequest.newBuilder()
                .setUsername(user)
                .setAction(ImmudbProto.PermissionAction.REVOKE)
                .setDatabase(database)
                .setPermission(permissions.permissionCode)
                .build();

        // noinspection ResultOfMethodCallIgnored
        blockingStub.changePermission(req);
    }

    //
    // ========== INDEX MGMT ==========
    //

    public synchronized void flushIndex(float cleanupPercentage) {
        ImmudbProto.FlushIndexRequest req = ImmudbProto.FlushIndexRequest.newBuilder()
                .setCleanupPercentage(cleanupPercentage)
                .setSynced(true)
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
            if (chunkSize < Long.BYTES) {
                throw new RuntimeException("invalid chunk size");
            }

            this.chunkSize = chunkSize;
            return this;
        }

        public int getChunkSize() {
            return chunkSize;
        }
    }

}
