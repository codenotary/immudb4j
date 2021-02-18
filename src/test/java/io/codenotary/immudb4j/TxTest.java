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

import io.codenotary.immudb4j.exceptions.CorruptedDataException;
import io.codenotary.immudb4j.exceptions.MaxWidthExceededException;
import io.codenotary.immudb4j.exceptions.VerificationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class TxTest extends ImmuClientIntegrationTest {

    @Test(testName = "verifiedSet, txById, verifiedTxById")
    public void t1() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        String key = "test-txid";
        byte[] val = "test-txid-value".getBytes(StandardCharsets.UTF_8);

        TxMetadata txMd = null;
        try {
            txMd = immuClient.verifiedSet(key, val);
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedSet", e);
        }

        Tx tx = null;
        try {
            tx = immuClient.txById(txMd.id);
        } catch (MaxWidthExceededException | NoSuchAlgorithmException e) {
            Assert.fail("Failed at txById", e);
        }

        Assert.assertEquals(txMd.id, tx.metadata().id);

        try {
            tx = immuClient.verifiedTxById(txMd.id);
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedTxById", e);
        }

        Assert.assertEquals(txMd.id, tx.metadata().id);

        immuClient.logout();
    }

    @Test(testName = "set, txScan")
    public void t2() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        String key = "txtest-t2";
        byte[] val1 = "immuRocks!".getBytes(StandardCharsets.UTF_8);
        byte[] val2 = "immuRocks! Again!".getBytes(StandardCharsets.UTF_8);

        long initialTxId = 1;
        try {
            TxMetadata txMd = immuClient.set(key, val1);
            Assert.assertNotNull(txMd);
            initialTxId = txMd.id;
            txMd = immuClient.set(key, val2);
            Assert.assertNotNull(txMd);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at set.", e);
        }

        List<Tx> txs = immuClient.txScan(initialTxId, 1, false);
        Assert.assertNotNull(txs);
        Assert.assertEquals(txs.size(), 1);

        txs = immuClient.txScan(initialTxId, 2, false);
        Assert.assertNotNull(txs);
        Assert.assertEquals(txs.size(), 2);

        immuClient.logout();
    }

}
