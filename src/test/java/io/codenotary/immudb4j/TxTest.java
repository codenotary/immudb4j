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

import io.codenotary.immudb4j.exceptions.TxNotFoundException;
import io.codenotary.immudb4j.exceptions.VerificationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class TxTest extends ImmuClientIntegrationTest {

    @Test(testName = "verifiedSet, txById, verifiedTxById")
    public void t1() throws NoSuchAlgorithmException, VerificationException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        String key = "test-txid";
        byte[] val = "test-txid-value".getBytes(StandardCharsets.UTF_8);

        TxHeader txHdr = null;
        try {
            txHdr = immuClient.verifiedSet(key, val);
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedSet", e);
        }

        Tx tx = immuClient.txById(txHdr.getId());

        Assert.assertEquals(txHdr.getId(), tx.getHeader().getId());

        try {
            tx = immuClient.verifiedTxById(txHdr.getId());
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedTxById", e);
        }

        Assert.assertEquals(txHdr.getId(), tx.getHeader().getId());

        try {
            immuClient.txById(txHdr.getId() + 1);
            Assert.fail("Failed at txById.");
        } catch (TxNotFoundException _) {
        }

        try {
            immuClient.verifiedTxById(txHdr.getId() + 1);
            Assert.fail("Failed at verifiedTxById.");
        } catch (TxNotFoundException _) {
        }

        immuClient.closeSession();
    }

    @Test(testName = "set, txScan")
    public void t2() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        String key = "txtest-t2";
        byte[] val1 = "immuRocks!".getBytes(StandardCharsets.UTF_8);
        byte[] val2 = "immuRocks! Again!".getBytes(StandardCharsets.UTF_8);

        long initialTxId = 1;

        TxHeader txHdr = immuClient.set(key, val1);
        Assert.assertNotNull(txHdr);
        initialTxId = txHdr.getId();
        txHdr = immuClient.set(key, val2);
        Assert.assertNotNull(txHdr);

        List<Tx> txs = immuClient.txScanAll(initialTxId, false, 1);
        Assert.assertNotNull(txs);
        Assert.assertEquals(txs.size(), 1);

        txs = immuClient.txScanAll(initialTxId, false, 2);
        Assert.assertNotNull(txs);
        Assert.assertEquals(txs.size(), 2);

        Assert.assertNotNull(immuClient.txScanAll(initialTxId));

        immuClient.closeSession();
    }

}
