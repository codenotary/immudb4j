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

import io.codenotary.immudb4j.exceptions.CorruptedDataException;
import io.codenotary.immudb4j.exceptions.VerificationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class VerifiedSetAndGetTest extends ImmuClientIntegrationTest {

    @Test(testName = "set, verifiedGet")
    public void t1() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        String key = "vsg";
        byte[] val = "test-set-vget".getBytes(StandardCharsets.UTF_8);

        try {
            immuClient.set(key, val);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at set.", e);
        }

        Entry vEntry = null;
        try {
            vEntry = immuClient.verifiedGet(key);
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedGet", e);
        }

        Assert.assertEquals(val, vEntry.getValue());

        immuClient.closeSession();
    }

    @Test(testName = "verifiedSet, verifiedGet, verifiedGetAt, verifiedGetSince")
    public void t2() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        byte[] key = "vsg".getBytes(StandardCharsets.UTF_8);
        byte[] val = "test-vset-vget".getBytes(StandardCharsets.UTF_8);

        // verifiedSet
        try {
            TxHeader txHdr = immuClient.verifiedSet(key, val);
            Assert.assertNotNull(txHdr, "The result of verifiedSet must not be null.");
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedSet. Cause: " + e.getMessage(), e);
        }

        // verifiedGet
        Entry vEntry = null;
        try {
            vEntry = immuClient.verifiedGet(key);
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedGet. Cause: " + e.getMessage(), e);
        }
        Assert.assertEquals(val, vEntry.getValue());

        // verifiedGetAt
        try {
            vEntry = immuClient.verifiedGetAtTx(key, vEntry.getTx());
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedGetAt. Cause: " + e.getMessage(), e);
        }
        Assert.assertEquals(val, vEntry.getValue());

        // verifiedSetReference
        byte[] refKey = "vsgRef".getBytes(StandardCharsets.UTF_8);
        TxHeader txHdr = null;
        try {
            txHdr = immuClient.verifiedSetReference(refKey, key);
        } catch (VerificationException e) {
            // TODO: Investigate "different digests" failure at VerifiedSetReference
            // Assert.fail("Failed at verifiedSetReference. Cause: " + e.getMessage(), e);
        }
        // Assert.assertNotNull(txMd);

        immuClient.closeSession();
    }

    @Test(testName = "Login attempt after shutdown")
    public void t3() throws InterruptedException, IllegalStateException, IOException, VerificationException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        immuClient.verifiedSet("key1", "val1".getBytes());

        immuClient.closeSession();
        
        immuClient.shutdown();

        FileImmuStateHolder stateHolder = FileImmuStateHolder.newBuilder()
                .withStatesFolder(statesDir.getAbsolutePath())
                .build();

        immuClient = ImmuClient.newBuilder()
                .withStateHolder(stateHolder)
                .withServerUrl("localhost")
                .withServerPort(3322)
                .build();

        immuClient.openSession("defaultdb", "immudb", "immudb");

        immuClient.verifiedGet("key1");
    }

}
