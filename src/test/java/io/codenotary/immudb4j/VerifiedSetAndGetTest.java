/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

import java.nio.charset.StandardCharsets;

public class VerifiedSetAndGetTest extends ImmuClientIntegrationTest {

    @Test(testName = "set, verifiedGet")
    public void t1() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

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

        Assert.assertEquals(val, vEntry.kv.getValue());

        immuClient.logout();
    }

    @Test(testName = "verifiedSet, verifiedGet, verifiedGetAt, verifiedGetSince")
    public void t2() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        String key = "vsg";
        byte[] val = "test-vset-vget".getBytes(StandardCharsets.UTF_8);

        // verifiedSet
        try {
            TxMetadata txMd = immuClient.verifiedSet(key, val);
            Assert.assertNotNull(txMd, "The result of verifiedSet must not be null.");
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
        Assert.assertEquals(val, vEntry.kv.getValue());

        // verifiedGetAt
        try {
            vEntry = immuClient.verifiedGetAt(key.getBytes(StandardCharsets.UTF_8), vEntry.txId);
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedGetAt. Cause: " + e.getMessage(), e);
        }
        Assert.assertEquals(val, vEntry.kv.getValue());

        // verifiedGetSince
        try {
            vEntry = immuClient.verifiedGetSince(key.getBytes(StandardCharsets.UTF_8), vEntry.txId);
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedGetSince. Cause: " + e.getMessage(), e);
        }
        Assert.assertEquals(val, vEntry.kv.getValue());

        // verifiedSetReference
        byte[] refKey = "vsgRef".getBytes(StandardCharsets.UTF_8);
        TxMetadata txMd = null;
        try {
            txMd = immuClient.verifiedSetReference(refKey, key.getBytes(StandardCharsets.UTF_8));
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedSetReference. Cause: " + e.getMessage(), e);
        }
        Assert.assertNotNull(txMd);

        immuClient.logout();
    }



}
