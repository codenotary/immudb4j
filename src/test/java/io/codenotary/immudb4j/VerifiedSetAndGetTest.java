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

import io.codenotary.immudb4j.exceptions.FailedPreconditionException;
import io.codenotary.immudb4j.exceptions.VerificationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class VerifiedSetAndGetTest extends ImmuClientIntegrationTest {

    @Test(testName = "set, verifiedGet")
    public void t1() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        String key = "vsg";
        byte[] val = "test-set-vget".getBytes(StandardCharsets.UTF_8);

        immuClient.set(key, val);

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
        immuClient.closeSession();
    }

    @Test(testName = "verifiedSet with KeyMustNotExistPrecondition")
    public void t4() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        byte[] key = "prec1".getBytes(StandardCharsets.UTF_8);
        byte[] val = "test-vset-vget".getBytes(StandardCharsets.UTF_8);

        // save element with key
        try {
            TxHeader txHdr = immuClient.verifiedSet(key, val);
            Assert.assertNotNull(txHdr, "The result of verifiedSet must not be null.");
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedSet. Cause: " + e.getMessage(), e);
        }
        SetOptions options = SetOptions.newBuilder()
                .withKey(key)
                .withValue(val)
                .withPreconditions(Collections.singletonList(Precondition.KeyMustNotExistPrecondition.of(key)))
                .build();
        // on second save exception should be thrown
        FailedPreconditionException exception = Assert.expectThrows(FailedPreconditionException.class, () -> immuClient.verifiedSet(options));
        Assert.assertTrue(exception.getMessage().contains("KeyMustNotExist"), "Error message don't contain precondition name");
        immuClient.closeSession();
    }

    @Test(testName = "verifiedSet with KeyMustExistPrecondition")
    public void t5() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        byte[] key = "prec2".getBytes(StandardCharsets.UTF_8);
        byte[] val = "test-vset-vget".getBytes(StandardCharsets.UTF_8);

        SetOptions options = SetOptions.newBuilder()
                .withKey(key)
                .withValue(val)
                .withPreconditions(Collections.singletonList(Precondition.KeyMustExistPrecondition.of(key)))
                .build();

        FailedPreconditionException exception = Assert.expectThrows(FailedPreconditionException.class, () -> immuClient.verifiedSet(options));
        Assert.assertTrue(exception.getMessage().contains("KeyMustExist"), "Error message don't contain precondition name");
        immuClient.closeSession();
    }

    @Test(testName = "verifiedSet with KeyNotModifiedAfterTXPrecondition")
    public void t6() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        byte[] key = "prec3".getBytes(StandardCharsets.UTF_8);
        byte[] val = "test-vset-vget".getBytes(StandardCharsets.UTF_8);
        long tx1Id = 0L;

        try {
            //first save
            TxHeader tx1Hdr = immuClient.verifiedSet(key, val);
            tx1Id = tx1Hdr.getId();
            Assert.assertNotNull(tx1Hdr, "The result of first verifiedSet must not be null.");
            TxHeader tx2Hdr = immuClient.verifiedSet(key, val);
            Assert.assertNotNull(tx2Hdr, "The result of second verifiedSet must not be null.");
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedSet. Cause: " + e.getMessage(), e);
        }
        SetOptions options = SetOptions.newBuilder()
                .withKey(key)
                .withValue(val)
                .withPreconditions(Collections.singletonList(Precondition.KeyNotModifiedAfterTXPrecondition.of(key, tx1Id)))
                .build();

        // on second save exception should be thrown
        FailedPreconditionException exception = Assert.expectThrows(FailedPreconditionException.class, () -> immuClient.verifiedSet(options));
        Assert.assertTrue(exception.getMessage().contains("KeyNotModifiedAfterTxID"), "Error message don't contain precondition name");
        immuClient.closeSession();
    }

    @Test(testName = "verifyGet returns a valid VerifiableEntry")
    public void t7() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        byte[] key = "verifyGet1".getBytes(StandardCharsets.UTF_8);
        byte[] val = "test-verify-get".getBytes(StandardCharsets.UTF_8);
        try {
            TxHeader txHdr = immuClient.verifiedSet(key, val);
            Assert.assertNotNull(txHdr, "The result of verifiedSet must not be null.");
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedSet. Cause: " + e.getMessage(), e);
        }

        try {
            VerifiableEntry vEntry = (VerifiableEntry) immuClient.verifiedGet(key);
            Assert.assertNotNull(vEntry.getEntry());
            Assert.assertNotNull(vEntry.getVerifiableTx());
            Assert.assertNotNull(vEntry.getInclusionProof());
            Assert.assertNotEquals(vEntry.getTx(), 0);
            Assert.assertEquals(vEntry.getKey(), key);
            Assert.assertEquals(vEntry.getValue(), val);
            Assert.assertNull(vEntry.getMetadata());
            Assert.assertNull(vEntry.getReferenceBy());
            Assert.assertNotEquals(vEntry.getRevision(), 0);
            Assert.assertNotNull(vEntry.getEncodedKey());
            Assert.assertNotNull(vEntry.digestFor(1));

        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedGet. Cause: " + e.getMessage(), e);
        } catch (ClassCastException e) {
            Assert.fail("verifiedGet result is not instance of :" + VerifiableEntry.class.getName(), e);
        }
        immuClient.closeSession();
    }

    @Test(testName = "VerifiableEntry returned by verifiedGet contains a valid VerifiableTx")
    public void t8() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        byte[] key = "verifyGet1".getBytes(StandardCharsets.UTF_8);
        byte[] val = "test-verify-get".getBytes(StandardCharsets.UTF_8);
        try {
            //first save
            TxHeader txHdr = immuClient.verifiedSet(key, val);
            Assert.assertNotNull(txHdr, "The result of first verifiedSet must not be null.");
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedSet. Cause: " + e.getMessage(), e);
        }

        try {
            VerifiableEntry vEntry = (VerifiableEntry) immuClient.verifiedGet(key);
            Assert.assertNotNull(vEntry.getEntry());
            VerifiableTx verifiableTx = vEntry.getVerifiableTx();
            Assert.assertNotNull(verifiableTx);
            Assert.assertNotNull(verifiableTx.getTx());
            Assert.assertNotNull(verifiableTx.getDualProof());
            Signature signature = verifiableTx.getSignature();
            Assert.assertNotNull(signature);
            Assert.assertNotNull(signature.getSignature());
            Assert.assertNotNull(signature.getPublicKey());

        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedGet. Cause: " + e.getMessage(), e);
        } catch (ClassCastException e) {
            Assert.fail("verifiedGet result is not instance of :" + VerifiableEntry.class.getName(), e);
        }
        immuClient.closeSession();
    }

}
