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
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

public class ReferenceTest extends ImmuClientIntegrationTest {

    @Test(testName = "set, setReference, setReferenceAt")
    public void t1() {
        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        byte[] key = "testRef".getBytes(StandardCharsets.UTF_8);
        byte[] val = "abc".getBytes(StandardCharsets.UTF_8);

        TxHeader setTxHdr = null;
        try {
            setTxHdr = immuClient.set(key, val);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at set.", e);
        }

        byte[] ref1Key = "ref1_to_testRef".getBytes(StandardCharsets.UTF_8);
        byte[] ref2Key = "ref2_to_testRef".getBytes(StandardCharsets.UTF_8);

        TxHeader ref1TxHdr = null;
        try {
            ref1TxHdr = immuClient.setReference(ref1Key, key);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at setReference", e);
        }
        Assert.assertNotNull(ref1TxHdr);


        TxHeader ref2TxHdr = null;
        try {
            ref2TxHdr = immuClient.setReferenceAt(ref2Key, key, setTxHdr.id);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at setReferenceAt.", e);
        }
        Assert.assertNotNull(ref2TxHdr);

        // And `verifiedSetReference` & `verifiedSetReferenceAt` tests are included `VerifiedSetAndGetTest`.


        immuClient.logout();
    }

}
