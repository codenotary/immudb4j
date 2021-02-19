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

        TxMetadata setTxMd = null;
        try {
            setTxMd = immuClient.set(key, val);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at set.", e);
        }

        byte[] ref1Key = "ref1_to_testRef".getBytes(StandardCharsets.UTF_8);
        byte[] ref2Key = "ref2_to_testRef".getBytes(StandardCharsets.UTF_8);

        TxMetadata ref1TxMd = null;
        try {
            ref1TxMd = immuClient.setReference(ref1Key, key);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at setReference", e);
        }
        Assert.assertNotNull(ref1TxMd);

        TxMetadata ref2TxMd = null;
        try {
            ref2TxMd = immuClient.setReferenceAt(ref2Key, key, setTxMd.id);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at setReferenceAt.", e);
        }
        Assert.assertNotNull(ref2TxMd);

        immuClient.logout();
    }

}
