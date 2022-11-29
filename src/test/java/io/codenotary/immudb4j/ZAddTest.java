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

import java.nio.charset.StandardCharsets;

public class ZAddTest extends ImmuClientIntegrationTest {

    @Test(testName = "zAdd, verifiedZAdd, verifiedZAddAt")
    public void t1() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        String set = "test-zadd";
        String key1 = "test-zadd-key1";
        byte[] val1 = "val123".getBytes(StandardCharsets.UTF_8);
        String key2 = "key2";
        byte[] val2 = "val234".getBytes(StandardCharsets.UTF_8);

        try {
            immuClient.set(key1, val1);
            immuClient.set(key2, val2);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at set.", e);
        }

        TxHeader txHdr = null;
        try {
            txHdr = immuClient.zAdd(set, key1, 10);
            Assert.assertNotNull(txHdr);

            txHdr = immuClient.zAdd(set, key2, 4);
            Assert.assertNotNull(txHdr);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at zAdd.", e);
        }

        try {
            txHdr = immuClient.verifiedZAdd(set, key2, 8);
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedZAdd", e);
        }
        Assert.assertNotNull(txHdr);

        immuClient.closeSession();
    }

}
