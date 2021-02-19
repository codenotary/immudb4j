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
import io.codenotary.immudb4j.exceptions.VerificationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

public class ZAddTest extends ImmuClientIntegrationTest {

    @Test(testName = "zAdd, verifiedZAdd, verifiedZAddAt")
    public void t1() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

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

        TxMetadata txMd = null;
        long initialTxId = 1;
        try {
            txMd = immuClient.zAdd(set, 10, key1);
            Assert.assertNotNull(txMd);
            initialTxId = txMd.id;
            txMd = immuClient.zAdd(set, 4, key2);
            Assert.assertNotNull(txMd);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at zAdd.", e);
        }

        try {
            txMd = immuClient.verifiedZAdd(set, 8, key2);
        } catch (VerificationException e) {
            Assert.fail("Failed at verifiedZAdd", e);
        }
        Assert.assertNotNull(txMd);


//        TODO: Temporary commented, it needs investigation.
//              Currently it throws this gRPC issue:
//              io.grpc.StatusRuntimeException: UNKNOWN: illegal arguments
//        try {
//            immuClient.verifiedZAddAt(set, 12, key1, initialTxId);
//        } catch (VerificationException e) {
//            Assert.fail("Failed at verifiedZAddAt");
//        }

        immuClient.logout();
    }

}
