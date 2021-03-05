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
import java.util.Arrays;
import java.util.List;

public class ScanTest extends ImmuClientIntegrationTest {

    @Test(testName = "scan", priority = 2)
    public void t1() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        byte[] value1 = {0, 1, 2, 3};
        byte[] value2 = {4, 5, 6, 7};

        try {
            immuClient.set("scan1", value1);
            immuClient.set("scan2", value2);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at set.", e);
        }

        List<KV> scanResult = immuClient.scan("scan", 1, 5, false);

        Assert.assertEquals(scanResult.size(), 2);
        Assert.assertEquals(scanResult.get(0).getKey(), "scan1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(scanResult.get(0).getValue(), value1);
        Assert.assertEquals(scanResult.get(1).getKey(), "scan2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(scanResult.get(1).getValue(), value2);

        Assert.assertTrue(immuClient.scan("scan").size() > 0);

        Assert.assertEquals(immuClient.scan("scan", "scan1", 1, 5, false).size(), 1);

        immuClient.logout();
    }

    @Test(testName = "set, zAdd, zScan", priority = 3)
    public void t2() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        byte[] value1 = {0, 1, 2, 3};
        byte[] value2 = {4, 5, 6, 7};

        try {
            immuClient.set("zadd1", value1);
            immuClient.set("zadd2", value2);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at set.", e);
        }

        TxMetadata set1TxMd = null;
        try {
            immuClient.zAdd("set1", 1, "zadd1");
            set1TxMd = immuClient.zAdd("set1", 2, "zadd2");

            immuClient.zAdd("set2", 2, "zadd1");
            immuClient.zAdd("set2", 1, "zadd2");
        } catch (CorruptedDataException e) {
            Assert.fail("Failed to zAdd", e);
        }

        Assert.assertNotNull(set1TxMd);

        List<KV> zScan1 = immuClient.zScan("set1", set1TxMd.id, 5, false);

        //Assert.assertEquals(zScan1.size(), 2);
        Assert.assertEquals(zScan1.size(), 3);

        Assert.assertEquals(zScan1.get(0).getKey(), "zadd1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(zScan1.get(0).getValue(), value1);

        List<KV> zScan2 = immuClient.zScan("set2", 5, false);

        Assert.assertEquals(zScan2.size(), 2);

        immuClient.logout();
    }

}
