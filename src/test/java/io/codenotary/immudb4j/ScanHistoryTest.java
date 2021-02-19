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
import java.util.List;

public class ScanHistoryTest extends ImmuClientIntegrationTest {

    @Test(testName = "set, history", priority = 2)
    public void t1() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        byte[] value1 = {0, 1, 2, 3};
        byte[] value2 = {4, 5, 6, 7};
        byte[] value3 = {8, 9, 10, 11};

        try {
            immuClient.set("history1", value1);
            immuClient.set("history1", value2);
            immuClient.set("history2", value1);
            immuClient.set("history2", value2);
            immuClient.set("history2", value3);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at set.", e);
        }

        List<KV> historyResponse1 = immuClient.history("history1", 10, 0, false);

        Assert.assertEquals(historyResponse1.size(), 2);

        Assert.assertEquals(historyResponse1.get(0).getKey(), "history1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse1.get(0).getValue(), value1);

        Assert.assertEquals(historyResponse1.get(1).getKey(), "history1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse1.get(1).getValue(), value2);

        List<KV> historyResponse2 = immuClient.history("history2", 10, 0, false);

        Assert.assertEquals(historyResponse2.size(), 3);

        Assert.assertEquals(historyResponse2.get(0).getKey(), "history2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse2.get(0).getValue(), value1);

        Assert.assertEquals(historyResponse2.get(1).getKey(), "history2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse2.get(1).getValue(), value2);

        Assert.assertEquals(historyResponse2.get(2).getKey(), "history2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse2.get(2).getValue(), value3);

        List<KV> nonExisting = immuClient.history("nonExisting", 10, 0, false);
        Assert.assertTrue(nonExisting.isEmpty());

        immuClient.logout();
    }

    @Test(testName = "scan", priority = 2)
    public void t2() {

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

        List<KV> scan = immuClient.scan("scan", 1, 5, false);

        Assert.assertEquals(scan.size(), 2);
        Assert.assertEquals(scan.get(0).getKey(), "scan1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(scan.get(0).getValue(), value1);
        Assert.assertEquals(scan.get(1).getKey(), "scan2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(scan.get(1).getValue(), value2);

        immuClient.logout();
    }

    @Test(testName = "set, zAdd, zScan", priority = 3)
    public void t3() {

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

        try {
            immuClient.zAdd("set1", 1, "zadd1");
            immuClient.zAdd("set1", 2, "zadd2");

            immuClient.zAdd("set2", 2, "zadd1");
            immuClient.zAdd("set2", 1, "zadd2");
        } catch (CorruptedDataException e) {
            Assert.fail("Failed to zAdd", e);
        }

        List<KV> zScan1 = immuClient.zScan("set1", 5, false);

        Assert.assertEquals(zScan1.size(), 2);
        Assert.assertEquals(zScan1.get(0).getKey(), "zadd1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(zScan1.get(0).getValue(), value1);
        Assert.assertEquals(zScan1.get(1).getKey(), "zadd2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(zScan1.get(1).getValue(), value2);

        List<KV> zScan2 = immuClient.zScan("set2", 5, false);

        Assert.assertEquals(zScan2.size(), 2);
        Assert.assertEquals(zScan2.get(0).getKey(), "zadd2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(zScan2.get(0).getValue(), value2);
        Assert.assertEquals(zScan2.get(1).getKey(), "zadd1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(zScan2.get(1).getValue(), value1);

        immuClient.logout();
    }

}
