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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class ScanTest extends ImmuClientIntegrationTest {

    @Test(testName = "scan zscan")
    public void t1() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        byte[] value1 = { 0, 1, 2, 3 };
        byte[] value2 = { 4, 5, 6, 7 };

        immuClient.set("scan1", value1);
        immuClient.set("scan2", value2);

        immuClient.set("zadd1", value1);
        immuClient.set("zadd2", value2);

        immuClient.zAdd("set1", "zadd1", 1);
        immuClient.zAdd("set1", "zadd2", 2);

        immuClient.zAdd("set2", "zadd1", 2);
        immuClient.zAdd("set2", "zadd2", 1);

        List<Entry> scanResult = immuClient.scanAll("scan");
        System.out.println(scanResult.size());

        Assert.assertEquals(scanResult.size(), 2);
        Assert.assertEquals(scanResult.get(0).getKey(), "scan1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(scanResult.get(0).getValue(), value1);
        Assert.assertEquals(scanResult.get(1).getKey(), "scan2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(scanResult.get(1).getValue(), value2);

        Assert.assertTrue(immuClient.scanAll("scan").size() > 0);

        Assert.assertEquals(immuClient.scanAll("scan".getBytes(), "scan1".getBytes(), false, 1).size(), 1);

        Iterator<Entry> scanResult1 = immuClient.scan("scan", false, 5);

        int i = 0;

        while (scanResult1.hasNext()) {
            Assert.assertEquals(scanResult1.next().getKey(), scanResult.get(i).getKey());
            i++;
        }

        Assert.assertEquals(i, 2);

        Assert.assertFalse(immuClient.scan("nonexistent-prefix").hasNext());

        List<ZEntry> zScan1 = immuClient.zScanAll("set1", false, 5);
        Assert.assertEquals(zScan1.size(), 2);

        Assert.assertEquals(zScan1.get(0).getSet(), "set1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(zScan1.get(0).getKey(), "zadd1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(zScan1.get(0).getScore(), 1.0);
        Assert.assertEquals(zScan1.get(0).getAtTx(), 0);
        Assert.assertEquals(zScan1.get(0).getEntry().getValue(), value1);

        List<ZEntry> zScan2 = immuClient.zScanAll("set2");
        Assert.assertEquals(zScan2.size(), 2);

        Iterator<ZEntry> zScan3 = immuClient.zScan("set2");
        i = 0;

        while (zScan3.hasNext()) {
            Assert.assertEquals(zScan3.next().getKey(), zScan2.get(i).getKey());
            i++;
        }

        Assert.assertEquals(i, 2);

        Assert.assertFalse(immuClient.zScan("nonexistent-set").hasNext());

        immuClient.closeSession();
    }

}
