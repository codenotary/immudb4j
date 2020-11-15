/*
Copyright 2019-2020 vChain, Inc.

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

import io.codenotary.immudb4j.crypto.VerificationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ScanHistoryTest extends ImmuClientIntegrationTest {

    @Test
    public void testHistory() {
        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        byte[] value1 = {0, 1, 2, 3};
        byte[] value2 = {4, 5, 6, 7};
        byte[] value3 = {8, 9, 10, 11};

        immuClient.set("history1", value1);
        immuClient.set("history1", value2);
        immuClient.set("history2", value1);
        immuClient.set("history2", value2);
        immuClient.set("history2", value3);

        List<KV> historyResponse1 = immuClient.history("history1");

        Assert.assertEquals(historyResponse1.size(), 2);

        Assert.assertEquals(historyResponse1.get(0).getKey(), "history1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse1.get(0).getValue(), value1);

        Assert.assertEquals(historyResponse1.get(1).getKey(), "history1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse1.get(1).getValue(), value2);

        List<KV> historyResponse2 = immuClient.history("history2");

        Assert.assertEquals(historyResponse2.size(), 3);

        Assert.assertEquals(historyResponse2.get(0).getKey(), "history2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse2.get(0).getValue(), value1);

        Assert.assertEquals(historyResponse2.get(1).getKey(), "history2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse2.get(1).getValue(), value2);

        Assert.assertEquals(historyResponse2.get(2).getKey(), "history2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse2.get(2).getValue(), value3);

        List<KV> nonExisting = immuClient.history("nonExisting");
        Assert.assertTrue(nonExisting.isEmpty());

        immuClient.logout();
    }

    @Test
    public void testScan() {
        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");
        byte[] value1 = {0, 1, 2, 3};
        byte[] value2 = {4, 5, 6, 7};

        immuClient.set("history1", value1);
        immuClient.set("history2", value2);

        // TODO COMPLETE
        String offset = null;
        long limit = 0;
        boolean reverse = false;
        boolean deep = false;
        List<KV> scan = immuClient.scan("history", offset, limit, reverse, deep);


        immuClient.logout();
    }

    @Test
    public void testIScan() {
        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");
        byte[] value1 = {0, 1, 2, 3};
        byte[] value2 = {4, 5, 6, 7};

        immuClient.set("history1", value1);
        immuClient.set("history2", value2);

        // TODO COMPLETE
        String offset = null;
        long limit = 0;
        boolean reverse = false;
        boolean deep = false;
        KVPage kvPage = immuClient.iScan(1, 20);

        immuClient.logout();
    }

    @Test
    public void testZScan() throws VerificationException {
        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");
        byte[] value1 = {0, 1, 2, 3};
        byte[] value2 = {4, 5, 6, 7};

        immuClient.set("history1", value1);
        immuClient.set("history2", value2);

        immuClient.zAdd("set", 1, "history1");
        immuClient.zAdd("set", 2, "history2");
        immuClient.safeZAdd("safeSet", 2, "history1");
        immuClient.safeZAdd("safeSet", 1, "history2");

        // TODO COMPLETE
        String offset = null;
        long limit = 0;
        boolean reverse = false;
        List<KV> zScan1 = immuClient.zScan("set", offset, limit, reverse);
        List<KV> zScan2 = immuClient.zScan("safeSet", offset, limit, reverse);

        immuClient.logout();
    }
}
