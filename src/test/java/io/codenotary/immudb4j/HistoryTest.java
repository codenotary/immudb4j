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

import io.codenotary.immudb4j.exceptions.KeyNotFoundException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HistoryTest extends ImmuClientIntegrationTest {

    @Test(testName = "set, history", priority = 2)
    public void t1() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        byte[] value1 = { 0, 1, 2, 3 };
        byte[] value2 = { 4, 5, 6, 7 };
        byte[] value3 = { 8, 9, 10, 11 };

        immuClient.set("history1", value1);
        immuClient.set("history1", value2);
        immuClient.set("history2", value1);
        immuClient.set("history2", value2);
        immuClient.set("history2", value3);

        List<Entry> historyResponse1 = immuClient.historyAll("history1", false, 0, 2);

        Assert.assertEquals(historyResponse1.size(), 2);

        Assert.assertEquals(historyResponse1.get(0).getKey(), "history1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse1.get(0).getValue(), value1);

        Assert.assertEquals(historyResponse1.get(1).getKey(), "history1".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse1.get(1).getValue(), value2);

        List<Entry> historyResponse2 = immuClient.historyAll("history2", false, 0, 3);

        Assert.assertEquals(historyResponse2.size(), 3);

        Assert.assertEquals(historyResponse2.get(0).getKey(), "history2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse2.get(0).getValue(), value1);

        Assert.assertEquals(historyResponse2.get(1).getKey(), "history2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse2.get(1).getValue(), value2);

        Assert.assertEquals(historyResponse2.get(2).getKey(), "history2".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(historyResponse2.get(2).getValue(), value3);

        historyResponse2 = immuClient.historyAll("history2", false, 2, 1);
        Assert.assertNotNull(historyResponse2);
        Assert.assertEquals(historyResponse2.size(), 1);

        Iterator<Entry> entriesIt = immuClient.history("history2", false, 2, 1);
        Assert.assertTrue(entriesIt.hasNext());

        Entry entry = entriesIt.next();
        Assert.assertNotNull(entry);

        Assert.assertFalse(entriesIt.hasNext());

        try {
            entriesIt.next();
            Assert.fail("NoSuchElementException exception expected");
        } catch (NoSuchElementException e) {
            // exception is expected here
        }

        try {
            immuClient.historyAll("nonExisting", false, 0, 0);
            Assert.fail("key not found exception expected");
        } catch (KeyNotFoundException e) {
            // exception is expected here
        }

        Iterator<Entry> entriesIt2 = immuClient.history("nonExisting", false, 0, 0);

        Assert.assertFalse(entriesIt2.hasNext());

        immuClient.closeSession();
    }

}
