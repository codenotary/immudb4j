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

import java.util.List;

public class MultidatabaseTest extends ImmuClientIntegrationTest {

    @Test(testName = "Interacting with multiple databases (creating them, setting, and getting, listing)")
    public void t1() throws VerificationException {

        immuClient.login("immudb", "immudb");

        immuClient.createDatabase("db1");
        immuClient.createDatabase("db2");

        immuClient.useDatabase("db1");
        byte[] v0 = new byte[]{0, 1, 2, 3};
        try {
            immuClient.set("k0", v0);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at set.", e);
        }

        immuClient.useDatabase("db2");

        byte[] v1 = new byte[]{3, 2, 1, 0};
        try {
            immuClient.set("k1", v1);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at set.", e);
        }

        immuClient.useDatabase("db1");

        Entry entry1 = immuClient.get("k0");
        Assert.assertNotNull(entry1);
        Assert.assertEquals(entry1.getValue(), v0);

        Entry ventry1 = immuClient.verifiedGet("k0");
        Assert.assertNotNull(ventry1);
        Assert.assertEquals(ventry1.getValue(), v0);

        immuClient.useDatabase("db2");

        Entry entry2 = immuClient.get("k1");
        Assert.assertEquals(entry2.getValue(), v1);

        Entry ventry2 = immuClient.verifiedGet("k1");
        Assert.assertEquals(ventry2.getValue(), v1);

        List<String> dbs = immuClient.databases();
        Assert.assertNotNull(dbs);
        Assert.assertEquals(3, dbs.size(), String.format("Expected 3, but got %d dbs: %s", dbs.size(), dbs));
        Assert.assertTrue(dbs.contains("defaultdb"));
        Assert.assertTrue(dbs.contains("db1"));
        Assert.assertTrue(dbs.contains("db2"));

        immuClient.logout();
    }

}
