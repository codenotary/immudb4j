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

import io.codenotary.immudb4j.exceptions.VerificationException;
import io.grpc.StatusRuntimeException;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class MultidatabaseTest extends ImmuClientIntegrationTest {

    @Test(testName = "createDatabase without open session", expectedExceptions = IllegalStateException.class)
    public void t1() {
        immuClient.createDatabase("db1");
    }

    @Test(testName = "loadDatabase without open session", expectedExceptions = IllegalStateException.class)
    public void t2() {
        immuClient.loadDatabase("db1");
    }

    @Test(testName = "unloadDatabase without open session", expectedExceptions = IllegalStateException.class)
    public void t3() {
        immuClient.unloadDatabase("db1");
    }

    @Test(testName = "deleteDatabase without open session", expectedExceptions = IllegalStateException.class)
    public void t4() {
        immuClient.deleteDatabase("db1");
    }

    @Test(testName = "Interacting with multiple databases (creating them, setting, and getting, listing)")
    public void t5() throws VerificationException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        immuClient.createDatabase("db1", true);
        immuClient.createDatabase("db2", true);

        immuClient.closeSession();

        immuClient.openSession("db1", "immudb", "immudb");

        byte[] v0 = new byte[] { 0, 1, 2, 3 };

        immuClient.set("k0", v0);

        immuClient.closeSession();

        immuClient.openSession("db2", "immudb", "immudb");

        byte[] v1 = new byte[] { 3, 2, 1, 0 };

        immuClient.set("k1", v1);

        immuClient.closeSession();

        immuClient.openSession("db1", "immudb", "immudb");

        Entry entry1 = immuClient.get("k0");
        Assert.assertNotNull(entry1);
        Assert.assertEquals(entry1.getValue(), v0);

        Entry ventry1 = immuClient.verifiedGet("k0");
        Assert.assertNotNull(ventry1);
        Assert.assertEquals(ventry1.getValue(), v0);

        immuClient.closeSession();

        immuClient.openSession("db2", "immudb", "immudb");

        Entry entry2 = immuClient.get("k1");
        Assert.assertEquals(entry2.getValue(), v1);

        Entry ventry2 = immuClient.verifiedGet("k1");
        Assert.assertEquals(ventry2.getValue(), v1);

        List<Database> dbs = immuClient.databases();
        Assert.assertNotNull(dbs);
        Assert.assertEquals(3, dbs.size(), String.format("Expected 3, but got %d dbs: %s", dbs.size(), dbs));
        Assert.assertEquals(dbs.get(0).getName(), "defaultdb");
        Assert.assertEquals(dbs.get(1).getName(), "db1");
        Assert.assertEquals(dbs.get(2).getName(), "db2");

        immuClient.closeSession();
    }

    @Test(testName = "create, unload and delete database")
    public void t6() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        immuClient.createDatabase("manageddb");

        immuClient.unloadDatabase("manageddb");

        immuClient.deleteDatabase("manageddb");

        try {
            immuClient.loadDatabase("manageddb");
            Assert.fail("exception expected");
        } catch (StatusRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("database does not exist"));
        }

        immuClient.closeSession();
    }
}
