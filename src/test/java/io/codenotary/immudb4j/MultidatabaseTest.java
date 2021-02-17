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

import io.codenotary.immudb4j.exceptions.VerificationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class MultidatabaseTest extends ImmuClientIntegrationTest {

    @Test
    public void testCreateDatabase() throws VerificationException {
        immuClient.login("immudb", "immudb");

        immuClient.createDatabase("db1");
        immuClient.createDatabase("db2");

        immuClient.useDatabase("db1");
        byte[] v0 = new byte[]{0, 1, 2, 3};
        immuClient.set("k0", v0);

        immuClient.useDatabase("db2");

        byte[] v1 = new byte[]{3, 2, 1, 0};
        immuClient.set("k1", v1);

        immuClient.useDatabase("db1");

        byte[] gv0 = immuClient.get("k0");
        Assert.assertEquals(v0, gv0);

        Entry ev0 = immuClient.verifiedGet("k0");
        Assert.assertNotNull(ev0);
        Assert.assertEquals(ev0.kv.getValue(), v0);

        immuClient.useDatabase("db2");

        byte[] gv1 = immuClient.get("k1");
        Assert.assertEquals(v1, gv1);

        Entry evgv1 = immuClient.verifiedGet("k1");
        Assert.assertEquals(evgv1.kv.getValue(), v1);

        List<String> dbs = immuClient.databases();
        Assert.assertNotNull(dbs);
        Assert.assertEquals(3, dbs.size());
        Assert.assertTrue(dbs.contains("defaultdb"));
        Assert.assertTrue(dbs.contains("db1"));
        Assert.assertTrue(dbs.contains("db2"));

        immuClient.logout();
    }
}
