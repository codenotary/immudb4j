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

import com.google.common.base.Charsets;
import io.codenotary.immudb4j.exceptions.CorruptedDataException;
import io.codenotary.immudb4j.exceptions.VerificationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public class BasicImmuClientTest extends ImmuClientIntegrationTest {

    @Test(testName = "set, get")
    public void t1() throws VerificationException {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        byte[] v0 = new byte[]{0, 1, 2, 3};
        byte[] v1 = new byte[]{3, 2, 1, 0};

        try {
            immuClient.set("k0", v0);
            immuClient.set("k1", v1);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at set.", e);
        }

        byte[] rv0 = null;
        byte[] rv1 = null;
        try {
            rv0 = immuClient.get("k0");
            rv1 = immuClient.get("k1");
        } catch (Exception e) {
            Assert.fail("Failed at get.", e);
        }

        Assert.assertEquals(v0, rv0);
        Assert.assertEquals(v1, rv1);

        Entry sv0 = immuClient.verifiedGet("k0");
        Entry sv1 = immuClient.verifiedGet("k1");

        Assert.assertEquals(sv0.kv.getValue(), v0);
        Assert.assertEquals(sv1.kv.getValue(), v1);

        byte[] v2 = new byte[]{0, 1, 2, 3};

        immuClient.verifiedSet("k2", v2);
        Entry sv2 = immuClient.verifiedGet("k2");
        Assert.assertEquals(v2, sv2.kv.getValue());

        immuClient.logout();
    }

    @Test(testName = "setAll, getAll")
    public void t2() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        List<String> keys = new ArrayList<>();
        keys.add("k0");
        keys.add("k1");

        List<byte[]> values = new ArrayList<>();
        values.add(new byte[]{0, 1, 0, 1});
        values.add(new byte[]{1, 0, 1, 0});

        KVList.KVListBuilder kvListBuilder = KVList.newBuilder();

        for (int i = 0; i < keys.size(); i++) {
            kvListBuilder.add(keys.get(i), values.get(i));
        }

        KVList kvList = kvListBuilder.addAll(new LinkedList<>()).build();

        try {
            immuClient.setAll(kvList);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at setAll.", e);
        }

        List<KV> getAllResult = immuClient.getAll(keys);

        Assert.assertNotNull(getAllResult);
        Assert.assertEquals(keys.size(), getAllResult.size());

        for (int i = 0; i < getAllResult.size(); i++) {
            KV kv = getAllResult.get(i);
            Assert.assertEquals(kv.getKey(), keys.get(i).getBytes(Charsets.UTF_8));
            Assert.assertEquals(kv.getValue(), values.get(i));
        }

        for (int i = 0; i < keys.size(); i++) {
            byte[] v = new byte[0];
            try {
                v = immuClient.get(keys.get(i));
            } catch (Exception e) {
                Assert.fail("Failed at get.", e);
            }
            Assert.assertEquals(v, values.get(i));
        }

        immuClient.logout();
    }

}
