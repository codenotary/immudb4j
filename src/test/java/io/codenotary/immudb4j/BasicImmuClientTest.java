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

import com.google.common.base.Charsets;
import io.codenotary.immudb4j.exceptions.CorruptedDataException;
import io.codenotary.immudb4j.exceptions.KeyNotFoundException;
import io.codenotary.immudb4j.exceptions.VerificationException;
import io.grpc.StatusRuntimeException;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class BasicImmuClientTest extends ImmuClientIntegrationTest {

    @Test(testName = "set, get")
    public void t1() throws VerificationException, CorruptedDataException, InterruptedException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        byte[] v0 = new byte[] { 0, 1, 2, 3 };
        byte[] v1 = new byte[] { 3, 2, 1, 0 };

        TxHeader hdr0 = immuClient.set("k0", v0);
        Assert.assertNotNull(hdr0);

        TxHeader hdr1 = immuClient.set("k1", v1);
        Assert.assertNotNull(hdr1);

        Entry entry0 = immuClient.get("k0");
        Assert.assertEquals(entry0.getValue(), v0);

        Entry entry1 = immuClient.get("k1");
        Assert.assertEquals(entry1.getValue(), v1);

        Entry ventry0 = immuClient.verifiedGet("k0");
        Assert.assertEquals(ventry0.getValue(), v0);

        Entry ventry1 = immuClient.verifiedGet("k1");
        Assert.assertEquals(ventry1.getValue(), v1);

        byte[] v2 = new byte[] { 0, 1, 2, 3 };

        TxHeader hdr2 = immuClient.verifiedSet("k2", v2);
        Assert.assertNotNull(hdr2);

        Entry ventry2 = immuClient.verifiedGet("k2");
        Assert.assertEquals(v2, ventry2.getValue());

        Entry e = immuClient.getSinceTx("k2", hdr2.getId());
        Assert.assertNotNull(e);
        Assert.assertEquals(e.getValue(), v2);

        Assert.assertEquals(v2, immuClient.getSinceTx("k2", ventry2.getRevision()).getValue());

        immuClient.set("k0", v1);

        Assert.assertEquals(v0, immuClient.getAtRevision("k0", entry0.getRevision()).getValue());
        Assert.assertEquals(v1, immuClient.getAtRevision("k0", entry0.getRevision()+1).getValue());

        Assert.assertEquals(v0, immuClient.verifiedGetAtRevision("k0", entry0.getRevision()).getValue());
        Assert.assertEquals(v1, immuClient.verifiedGetAtRevision("k0", entry0.getRevision()+1).getValue());
        
        try {
            immuClient.verifiedGet("non-existent-key");
            Assert.fail("Failed at verifiedGet.");
        } catch (KeyNotFoundException _) {
        }

        try {
            immuClient.getSinceTx("non-existent-key", 1);
            Assert.fail("Failed at getSinceTx.");
        } catch (KeyNotFoundException _) {
        }

        try {
            immuClient.verifiedGetSinceTx("non-existent-key", 1);
            Assert.fail("Failed at verifiedGetSinceTx.");
        } catch (KeyNotFoundException _) {
        }

        try {
            immuClient.getAtRevision("k0", entry0.getRevision()+2);
            Assert.fail("Failed at getSinceTx.");
        } catch (StatusRuntimeException e1) {
            Assert.assertTrue(e1.getMessage().contains("invalid key revision number"));
        }

        try {
            immuClient.verifiedGetAtRevision("k0", entry0.getRevision()+2);
            Assert.fail("Failed at verifiedGetAtRevision.");
        } catch (StatusRuntimeException e1) {
            Assert.assertTrue(e1.getMessage().contains("invalid key revision number"));
        }


        immuClient.closeSession();
    }

    @Test(testName = "setAll, getAll")
    public void t2() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        List<String> keys = new ArrayList<>();
        keys.add("k0");
        keys.add("k1");

        List<byte[]> values = new ArrayList<>();
        values.add(new byte[] { 0, 1, 0, 1 });
        values.add(new byte[] { 1, 0, 1, 0 });

        KVListBuilder kvListBuilder = KVListBuilder.newBuilder();

        for (int i = 0; i < keys.size(); i++) {
            kvListBuilder.add(keys.get(i), values.get(i));
        }

        try {
            immuClient.setAll(kvListBuilder.entries());
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at setAll.", e);
        }

        List<Entry> getAllResult = immuClient.getAll(keys);

        Assert.assertNotNull(getAllResult);
        Assert.assertEquals(keys.size(), getAllResult.size());

        for (int i = 0; i < getAllResult.size(); i++) {
            Entry entry = getAllResult.get(i);
            Assert.assertEquals(entry.getKey(), keys.get(i).getBytes(Charsets.UTF_8));
            Assert.assertEquals(entry.getValue(), values.get(i));
        }

        for (int i = 0; i < keys.size(); i++) {
            Entry entry = immuClient.get(keys.get(i));
            Assert.assertEquals(entry.getValue(), values.get(i));
        }

        immuClient.closeSession();
    }
}
