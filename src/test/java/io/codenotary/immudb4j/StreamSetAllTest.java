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
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class StreamSetAllTest extends ImmuClientIntegrationTest {

    @Test(testName = "setAll & getAll")
    public void t1() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        String key1 = "sga-key1";
        byte[] val1 = new byte[] { 1 };
        String key2 = "sga-key2";
        byte[] val2 = new byte[] { 2, 3 };
        String key3 = "sga-key3";
        byte[] val3 = new byte[] { 3, 4, 5 };

        final List<KVPair> kvs = KVListBuilder.newBuilder()
                .add(new KVPair(key1, val1))
                .add(new KVPair(key2, val2))
                .add(new KVPair(key3, val3))
                .entries();

        try {
            TxHeader txHdr = immuClient.streamSetAll(kvs);
            Assert.assertNotNull(txHdr);
        } catch (InterruptedException|CorruptedDataException e) {
            Assert.fail("Failed at SetAll.", e);
        }

        List<String> keys = Arrays.asList(key1, key2, key3);
        List<Entry> got = immuClient.getAll(keys);

        Assert.assertEquals(kvs.size(), got.size());

        for (int i = 0; i < kvs.size(); i++) {
            Assert.assertEquals(got.get(i).getValue(), kvs.get(i).getValue(), String.format("Expected: %s got: %s", kvs.get(i), got.get(i)));
        }

        immuClient.closeSession();
    }

}
