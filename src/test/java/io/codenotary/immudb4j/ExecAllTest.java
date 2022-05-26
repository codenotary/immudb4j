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

import io.codenotary.immudb4j.basics.Pair;
import io.codenotary.immudb4j.basics.Triple;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

public class ExecAllTest extends ImmuClientIntegrationTest {

    @Test(testName = "execAll for setting KVs")
    public void t1() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        byte[] item1 = "execAll_key1".getBytes(StandardCharsets.UTF_8);

        byte[] item2 = "execAll_key2".getBytes(StandardCharsets.UTF_8);

        TxHeader txHdr = immuClient.execAll(
                Arrays.asList(
                        Pair.of(item1, item1),
                        Pair.of(item2, item2)
                ),
                null, // No refList provided.
                null // No zaddList provided.
        );

        Assert.assertNotNull(txHdr);
        Assert.assertEquals(txHdr.nEntries, 2);

        // It works! Left here just for any other verification
        // (since `immuclient get execAll_key1` fails).
        // List<KV> result = immuClient.scan("execAll");
        // result.forEach(kv -> {
        //            System.out.printf("KV(%s,%s)\n", new String(kv.getKey()), new String(kv.getValue()));
        //        }
        // );

        txHdr = immuClient.execAll(
                null, //
                Arrays.asList(
                        Pair.of("ref1".getBytes(StandardCharsets.UTF_8), item1),
                        Pair.of("ref2".getBytes(StandardCharsets.UTF_8), item2)
                ),
                Collections.singletonList(Triple.of("set1", 1.0, "execAll_key1"))
        );

        Assert.assertNotNull(txHdr);
        Assert.assertEquals(txHdr.nEntries, 3);

        immuClient.logout();
    }

}
