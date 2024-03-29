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

public class StreamSetAndGetTest extends ImmuClientIntegrationTest {

    @Test(testName = "stream set, get")
    public void t1() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        String key = "key1";
        byte[] val = new byte[] { 1, 2, 3, 4, 5 };

        TxHeader txHdr = null;
        try {
            txHdr = immuClient.streamSet(key, val);
        } catch (InterruptedException e) {
            Assert.fail("Failed at set.", e);
        }
        Assert.assertNotNull(txHdr);

        Entry entry1 = immuClient.streamGet(key);
        Assert.assertNotNull(entry1);
        Assert.assertEquals(entry1.getValue(), val);

        immuClient.delete(key);

        try {
            immuClient.streamGet(key);
            Assert.fail("key not found exception expected");
        } catch (KeyNotFoundException e) {
            // expected
        } catch (Exception e) {
            Assert.fail("key not found exception expected");
        }

        immuClient.closeSession();
    }

}
