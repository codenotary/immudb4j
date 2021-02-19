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

import io.codenotary.immudb4j.exceptions.CorruptedDataException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SetAndGetTest extends ImmuClientIntegrationTest {

    @Test(testName = "set, get")
    public void t1() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        String key = "key1";
        byte[] val = new byte[]{1, 2, 3, 4, 5};

        try {
            TxMetadata txMd = immuClient.set(key, val);
            Assert.assertNotNull(txMd);
        } catch (CorruptedDataException e) {
            Assert.fail("Failed at set.", e);
        }

        byte[] got = immuClient.get(key);

        Assert.assertEquals(val, got);

        immuClient.logout();
    }

}
