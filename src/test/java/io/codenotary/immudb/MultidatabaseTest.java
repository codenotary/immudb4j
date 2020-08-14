/*
Copyright 2019-2020 vChain, Inc.

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
package io.codenotary.immudb;

import io.codenotary.immudb.crypto.VerificationException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MultidatabaseTest extends ImmuClientIntegrationTest {

  @Test
  public void testCreateDatabase() throws VerificationException {
    immuClient.login("immudb", "immudb");

    immuClient.createDatabase("db1");
    immuClient.createDatabase("db2");

    immuClient.useDatabase("db1");
    byte[] v0 = new byte[] {0, 1, 2, 3};
    immuClient.set("k0", v0);

    immuClient.useDatabase("db2");
    byte[] v1 = new byte[] {3, 2, 1, 0};
    immuClient.set("k1", v1);

    immuClient.useDatabase("db1");
    byte[] rv0 = immuClient.get("k0");
    Assert.assertEquals(v0, rv0);
    byte[] sv0 = immuClient.safeGet("k0");
    Assert.assertEquals(sv0, v0);

    immuClient.useDatabase("db2");
    byte[] rv1 = immuClient.get("k1");
    Assert.assertEquals(v1, rv1);
    byte[] sv1 = immuClient.safeGet("k1");
    Assert.assertEquals(sv1, v1);

    immuClient.logout();
  }
}
