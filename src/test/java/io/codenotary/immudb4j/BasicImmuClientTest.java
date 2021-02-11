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

// import com.google.common.base.Charsets;
import io.codenotary.immudb4j.exceptions.VerificationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class BasicImmuClientTest extends ImmuClientIntegrationTest {

  @Test
  public void testGet() throws VerificationException {
    immuClient.login("immudb", "immudb");
    immuClient.useDatabase("defaultdb");

    byte[] v0 = new byte[] { 0, 1, 2, 3 };
    byte[] v1 = new byte[] { 3, 2, 1, 0 };

    immuClient.set("k0", v0);
    immuClient.set("k1", v1);

    byte[] rv0 = immuClient.get("k0");
    byte[] rv1 = immuClient.get("k1");

    Assert.assertEquals(v0, rv0);
    Assert.assertEquals(v1, rv1);

    byte[] sv0 = immuClient.safeGet("k0");
    byte[] sv1 = immuClient.safeGet("k1");

    Assert.assertEquals(sv0, v0);
    Assert.assertEquals(sv1, v1);

    byte[] v2 = new byte[] { 0, 1, 2, 3 };

    immuClient.safeSet("k2", v2);
    byte[] sv2 = immuClient.safeGet("k2");
    Assert.assertEquals(v2, sv2);

    immuClient.logout();
  }

  @Test
  public void testRawGetAndSet() throws VerificationException {
    immuClient.login("immudb", "immudb");
    immuClient.useDatabase("defaultdb");

    byte[] v0 = new byte[] { 0, 1, 2, 3 };
    byte[] v1 = new byte[] { 3, 2, 1, 0 };

    // TODO
    // immuClient.rawSet("rawk0", v0);
    // immuClient.rawSet("rawk1", v1);

    // byte[] rv0 = immuClient.rawGet("rawk0");
    // byte[] rv1 = immuClient.rawGet("rawk1");

    // Assert.assertEquals(v0, rv0);
    // Assert.assertEquals(v1, rv1);

    // byte[] sv0 = immuClient.safeRawGet("rawk0");
    // byte[] sv1 = immuClient.safeRawGet("rawk1");

    // Assert.assertEquals(sv0, v0);
    // Assert.assertEquals(sv1, v1);

    // byte[] v2 = new byte[] {0, 1, 2, 3};
    //
    // immuClient.safeRawSet("rawk2", v2);
    // byte[] sv2 = immuClient.safeRawGet("rawk2");
    // Assert.assertEquals(v2, sv2);

    immuClient.logout();
  }

  @Test
  public void testGetAllAndSetAll() {
    immuClient.login("immudb", "immudb");

    List<String> keys = new ArrayList<>();
    keys.add("k0");
    keys.add("k1");

    List<byte[]> values = new ArrayList<>();
    values.add(new byte[] { 0, 1, 0, 1 });
    values.add(new byte[] { 1, 0, 1, 0 });

    KVList.KVListBuilder kvListBuilder = KVList.newBuilder();

    for (int i = 0; i < keys.size(); i++) {
      kvListBuilder.add(keys.get(i), values.get(i));
    }

    KVList kvList = kvListBuilder.addAll(new LinkedList<>()).build();

    immuClient.setAll(kvList);

    // List<KV> getAllResult = immuClient.getAll(keys);

    // Assert.assertNotNull(getAllResult);
    // Assert.assertTrue(getAllResult.size() == keys.size());
    //
    // for (int i = 0; i < getAllResult.size(); i++) {
    // KV kv = getAllResult.get(i);
    // Assert.assertEquals(kv.getKey(), keys.get(i).getBytes(Charsets.UTF_8));
    // Assert.assertEquals(kv.getValue(), values.get(i));
    // }
    //
    // for (int i = 0; i < keys.size(); i++) {
    // byte[] v = immuClient.get(keys.get(i));
    // Assert.assertEquals(v, values.get(i));
    // }

    immuClient.logout();
  }
}
