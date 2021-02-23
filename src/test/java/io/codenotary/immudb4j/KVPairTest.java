package io.codenotary.immudb4j;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

public class KVPairTest {

    @Test
    public void t1() {
        byte[] key = "someKey".getBytes(StandardCharsets.UTF_8);
        byte[] val = "someVal".getBytes(StandardCharsets.UTF_8);

        KV kv1 = new KVPair(key, val);
        KV kv2 = new KVPair(key, val);

        Assert.assertEquals(kv1.getKey(), key);
        Assert.assertEquals(kv1.getValue(), val);
        Assert.assertEquals(kv1.hashCode(), kv2.hashCode());
        Assert.assertEquals(kv1, kv2);

        Assert.assertEquals(kv1.digest().length, Consts.SHA256_SIZE);
    }

}
