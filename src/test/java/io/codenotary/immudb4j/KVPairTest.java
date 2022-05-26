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
        Assert.assertEquals(kv1.getTxId(), kv2.getTxId());

        Assert.assertEquals(kv1, kv2);

        Assert.assertEquals(kv1.digestFor(0).length, Consts.SHA256_SIZE);

        long txId = 123;
        KV kv3 = new KVPair(key, val, txId);
        Assert.assertEquals(kv3.getTxId(), txId);

    }

}
