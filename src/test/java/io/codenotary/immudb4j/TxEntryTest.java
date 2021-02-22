package io.codenotary.immudb4j;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class TxEntryTest {

    @Test
    public void t1() {
        byte[] k = "key".getBytes(StandardCharsets.UTF_8);

        TxEntry txe = new TxEntry(k);
        Assert.assertTrue(Arrays.equals(k, txe.getKey()));

        txe.setKey(k);
        Assert.assertTrue(Arrays.equals(k, txe.getKey()));
    }

}
