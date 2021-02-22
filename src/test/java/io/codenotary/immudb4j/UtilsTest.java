package io.codenotary.immudb4j;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class UtilsTest {

    @Test
    public void t1() {
        byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
        String s = Utils.convertBase16(hello);
        Assert.assertEquals(s, "68656C6C6F");

        Assert.assertNull(Utils.convertSha256ListToBytesArray(null));

        byte[][] data = new byte[2][4];
        data[0] = "okay".getBytes(StandardCharsets.UTF_8);
        data[1] = "nope".getBytes(StandardCharsets.UTF_8);
        String expected = "[b2theQ==, bm9wZQ==]";

        s = Utils.toStringAsBase64Values(data);

        Assert.assertEquals(s, expected);
    }

}
