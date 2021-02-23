package io.codenotary.immudb4j;

import io.codenotary.immudb4j.crypto.InclusionProof;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

public class InclusionProofTest {

    @Test
    public void t1() {
        byte[][] data = new byte[2][4];
        data[0] = "okay".getBytes(StandardCharsets.UTF_8);
        data[1] = "nope".getBytes(StandardCharsets.UTF_8);
        String expected = "[6F6B6179 6E6F7065]";

        InclusionProof inclusionProof = new InclusionProof(1, 1, data);

        Assert.assertNotNull(inclusionProof);

        Assert.assertTrue(inclusionProof.toString().contains(expected));
    }

}
