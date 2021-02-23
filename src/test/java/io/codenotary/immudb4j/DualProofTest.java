package io.codenotary.immudb4j;

import io.codenotary.immudb4j.crypto.DualProof;
import io.codenotary.immudb4j.crypto.InclusionProof;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class DualProofTest {

    @Test
    public void t1() {

        long id = 1;
        long ts = 1614009046;
        long blTxId = 0;
        byte[] prevAlh = Base64.getDecoder().decode("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=");
        byte[] eh = Base64.getDecoder().decode("rlpXQYggtyAs5tPDFQbqYHjJ7KtX/OV7fUxuxR08GKg=");
        byte[] blRoot = Base64.getDecoder().decode("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
        String alhAsBase64 = "cEH05opQ0CVh9rsz0NpbFmcEITNDoyearaQ1Lp+4slA=";
        byte[] alh = Base64.getDecoder().decode(alhAsBase64);
        TxMetadata txMd = new TxMetadata(id, prevAlh, ts, 1, eh, blTxId, blRoot);

        DualProof dualProof = new DualProof(txMd, txMd, null, null, null, null, null);

        String dualProofStr = dualProof.toString();
        Assert.assertTrue(dualProofStr.contains("DualProof{"));
        Assert.assertTrue(dualProofStr.contains("inclusionProof="));
        Assert.assertTrue(dualProofStr.contains("consistencyProof="));
        Assert.assertTrue(dualProofStr.contains("targetBlTxAlh="));
        Assert.assertTrue(dualProofStr.contains("lastInclusionProof="));
    }

}
