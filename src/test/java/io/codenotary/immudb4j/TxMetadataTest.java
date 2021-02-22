package io.codenotary.immudb4j;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public class TxMetadataTest {

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

        Assert.assertTrue(txMd.toString().contains(alhAsBase64));
    }

}
