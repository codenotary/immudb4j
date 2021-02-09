package io.codenotary.immudb4j.crypto;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HTree {

    private byte[][][] levels;
    private int maxWidth;
    private int width;
    private byte[] root;

    public static final byte LeafPrefix = 0;
    public static final byte NodePrefix = 1;

    public HTree(int maxWidth) throws IllegalArgumentException {

        if (maxWidth < 1) {
            throw new IllegalArgumentException("maxWidth must be greater or equal to 1");
        }
        this.maxWidth = maxWidth;
        int lw = 1;
        while (lw < maxWidth) {
            lw = lw << 1;
        }
        int height = BigInteger.valueOf(maxWidth - 1).bitLength() + 1;
        levels = new byte[height][][];
        for (int l = 0; l < height; l++) {
            levels[l] = new byte[lw >> l][32];
        }
    }

    public void buildWith(byte[][] digests) throws IllegalArgumentException, NoSuchAlgorithmException {

        if (digests.length > maxWidth) {
            throw new IllegalArgumentException(String.format(
                    "Provided digests' length of %d is bigger than tree's maxWidth of %d.", digests.length, maxWidth));
        }
        if (digests.length == 0) {
            throw new IllegalArgumentException("Provided digests length must be greater than 0.");
        }
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < digests.length; i++) {
            byte[] leaf = new byte[33];
            leaf[0] = LeafPrefix;
            System.arraycopy(digests[i], 0, leaf, 1, digests[i].length);
            levels[0][i] = digest.digest(leaf);
        }
        int l = 0;
        int w = digests.length;
        // to be continued ...
        // (on par with Go's implementation, line 82)
    }

    /**
     * Get the root of the tree.
     * 
     * @return A 32-long array of bytes.
     * @throws IllegalStateException
     */
    public byte[] root() throws IllegalStateException {
        if (width == 0) {
            throw new IllegalStateException();
        }
        return root;
    }
}
