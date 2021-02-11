package io.codenotary.immudb4j.crypto;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import com.google.common.base.Strings;
import io.codenotary.immudb4j.exceptions.MaxWidthExceededException;
import io.codenotary.immudb4j.Utils;

/**
 * This is a hash tree implementation.<br/>
 * It is closely based on the Go version that is part of immudb 0.9 Go SDK.
 */
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

    public void buildWith(byte[][] digests)
            throws IllegalArgumentException, NoSuchAlgorithmException, MaxWidthExceededException {

        if (digests == null || digests.length == 0) {
            throw new IllegalArgumentException(
                    "Provided digests must be non-null and have a length greater than 0.");
        }
        if (digests.length > maxWidth) {
            throw new MaxWidthExceededException(String.format(
                    "Provided digests' length of %d is bigger than tree's maxWidth of %d.",
                    digests.length, maxWidth));
        }

        for (int i = 0; i < digests.length; i++) {
            byte[] leaf = new byte[33]; // 33 = 32 (sha256.Size) + 1
            leaf[0] = LeafPrefix;
            System.arraycopy(digests[i], 0, leaf, 1, digests[i].length);
            levels[0][i] = CryptoUtils.digest(leaf);
        }

        int l = 0;
        int w = digests.length;

        while (w > 1) {
            byte[] b = new byte[65]; // 65 = 2 x 32 (sha256.Size) + 1
            b[0] = NodePrefix;
            int wn = 0;
            for (int i = 0; i + 1 < w; i += 2) {
                System.arraycopy(levels[l][i], 0, b, 1, levels[l][i].length);
                System.arraycopy(levels[l][i + 1], 0, b, 33, levels[l][i].length);
                levels[l + 1][wn] = CryptoUtils.digest(b);
                wn++;
            }
            if (w % 2 == 1) {
                levels[l + 1][wn] = levels[l][w - 1];
                wn++;
            }
            l++;
            w = wn;
        }
        width = digests.length;
        root = levels[l][0];
    }

    /**
     * Get the root of the tree.
     * 
     * @return A 32-long array of bytes.
     * @throws IllegalStateException when internal state (width) is not ready to
     *                               report it.
     */
    public byte[] root() throws IllegalStateException {
        if (width == 0) {
            throw new IllegalStateException();
        }
        return root;
    }

    /**
     * InclusionProof returns the shortest list of additional nodes required to
     * compute the root. It's an adaption of the algorithm for proof construction
     * that exists at github.com/codenotary/merkletree.
     * 
     * @param i Index of the node from which the inclusion proof will be provided.
     */
    public InclusionProof inclusionProof(int i) throws IllegalArgumentException {
        if (i >= width) {
            throw new IllegalArgumentException(String
                    .format("Provided index (%d) is higher then the tree's width (%d).", i, width));
        }
        int m = i;
        int n = width;
        int offset = 0, l = 0, r = 0;

        if (width == 1) {
            return new InclusionProof(i, width, null);
        }

        byte[][] terms = new byte[0][32];
        while (true) {
            int d = Utils.countBits(n - 1);
            int k = 1 << (d - 1);
            if (m < k) {
                l = offset + k;
                r = offset + n - 1;
                n = k;
            } else {
                l = offset;
                r = offset + k - 1;
                m = m - k;
                n = n - k;
                offset += k;
            }
            int layer = Utils.countBits(r - l);
            int index = l / (1 << layer);

            byte[][] newterms = new byte[1 + terms.length][32];
            newterms[0] = levels[layer][index];
            System.arraycopy(terms, 0, newterms, 1, terms.length);
            terms = newterms;

            if (n < 1 || (n == 1 && m == 0)) {
                return new InclusionProof(i, width, terms);
            }
        }
    }

    public boolean verifyInclusion(InclusionProof proof, byte[] digest, byte[] root)
            throws NoSuchAlgorithmException {

        if ((proof == null) || (proof.terms == null)) {
            return false;
        }

        byte[] leaf = new byte[33];
        leaf[0] = LeafPrefix;
        System.arraycopy(digest, 0, leaf, 1, digest.length);
        byte[] calcRoot = CryptoUtils.digest(leaf);
        int i = proof.leaf;
        int r = proof.width - 1;

        for (int j = 0; j < proof.terms.length; j++) {
            byte[] b = new byte[65]; // 65 = 1 + 2*32
            b[0] = NodePrefix;

            if (i % 2 == 0 && i != r) {
                System.arraycopy(calcRoot, 0, b, 1, 32);
                System.arraycopy(proof.terms[j], 0, b, 33, 32);
            } else {
                System.arraycopy(proof.terms[j], 0, b, 1, 32);
                System.arraycopy(calcRoot, 0, b, 33, 32);
            }

            calcRoot = CryptoUtils.digest(b);
            i /= 2;
            r /= 2;
        }

        return i == r && Utils.haveSameEntries(root, calcRoot);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        String tab = "";
        for (int i = levels.length - 1; i >= 0; i--) {
            sb.append(Strings.repeat("  ", (1 << i) - 1));
            tab = Strings.repeat("  ", (1 << (i + 1)) - 1);
            for (int j = 0; j < levels[i].length; j++) {
                sb.append(String.format("%s%s", Utils.convertBase16(levels[i][j]), tab));
            }
            sb.append("\n");
        }
        return sb.toString();
    }

}
