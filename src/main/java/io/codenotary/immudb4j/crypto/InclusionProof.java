package io.codenotary.immudb4j.crypto;

import io.codenotary.immudb4j.Utils;

public class InclusionProof {

    public final int leaf;
    public final int width;
    public final byte[][] terms;

    public InclusionProof(int leaf, int width, byte[][] terms) {
        this.leaf = leaf;
        this.width = width;
        this.terms = terms;
    }

    @Override
    public String toString() {
        return String.format("InclusionProof{ leaf: %d, width: %d, terms: %s }", leaf, width,
                termsToBase16());
    }

    private String termsToBase16() {

        StringBuffer sb = new StringBuffer("[");
        for (int i = 0; i < terms.length; i++) {
            sb.append(Utils.convertBase16(terms[i]));
            if (i < terms.length - 1) {
                sb.append(" ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

}
