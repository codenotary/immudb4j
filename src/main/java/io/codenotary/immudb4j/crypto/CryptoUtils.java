/*
Copyright 2019-2021 vChain, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package io.codenotary.immudb4j.crypto;

import com.google.protobuf.ByteString;
import io.codenotary.immudb4j.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;


public class CryptoUtils {

    public static final int SHA256_SIZE = 32;

    private static final byte SET_KEY_PREFIX = 0;
    private static final byte PLAIN_VALUE_PREFIX = 0;
    private static final byte REFERENCE_VALUE_PREFIX = 1;

    /**
     * This method returns a SHA256 digest of the provided data.
     */
    public static byte[] sha256Sum(byte[] data) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            return sha256.digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[][] digestsFrom(List<ByteString> terms) {

        if (terms == null) {
            return null;
        }
        int size = terms.size();
        byte[][] result = new byte[size][SHA256_SIZE];
        for (int i = 0; i < size; i++) {
            byte[] term = terms.get(i).toByteArray();
            System.arraycopy(term, 0, result[i], 0, SHA256_SIZE);
        }
        return result;
    }

    /**
     * Copy the provided `digest` array into a byte[32] array.
     */
    public static byte[] digestFrom(byte[] digest) {
        if (digest.length != SHA256_SIZE) {
            return null;
        }
        byte[] d = new byte[SHA256_SIZE];
        System.arraycopy(digest, 0, d, 0, SHA256_SIZE);
        return d;
    }

    public static KV encodeKV(byte[] key, byte[] value) {
        return new KVPair(
                wrapWithPrefix(key, SET_KEY_PREFIX),
                wrapWithPrefix(value, PLAIN_VALUE_PREFIX)
        );
    }

    public static KV encodeReference(byte[] key, byte[] referencedKey, long atTx) {
        return new KVPair(
                wrapWithPrefix(key, SET_KEY_PREFIX),
                wrapReferenceValueAt(wrapWithPrefix(referencedKey, SET_KEY_PREFIX), atTx)
        );
    }

    private static byte[] wrapWithPrefix(byte[] b, byte prefix) {
        if (b == null) {
            return null;
        }
        byte[] wb = new byte[b.length + 1];
        wb[0] = prefix;
        System.arraycopy(b, 0, wb, 1, b.length);
        return wb;
    }

    private static byte[] wrapReferenceValueAt(byte[] key, long atTx) {
        byte[] refVal = new byte[1 + 8 + key.length];
        refVal[0] = REFERENCE_VALUE_PREFIX;

        Utils.putUint64(atTx, refVal, 1);

        System.arraycopy(key, 0, refVal, 1 + 8, key.length);
        return refVal;
    }

    public static boolean verifyDualProof(DualProof proof,
                                          long sourceTxId, long targetTxId,
                                          byte[] sourceAlh, byte[] targetAlh) {

        if (proof == null || proof.sourceTxMetadata == null || proof.targetTxMetadata == null
                || proof.sourceTxMetadata.id != sourceTxId || proof.targetTxMetadata.id != targetTxId) {
            return false;
        }

        if (proof.sourceTxMetadata.id == 0 || proof.sourceTxMetadata.id > proof.targetTxMetadata.id) {
            return false;
        }

        if (sourceAlh != proof.sourceTxMetadata.alh() || targetAlh != proof.targetTxMetadata.alh()) {
            return false;
        }

        if (sourceTxId < proof.targetTxMetadata.blTxId) {
            if (!CryptoUtils.verifyInclusion(
                    proof.inclusionProof,
                    sourceTxId,
                    proof.targetTxMetadata.blTxId,
                    leafFor(proof.targetBlTxAlh),
                    proof.targetTxMetadata.blRoot)) {
                return false;
            }
        }

        if (proof.sourceTxMetadata.blTxId > 0) {
            if (!CryptoUtils.verifyConsistency(
                    proof.consistencyProof,
                    proof.sourceTxMetadata.blTxId,
                    proof.targetTxMetadata.blTxId,
                    proof.sourceTxMetadata.blRoot,
                    proof.targetTxMetadata.blRoot
            )) {
                return false;
            }
        }

        if (proof.targetTxMetadata.blTxId > 0) {
            if (!verifyLastInclusion(
                    proof.lastInclusionProof,
                    proof.targetTxMetadata.blTxId,
                    leafFor(proof.targetBlTxAlh),
                    proof.targetTxMetadata.blRoot
            )) {
                return false;
            }
        }

        if (sourceTxId < proof.targetTxMetadata.blTxId) {
            return verifyLinearProof(proof.linearProof,
                    proof.targetTxMetadata.blTxId, targetTxId, proof.targetBlTxAlh, targetAlh);
        }
        return verifyLinearProof(proof.linearProof, sourceTxId, targetTxId, sourceAlh, targetAlh);
    }

    private static byte[] leafFor(byte[] d) {
        byte[] b = new byte[1 + SHA256_SIZE];
        b[0] = Constants.LEAF_PREFIX;
        System.arraycopy(d, 0, b, 1, d.length);
        return sha256Sum(b);
    }

    private static boolean verifyLinearProof(LinearProof proof,
                                             long sourceTxId, long targetTxId,
                                             byte[] sourceAlh, byte[] targetAlh) {

        if (proof == null || proof.sourceTxId != sourceTxId || proof.targetTxId != targetTxId) {
            return false;
        }
        if (proof.sourceTxId == 0 || proof.sourceTxId > proof.targetTxId
                || proof.terms.length == 0 || sourceAlh != proof.terms[0]) {
            return false;
        }
        if (proof.terms.length != targetTxId - sourceTxId + 1) {
            return false;
        }
        byte[] calculatedAlh = proof.terms[0];

        for (int i = 1; i < proof.terms.length; i++) {
            byte[] bs = new byte[Constants.TX_ID_SIZE + 2 * SHA256_SIZE];
            Utils.putUint64(proof.sourceTxId + i, bs);
            System.arraycopy(calculatedAlh, 0, bs, Constants.TX_ID_SIZE, calculatedAlh.length);
            System.arraycopy(proof.terms[i], 0, bs, Constants.TX_ID_SIZE + SHA256_SIZE, proof.terms[i].length);
            calculatedAlh = sha256Sum(bs);
        }

        return Arrays.equals(targetAlh, calculatedAlh);
    }

    public static boolean verifyInclusion(byte[][] iProof, long i, long j, byte[] iLeaf, byte[] jRoot) {
        if (i > j || i == 0 || (i < j && iProof.length == 0)) {
            return false;
        }
        byte[] ciRoot = evalInclusion(iProof, i, j, iLeaf);
        return jRoot == ciRoot;
    }

    private static byte[] evalInclusion(byte[][] iProof, long i, long j, byte[] iLeaf) {
        long i1 = i - 1;
        long j1 = j - 1;
        byte[] ciRoot = iLeaf;

        byte[] b = new byte[1 + CryptoUtils.SHA256_SIZE * 2];
        b[0] = Constants.NODE_PREFIX;

        for (byte[] h : iProof) {
            if (i1 % 2 == 0 && i1 != j1) {
                System.arraycopy(ciRoot, 0, b, 1, ciRoot.length);
                System.arraycopy(h, 0, b, CryptoUtils.SHA256_SIZE + 1, h.length);
            } else {
                System.arraycopy(h, 0, b, 1, h.length);
                System.arraycopy(ciRoot, 0, b, CryptoUtils.SHA256_SIZE + 1, ciRoot.length);
            }

            ciRoot = CryptoUtils.sha256Sum(b);

            i1 >>= 1;
            j1 >>= 1;
        }

        return ciRoot;
    }

    public static boolean verifyLastInclusion(byte[][] iProof, long i, byte[] leaf, byte[] root) {
        if (i == 0) {
            return false;
        }
        return Arrays.equals(root, evalLastInclusion(iProof, i, leaf));
    }

    private static byte[] evalLastInclusion(byte[][] iProof, long i, byte[] leaf) {
        long i1 = i - 1;
        byte[] root = leaf;

        byte[] b = new byte[1 + SHA256_SIZE * 2];
        b[0] = Constants.NODE_PREFIX;

        for (byte[] h : iProof) {
            System.arraycopy(h, 0, b, 1, h.length);
            System.arraycopy(root, 0, b, SHA256_SIZE + 1, root.length);
            root = sha256Sum(b);
            i1 >>= 1;
        }
        return root;
    }

    public static boolean verifyInclusion(InclusionProof proof, byte[] digest, byte[] root) {

        if ((proof == null) || (proof.terms == null)) {
            return false;
        }

        byte[] leaf = new byte[1 + CryptoUtils.SHA256_SIZE];
        leaf[0] = Constants.LEAF_PREFIX;
        System.arraycopy(digest, 0, leaf, 1, digest.length);
        byte[] calcRoot = CryptoUtils.sha256Sum(leaf);
        int i = proof.leaf;
        int r = proof.width - 1;

        for (int j = 0; j < proof.terms.length; j++) {
            byte[] b = new byte[65]; // 65 = 1 + 2*32
            b[0] = Constants.NODE_PREFIX;

            if (i % 2 == 0 && i != r) {
                System.arraycopy(calcRoot, 0, b, 1, 32);
                System.arraycopy(proof.terms[j], 0, b, 33, 32);
            } else {
                System.arraycopy(proof.terms[j], 0, b, 1, 32);
                System.arraycopy(calcRoot, 0, b, 33, 32);
            }

            calcRoot = CryptoUtils.sha256Sum(b);
            i /= 2;
            r /= 2;
        }

        return i == r && Arrays.equals(root, calcRoot);
    }

    public static boolean verifyConsistency(byte[][] cProof, long i, long j, byte[] iRoot, byte[] jRoot) {
        if (i > j || i == 0 || (i < j && cProof.length == 0)) {
            return false;
        }

        if (i == j && cProof.length == 0) {
            return iRoot == jRoot;
        }

        byte[][] result = evalConsistency(cProof, i, j);
        byte[] ciRoot = result[0];
        byte[] cjRoot = result[1];

        return iRoot == ciRoot && jRoot == cjRoot;
    }

    // Returns a "pair" (two) byte[] values (ciRoot, cjRoot), that's why
    // the returned data is byte[][] just to keep it simple.
    public static byte[][] evalConsistency(byte[][] cProof, long i, long j) {

        long fn = i - 1;
        long sn = j - 1;

        while (fn % 2 == 1) {
            fn >>= 1;
            sn >>= 1;
        }

        byte[] ciRoot = cProof[0];
        byte[] cjRoot = cProof[1];

        byte[] b = new byte[1 + CryptoUtils.SHA256_SIZE * 2];
        b[0] = Constants.NODE_PREFIX;

        for (int k = 1; k < cProof.length; k++) {
            byte[] h = cProof[k];
            if (fn % 2 == 1 || fn == sn) {
                System.arraycopy(h, 0, b, 1, h.length);

                System.arraycopy(ciRoot, 0, b, 1 + CryptoUtils.SHA256_SIZE, ciRoot.length);
                ciRoot = CryptoUtils.sha256Sum(b);

                System.arraycopy(cjRoot, 0, b, 1 + CryptoUtils.SHA256_SIZE, cjRoot.length);
                cjRoot = CryptoUtils.sha256Sum(b);

                while (fn % 2 == 0 && fn != 0) {
                    fn >>= 1;
                    sn >>= 1;
                }
            } else {
                System.arraycopy(cjRoot, 0, b, 1, cjRoot.length);
                System.arraycopy(h, 0, b, 1 + CryptoUtils.SHA256_SIZE, h.length);
                cjRoot = CryptoUtils.sha256Sum(b);
            }
            fn >>= 1;
            sn >>= 1;
        }

        byte[][] result = new byte[2][CryptoUtils.SHA256_SIZE];
        result[0] = ciRoot;
        result[1] = cjRoot;
        return result;
    }


}
