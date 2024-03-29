/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
import io.codenotary.immudb4j.Consts;
import io.codenotary.immudb4j.Utils;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class CryptoUtils {

    // FYI: Interesting enough, Go returns a fixed value for sha256.Sum256(nil) and
    // this value is:
    // [227 176 196 66 152 252 28 20 154 251 244 200 153 111 185 36 39 174 65 228
    // 100 155 147 76 164 149 153 27 120 82 184 85]
    // whose Base64 encoded value is 47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=.
    // But Java's MessageDigest fails with NPE when providing a null value. So we
    // treat this case as in Go.
    private static final byte[] SHA256_SUM_OF_NULL = Base64.getDecoder()
            .decode("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=");

    /**
     * This method returns a SHA256 digest of the provided data.
     */
    public static byte[] sha256Sum(byte[] data) {
        if (data == null) {
            return SHA256_SUM_OF_NULL;
        }
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
        byte[][] result = new byte[size][Consts.SHA256_SIZE];
        for (int i = 0; i < size; i++) {
            byte[] term = terms.get(i).toByteArray();
            System.arraycopy(term, 0, result[i], 0, Consts.SHA256_SIZE);
        }
        return result;
    }

    /**
     * Copy the provided `digest` array into a byte[32] array.
     */
    public static byte[] digestFrom(byte[] digest) {
        if (digest.length != Consts.SHA256_SIZE) {
            return null;
        }
        byte[] d = new byte[Consts.SHA256_SIZE];
        System.arraycopy(digest, 0, d, 0, Consts.SHA256_SIZE);
        return d;
    }

    public static byte[] advanceLinearHash(byte[] alh, long txID, byte[] term) {
        byte[] bs = new byte[Consts.TX_ID_SIZE + 2 * Consts.SHA256_SIZE];
        Utils.putUint64(txID, bs);
        System.arraycopy(alh, 0, bs, Consts.TX_ID_SIZE, Consts.SHA256_SIZE);
        // innerHash = hash(ts + mdLen + md + nentries + eH + blTxID + blRoot)
        System.arraycopy(term, 0, bs, Consts.TX_ID_SIZE + Consts.SHA256_SIZE, Consts.SHA256_SIZE);
        // hash(txID + prevAlh + innerHash)
        return sha256Sum(bs);
    }

    public static boolean verifyLinearAdvanceProof(
            LinearAdvanceProof proof,
            long startTxID,
            long endTxID,
            byte[] endAlh,
            byte[] treeRoot,
            long treeSize) {
        //
        // Old
        // \ Merkle Tree
        // \
        // \
        // \ Additional Inclusion proof
        // \ for those nodes
        // \ in new Merkle Tree
        // \ ......................
        // \ / \
        // \
        // \+--+ +--+ +--+ +--+ +--+
        // -----------| |-----| |-----| |-----| |-----| |
        // +--+ +--+ +--+ +--+ +--+
        //
        // startTxID endTxID
        //
        if (endTxID < startTxID) {
            // This must not happen - that's an invalid proof
            return false;
        }

        if (endTxID <= startTxID + 1) {
            // Linear Advance Proof is not needed
            return true;
        }

        if (proof == null ||
                proof.terms.length != endTxID - startTxID ||
                proof.inclusionProofs.size() != (endTxID - startTxID) - 1) {
            // Check more preconditions that would indicate broken proof
            return false;
        }

        byte[] calculatedAlh = proof.terms[0]; // alh at startTx+1

        for (long txID = startTxID + 1; txID < endTxID; txID++) {
            // Ensure the node in the chain is included in the target Merkle Tree
            if (!verifyInclusion(
                    proof.inclusionProofs.get((int) (txID - startTxID - 1)).terms,
                    txID,
                    treeSize,
                    leafFor(calculatedAlh),
                    treeRoot)) {
                return false;
            }

            // Get the Alh for the next transaction
            calculatedAlh = advanceLinearHash(calculatedAlh, txID + 1, proof.terms[(int) (txID - startTxID)]);
        }

        // We must end up with the final Alh - that one is also checked for inclusion
        // but in different part of the proof
        return Arrays.equals(calculatedAlh, endAlh);
    }

    public static boolean verifyDualProof(DualProof proof,
            long sourceTxId, long targetTxId,
            byte[] sourceAlh, byte[] targetAlh) {

        if (proof == null || proof.sourceTxHeader == null || proof.targetTxHeader == null
                || proof.sourceTxHeader.getId() != sourceTxId || proof.targetTxHeader.getId() != targetTxId) {
            return false;
        }

        if (proof.sourceTxHeader.getId() == 0 || proof.sourceTxHeader.getId() > proof.targetTxHeader.getId()) {
            return false;
        }

        if (!Arrays.equals(sourceAlh, proof.sourceTxHeader.alh())
                || !Arrays.equals(targetAlh, proof.targetTxHeader.alh())) {
            return false;
        }

        if (proof.linearAdvanceProof == null) {
            // Find the range startTxID / endTxID to fill with linear inclusion proof
            long startTxID = proof.sourceTxHeader.getBlTxId();
            long endTxID = Math.min(proof.sourceTxHeader.getId(), proof.targetTxHeader.getBlTxId());

            if (endTxID > startTxID + 1) {
                // Linear Advance Proof is needed
                throw new RuntimeException("Linear Advance Proof is needed");
            }
        }

        if (sourceTxId < proof.targetTxHeader.getBlTxId()) {
            if (!CryptoUtils.verifyInclusion(
                    proof.inclusionProof,
                    sourceTxId,
                    proof.targetTxHeader.getBlTxId(),
                    leafFor(sourceAlh),
                    proof.targetTxHeader.getBlRoot())) {
                return false;
            }
        }

        if (proof.sourceTxHeader.getBlTxId() > 0) {
            if (!CryptoUtils.verifyConsistency(
                    proof.consistencyProof,
                    proof.sourceTxHeader.getBlTxId(),
                    proof.targetTxHeader.getBlTxId(),
                    proof.sourceTxHeader.getBlRoot(),
                    proof.targetTxHeader.getBlRoot())) {
                return false;
            }
        }

        if (proof.targetTxHeader.getBlTxId() > 0) {
            if (!verifyLastInclusion(
                    proof.lastInclusionProof,
                    proof.targetTxHeader.getBlTxId(),
                    leafFor(proof.targetBlTxAlh),
                    proof.targetTxHeader.getBlRoot())) {
                return false;
            }
        }

        if (sourceTxId < proof.targetTxHeader.getBlTxId()) {
            if (!verifyLinearProof(proof.linearProof,
                    proof.targetTxHeader.getBlTxId(), targetTxId, proof.targetBlTxAlh, targetAlh)) {
                return false;
            }

            // Verify that the part of the linear proof consumed by the new merkle tree is
            // consistent with that Merkle Tree
            // In this case, this is the whole chain to the SourceTxID from the previous
            // Merkle Tree.
            // The sourceTxID consistency is already proven using proof.InclusionProof
            return verifyLinearAdvanceProof(
                    proof.linearAdvanceProof,
                    proof.sourceTxHeader.getBlTxId(),
                    sourceTxId,
                    sourceAlh,
                    proof.targetTxHeader.getBlRoot(),
                    proof.targetTxHeader.getBlTxId());
        }

        if (!verifyLinearProof(proof.linearProof, sourceTxId, targetTxId, sourceAlh, targetAlh)) {
            return false;
        }

        // Verify that the part of the linear proof consumed by the new merkle tree is
        // consistent with that Merkle Tree
        // In this case, this is the whole linear chain between the old Merkle Tree and
        // the new Merkle Tree. The last entry
        // in the new Merkle Tree is already proven through the LastInclusionProof, the
        // remaining part of the liner proof
        // that goes outside of the target Merkle Tree will be validated in future
        // DualProof validations
        return verifyLinearAdvanceProof(
                proof.linearAdvanceProof,
                proof.sourceTxHeader.getBlTxId(),
                proof.targetTxHeader.getBlTxId(),
                proof.targetBlTxAlh,
                proof.targetTxHeader.getBlRoot(),
                proof.targetTxHeader.getBlTxId());
    }

    private static byte[] leafFor(byte[] d) {
        byte[] b = new byte[1 + Consts.SHA256_SIZE];
        b[0] = Consts.LEAF_PREFIX;
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
                || proof.terms.length == 0 || !Arrays.equals(sourceAlh, proof.terms[0])) {
            return false;
        }
        if (proof.terms.length != targetTxId - sourceTxId + 1) {
            return false;
        }
        byte[] calculatedAlh = proof.terms[0];

        for (int i = 1; i < proof.terms.length; i++) {
            byte[] bs = new byte[Consts.TX_ID_SIZE + 2 * Consts.SHA256_SIZE];
            Utils.putUint64(proof.sourceTxId + i, bs);
            System.arraycopy(calculatedAlh, 0, bs, Consts.TX_ID_SIZE, calculatedAlh.length);
            System.arraycopy(proof.terms[i], 0, bs, Consts.TX_ID_SIZE + Consts.SHA256_SIZE, proof.terms[i].length);
            calculatedAlh = sha256Sum(bs);
        }

        return Arrays.equals(targetAlh, calculatedAlh);
    }

    public static boolean verifyInclusion(byte[][] iProof, long i, long j, byte[] iLeaf, byte[] jRoot) {
        if (i > j || i == 0 || (i < j && iProof.length == 0)) {
            return false;
        }
        byte[] ciRoot = evalInclusion(iProof, i, j, iLeaf);
        return Arrays.equals(jRoot, ciRoot);
    }

    private static byte[] evalInclusion(byte[][] iProof, long i, long j, byte[] iLeaf) {
        long i1 = i - 1;
        long j1 = j - 1;
        byte[] ciRoot = iLeaf;

        byte[] b = new byte[1 + Consts.SHA256_SIZE * 2];
        b[0] = Consts.NODE_PREFIX;

        for (byte[] h : iProof) {
            if (i1 % 2 == 0 && i1 != j1) {
                System.arraycopy(ciRoot, 0, b, 1, ciRoot.length);
                System.arraycopy(h, 0, b, Consts.SHA256_SIZE + 1, h.length);
            } else {
                System.arraycopy(h, 0, b, 1, h.length);
                System.arraycopy(ciRoot, 0, b, Consts.SHA256_SIZE + 1, ciRoot.length);
            }

            ciRoot = sha256Sum(b);

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

        byte[] b = new byte[1 + Consts.SHA256_SIZE * 2];
        b[0] = Consts.NODE_PREFIX;

        for (byte[] h : iProof) {
            System.arraycopy(h, 0, b, 1, h.length);
            System.arraycopy(root, 0, b, Consts.SHA256_SIZE + 1, root.length);
            root = sha256Sum(b);
            i1 >>= 1;
        }
        return root;
    }

    public static boolean verifyInclusion(InclusionProof proof, byte[] digest, byte[] root) {

        if (proof == null) {
            return false;
        }

        byte[] leaf = new byte[1 + Consts.SHA256_SIZE];
        leaf[0] = Consts.LEAF_PREFIX;
        System.arraycopy(digest, 0, leaf, 1, digest.length);
        byte[] calcRoot = sha256Sum(leaf);
        int i = proof.leaf;
        int r = proof.width - 1;

        if (proof.terms != null) {
            for (int j = 0; j < proof.terms.length; j++) {
                byte[] b = new byte[1 + 2 * Consts.SHA256_SIZE];
                b[0] = Consts.NODE_PREFIX;

                if (i % 2 == 0 && i != r) {
                    Utils.copy(calcRoot, b, 1);
                    Utils.copy(proof.terms[j], b, 1 + Consts.SHA256_SIZE);
                } else {
                    Utils.copy(proof.terms[j], b, 1);
                    Utils.copy(calcRoot, b, 1 + Consts.SHA256_SIZE);
                }

                calcRoot = sha256Sum(b);
                i /= 2;
                r /= 2;
            }
        }

        return i == r && Arrays.equals(root, calcRoot);
    }

    public static boolean verifyConsistency(byte[][] cProof, long i, long j, byte[] iRoot, byte[] jRoot) {
        if (i > j || i == 0 || (i < j && cProof.length == 0)) {
            return false;
        }

        if (i == j && cProof.length == 0) {
            return Arrays.equals(iRoot, jRoot);
        }

        byte[][] result = evalConsistency(cProof, i, j);
        byte[] ciRoot = result[0];
        byte[] cjRoot = result[1];

        return Arrays.equals(iRoot, ciRoot) && Arrays.equals(jRoot, cjRoot);
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
        byte[] cjRoot = cProof[0];

        byte[] b = new byte[1 + Consts.SHA256_SIZE * 2];
        b[0] = Consts.NODE_PREFIX;

        for (int k = 1; k < cProof.length; k++) {
            byte[] h = cProof[k];
            if (fn % 2 == 1 || fn == sn) {
                System.arraycopy(h, 0, b, 1, h.length);

                System.arraycopy(ciRoot, 0, b, 1 + Consts.SHA256_SIZE, ciRoot.length);
                ciRoot = sha256Sum(b);

                System.arraycopy(cjRoot, 0, b, 1 + Consts.SHA256_SIZE, cjRoot.length);
                cjRoot = sha256Sum(b);

                while (fn % 2 == 0 && fn != 0) {
                    fn >>= 1;
                    sn >>= 1;
                }
            } else {
                System.arraycopy(cjRoot, 0, b, 1, cjRoot.length);
                System.arraycopy(h, 0, b, 1 + Consts.SHA256_SIZE, h.length);
                cjRoot = sha256Sum(b);
            }
            fn >>= 1;
            sn >>= 1;
        }

        byte[][] result = new byte[2][Consts.SHA256_SIZE];
        result[0] = ciRoot;
        result[1] = cjRoot;
        return result;
    }

    /**
     * Reads a public key from a DER file.
     */
    public static PublicKey getDERPublicKey(String filepath) throws Exception {

        File f = new File(filepath);
        FileInputStream fis = new FileInputStream(f);
        DataInputStream dis = new DataInputStream(fis);
        byte[] keyBytes = new byte[(int) f.length()];
        dis.readFully(keyBytes);
        dis.close();

        String publicKeyPEM = new String(keyBytes)
                .replace("-----BEGIN PUBLIC KEY-----\n", "")
                .replaceAll(System.lineSeparator(), "")
                .replace("-----END PUBLIC KEY-----", "")
                .trim();

        byte[] decoded = Base64.getDecoder().decode(publicKeyPEM);

        X509EncodedKeySpec spec = new X509EncodedKeySpec(decoded);
        KeyFactory kf = KeyFactory.getInstance("EC");
        return kf.generatePublic(spec);
    }

}
