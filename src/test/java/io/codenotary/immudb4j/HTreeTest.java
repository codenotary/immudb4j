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
package io.codenotary.immudb4j;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;

import org.testng.Assert;
import org.testng.annotations.Test;
import io.codenotary.immudb4j.crypto.CryptoUtils;
import io.codenotary.immudb4j.crypto.HTree;
import io.codenotary.immudb4j.crypto.InclusionProof;
import io.codenotary.immudb4j.exceptions.MaxWidthExceededException;

public class HTreeTest {

    @Test(testName = "Empty HTree", expectedExceptions = IllegalArgumentException.class)
    public void t1() {

        new HTree(0);
    }

    @Test(testName = "HTree init & root", expectedExceptions = IllegalStateException.class)
    public void t2() {

        final int maxWidth = 1000;

        HTree tree = new HTree(maxWidth);
        Assert.assertNotNull(tree);

        tree.root();
    }

    @Test(testName = "HTree buildWith, root, inclusionProof, verifyInclusion")
    public void t3() {

        final int maxWidth = 1000;

        HTree tree = new HTree(maxWidth);

        byte[][] digests = new byte[maxWidth][32];

        for (int i = 0; i < digests.length; i++) {
            ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(i);
            digests[i] = CryptoUtils.sha256Sum(bb.array());
        }

        try {
            tree.buildWith(digests);
        } catch (IllegalArgumentException | MaxWidthExceededException | NoSuchAlgorithmException e) {
            Assert.fail("Got exception while building the tree.", e);
        }

        byte[] root = null;
        try {
            root = tree.root();
        } catch (IllegalStateException e) {
            Assert.fail("Got exception getting the root of the tree.", e);
        }

        for (int i = 0; i < digests.length; i++) {

            InclusionProof proof = null;
            try {
                proof = tree.inclusionProof(i);
            } catch (IllegalArgumentException e) {
                Assert.fail(String.format("Got exception while calling inclusionProof(%d).", i), e);
            }
            Assert.assertNotNull(proof);

            Assert.assertTrue(CryptoUtils.verifyInclusion(proof, digests[i], root));

            Assert.assertFalse(CryptoUtils.verifyInclusion(proof, CryptoUtils.sha256Sum(digests[i]), root));

            Assert.assertFalse(CryptoUtils.verifyInclusion(proof, digests[i], CryptoUtils.sha256Sum(root)));

            InclusionProof incompleteProof = new InclusionProof(proof.leaf, proof.width, null);
            Assert.assertFalse(CryptoUtils.verifyInclusion(incompleteProof, digests[i], root));

            Assert.assertFalse(CryptoUtils.verifyInclusion(null, digests[i], root));

        }

        try {
            tree.buildWith(null);
            Assert.fail("Tree buildWith(null) did not throw an exception.");
        } catch (IllegalArgumentException e) {
            // all good if it got here.
        } catch (MaxWidthExceededException | NoSuchAlgorithmException e) {
            Assert.fail("Tree buildWith(null) threw a wrong exception.");
        }

        try {
            tree.buildWith(new byte[maxWidth + 1][32]);
            Assert.fail("Tree buildWith(maxWidth+1) must throw an exception.");
        } catch (IllegalArgumentException | NoSuchAlgorithmException e) {
            Assert.fail("Tree buildWith(maxWidth+1) threw a wrong exception.");
        } catch (MaxWidthExceededException e) {
            // all good if it got here.
        }

        try {
            tree.inclusionProof(maxWidth);
            Assert.fail("Tree inclusionProof(maxWidth) did not throw an exception.");
        } catch (IllegalArgumentException e) {
            // all good if it got here.
        }

    }

}
