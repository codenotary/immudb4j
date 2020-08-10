/*
Copyright 2019-2020 vChain, Inc.

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
package io.codenotary.immudb.crypto;

import com.google.protobuf.ByteString;
import io.codenotary.immudb.ImmudbProto;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * immudb client using grpc.
 *
 * @author Jeronimo Irazabal
 *     <p>Java port of proof verification algortihms implemented in github.com/codenotary/merkletree
 */
public class CryptoUtils {

  private static final byte LEAF_PREFIX = 0;
  private static final byte NODE_PREFIX = 1;

  private static MessageDigest sha256;

  static {
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  public static void verify(ImmudbProto.Proof proof, ImmudbProto.Item item, Root root)
      throws VerificationException {
    byte[] h = entryDigest(item);

    if (!Arrays.equals(proof.getLeaf().toByteArray(), h)) {
      throw new VerificationException("Proof does not verify!");
    }

    verifyInclusion(proof);

    if (root != null) {
      verifyConsistency(proof, root);
    }
  }

  public static void verifyInclusion(ImmudbProto.Proof proof) throws VerificationException {
    long at = proof.getAt();
    long i = proof.getIndex();

    if (i > at || (at > 0 && proof.getInclusionPathCount() == 0)) {
      throw new VerificationException("Inclusion proof does not verify!");
    }

    byte[] h = proof.getLeaf().toByteArray();

    for (ByteString v : proof.getInclusionPathList()) {
      ByteBuffer buffer = ByteBuffer.allocate(sha256.getDigestLength() * 2 + 1);

      buffer.put(NODE_PREFIX);

      if (i % 2 == 0 && i != at) {
        buffer.put(h);
        buffer.put(v.toByteArray());
      } else {
        buffer.put(v.toByteArray());
        buffer.put(h);
      }

      h = sha256.digest(buffer.array());
      i /= 2;
      at /= 2;
    }

    if (at != i || !Arrays.equals(h, proof.getRoot().toByteArray())) {
      throw new VerificationException("Inclusion proof does not verify!");
    }
  }

  public static void verifyConsistency(ImmudbProto.Proof proof, Root root)
      throws VerificationException {
    long second = proof.getAt();
    long first = root.getIndex();
    byte[] secondHash = proof.getRoot().toByteArray();
    byte[] firstHash = root.getDigest();

    int l = proof.getConsistencyPathCount();

    if (first == second && Arrays.equals(firstHash, secondHash) && l == 0) {
      return;
    }

    if (!(first < second) || l == 0) {
      throw new VerificationException("Consistency proof does not verify!");
    }

    List<byte[]> pp = new LinkedList<>();

    if (isPowerOfTwo(first + 1)) {
      pp.add(firstHash);
    }

    for (ByteString n : proof.getConsistencyPathList()) {
      pp.add(n.toByteArray());
    }

    long fn = first;
    long sn = second;

    while (fn % 2 == 1) {
      fn >>= 1;
      sn >>= 1;
    }

    byte[] fr = pp.get(0);
    byte[] sr = pp.get(0);

    for (int step = 0; step < pp.size(); step++) {
      if (step == 0) {
        continue;
      }

      if (sn == 0) {
        throw new VerificationException("Consistency proof does not verify!");
      }

      ByteBuffer bufferSr = ByteBuffer.allocate(sha256.getDigestLength() * 2 + 1);
      bufferSr.order(ByteOrder.BIG_ENDIAN);
      bufferSr.put(NODE_PREFIX);

      byte[] c = pp.get(step);

      if (fn % 2 == 1 || fn == sn) {
        ByteBuffer bufferFn = ByteBuffer.allocate(sha256.getDigestLength() * 2 + 1);
        bufferFn.order(ByteOrder.BIG_ENDIAN);
        bufferFn.put(NODE_PREFIX);
        bufferFn.put(c);
        bufferFn.put(fr);
        fr = sha256.digest(bufferFn.array());

        bufferSr.put(c);
        bufferSr.put(sr);
        sr = sha256.digest(bufferSr.array());

        while (fn % 2 == 0 && fn != 0) {
          fn >>= 1;
          sn >>= 1;
        }
      } else {
        bufferSr.put(sr);
        bufferSr.put(c);
        sr = sha256.digest(bufferSr.array());
      }

      fn >>= 1;
      sn >>= 1;
    }

    if (!Arrays.equals(fr, firstHash) || !Arrays.equals(sr, secondHash) || sn != 0) {
      throw new VerificationException("Consistency proof does not verify!");
    }
  }

  public static boolean isPowerOfTwo(long n) {
    return (n != 0) && ((n & (n - 1)) == 0);
  }

  public static byte[] entryDigest(ImmudbProto.Item item) {
    int kl = item.getKey().size();
    int vl = item.getValue().size();

    ByteBuffer buffer = ByteBuffer.allocate(1 + 8 + 8 + kl + vl);
    buffer.order(ByteOrder.BIG_ENDIAN);
    buffer.put(LEAF_PREFIX);
    buffer.putLong(item.getIndex());
    buffer.putLong(kl);
    buffer.put(item.getKey().toByteArray());
    buffer.put(item.getValue().toByteArray());

    return sha256.digest(buffer.array());
  }
}
