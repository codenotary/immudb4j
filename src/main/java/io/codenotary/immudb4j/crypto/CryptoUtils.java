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
import io.codenotary.immudb4j.KV;
import io.codenotary.immudb4j.KVPair;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;


public class CryptoUtils {

    public static final int SHA256_SIZE = 32;

    private static final byte SET_KEY_PREFIX = 0;
    private static final byte PLAIN_VALUE_PREFIX = 0;
    private static final byte REFERENCE_VALUE_PREFIX = 1;

    /**
     * This method returns a SHA256 digest of the provided data.
     */
    public static byte[] digest(byte[] data) {
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

    /** Copy the provided `digest` array into a byte[32] array. */
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
        byte[] refVal = new byte[1+8+key.length];
        refVal[0] = REFERENCE_VALUE_PREFIX;

        // The Java version of Go's: binary.BigEndian.PutUint64(refVal[1:], atTx)
        byte[] bs = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(atTx).array();
        System.arraycopy(bs, 0, refVal, 1, bs.length);

        System.arraycopy(key, 0, refVal, 1+8, key.length);
        return refVal;
    }

}
