/*
 * Copyright 2019-2020 vChain, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.codenotary.immudb4j.crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * immudb client using grpc.
 *
 * @author Jeronimo Irazabal
 * <p>Java port of proof verification algortihms implemented in github.com/codenotary/merkletree
 */
public class CryptoUtils {

    private static final byte LEAF_PREFIX = 0;
    private static final byte NODE_PREFIX = 1;
    private static final int DIGEST_LENGTH = 32;


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

}
