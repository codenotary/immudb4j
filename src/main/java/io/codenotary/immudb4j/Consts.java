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
package io.codenotary.immudb4j;

public class Consts {

    // __________ prefixes __________

    /**
     * HTree's byte prefix of a leaf's digest.
     */
    public static final byte LEAF_PREFIX = 0;

    /**
     * HTree's byte prefix of a node (non-leaf)'s digest.
     */
    public static final byte NODE_PREFIX = 1;

    public static final byte SET_KEY_PREFIX = 0;
    public static final byte SORTED_SET_KEY_PREFIX = 1;

    public static final byte PLAIN_VALUE_PREFIX = 0;
    public static final byte REFERENCE_VALUE_PREFIX = 1;

    // __________ sizes & lengths __________

    /**
     * The size (in bytes) of the data type used for storing the length of a SHA256 checksum.
     */
    public static final int SHA256_SIZE = 32;

    /**
     * The size (in bytes) of the data type used for storing the transaction identifier.
     */
    public static final int TX_ID_SIZE = 8;

    /**
     * The size (in bytes) of the data type used for storing the transaction timestamp.
     */
    public static final int TS_SIZE = 8;

    /**
     * The size (in bytes) of the data type used for storing the sorted set length.
     */
    public static final int SET_LEN_LEN = 8;

    /**
     * The size (in bytes) of the data type used for storing the score length.
     */
    public static final int SCORE_LEN = 8;

    /**
     * The size (in bytes) of the data type used for storing the length of a key length.
     */
    public static final int KEY_LEN_LEN = 8;

    private Consts(){}

}
