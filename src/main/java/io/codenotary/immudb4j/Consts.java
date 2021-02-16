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

public class Consts {

    public static final int SHA256_SIZE = 32;

    /**
     * HTree's byte prefix of a leaf's digest.
     */
    public static final byte LEAF_PREFIX = 0;

    /**
     * HTree's byte prefix of a node (non-leaf)'s digest.
     */
    public static final byte NODE_PREFIX = 1;

    /**
     * The size (in bytes) of the transaction ID data type.
     */
    public static final int TX_ID_SIZE = 8;

}
