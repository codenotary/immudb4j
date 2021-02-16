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

import io.codenotary.immudb.ImmudbProto;

public class Reference {

    public final long tx;
    public final byte[] key;
    public final long atTx;

    public Reference(long tx, byte[] key, long atTx) {
        this.tx = tx;
        this.key = key;
        this.atTx = atTx;
    }

    public static Reference valueOf(ImmudbProto.Reference ref) {
        return new Reference(ref.getTx(), ref.getKey().toByteArray(), ref.getAtTx());
    }

}
