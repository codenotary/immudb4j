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

public class KVPage {
    private final KVList kvList;
    private final boolean more;

    private KVPage(KVPageBuilder builder) {
        kvList = builder.kvList;
        more = builder.more;
    }

    public static KVPageBuilder newBuilder() {
        return new KVPageBuilder();
    }

    public KVList getKvList() {
        return kvList;
    }

    public boolean isMore() {
        return more;
    }

    public static class KVPageBuilder {
        private KVList kvList;
        private boolean more;

        public KVPageBuilder() {
            this.kvList = KVList.newBuilder().build();
            this.more = false;
        }

        public KVPageBuilder setKvList(KVList kvList) {
            this.kvList = kvList;
            return this;
        }

        public KVPageBuilder setMore(boolean more) {
            this.more = more;
            return this;
        }

        public KVPage build() {
            return new KVPage(this);
        }
    }
}
