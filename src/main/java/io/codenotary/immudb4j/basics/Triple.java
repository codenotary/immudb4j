/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
package io.codenotary.immudb4j.basics;

/**
 * A triple contains three generic elements.
 */
public final class Triple<A, B, C> {

    public final A a;
    public final B b;
    public final C c;

    public Triple(A a, B b, C c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof Triple<?, ?, ?>) {
            final Triple<?, ?, ?> other = (Triple<?, ?, ?>) obj;
            return equals(a, other.a) && equals(b, other.b) && equals(c, other.c);
        }
        return false;
    }


    private boolean equals(final Object object1, final Object object2) {
        return !(object1 == null || object2 == null) &&
                (object1 == object2 || object1.equals(object2));
    }

    @Override
    public int hashCode() {
        return (a == null ? 0 : a.hashCode()) ^
                (b == null ? 0 : b.hashCode()) ^
                (c == null ? 0 : c.hashCode());
    }

    @Override
    public String toString() {
        return "(" + a + ',' + b + ',' + c + ')';
    }

    public static <A, B, C> Triple<A, B, C> of(A a, B b, C c) {
        return new Triple<A, B, C>(a, b, c);
    }

}
