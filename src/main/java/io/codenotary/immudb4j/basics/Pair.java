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
 * A pair contains two generic elements.
 */
public final class Pair<A, B> {

    public final A a;
    public final B b;

    public Pair(A a, B b) {
        this.a = a;
        this.b = b;
    }

    public String toString() {
        return "(" + a + "," + b + ")";
    }

    private static boolean equals(Object x, Object y) {
        return (x == null && y == null) || (x != null && x.equals(y));
    }

    public boolean equals(Object other) {
        return other instanceof Pair<?, ?> &&
                equals(a, ((Pair<?, ?>) other).a) &&
                equals(b, ((Pair<?, ?>) other).b);
    }

    public int hashCode() {
        if (a == null) return (b == null) ? 0 : b.hashCode() + 1;
        else if (b == null) return a.hashCode() + 2;
        else return a.hashCode() * 17 + b.hashCode();
    }

    public static <A, B> Pair<A, B> of(A a, B b) {
        return new Pair<A, B>(a, b);
    }

}
