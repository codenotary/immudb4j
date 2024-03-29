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

import io.codenotary.immudb4j.basics.LatchHolder;

// Note: This test is more for the sake of code coverage, as you may see.

import io.codenotary.immudb4j.basics.Pair;
import io.codenotary.immudb4j.basics.Triple;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BasicsTest {

    @Test(testName = "Pair and Triple tests")
    public void t1() {
        String pa = "Snoop";
        String pb = "Dog";
        Pair<?, ?> pair = Pair.of(pa, pb);

        Assert.assertNotNull(pair);
        Assert.assertEquals(pair.a, pa);
        Assert.assertEquals(pair.b, pb);
        Assert.assertEquals(pair, Pair.of(pa, pb));

        Assert.assertTrue(pair.toString().contains("("));
        System.out.println("BasicsTest > t1 pair hashCode=" + pair.hashCode());

        String ta = "Bow";
        String tb = "Wow";
        String tc = "Wow";

        Triple<?,?,?> triple = Triple.of(ta, tb, tc);

        Assert.assertNotNull(triple);
        Assert.assertEquals(triple.a, ta);
        Assert.assertEquals(triple.b, tb);
        Assert.assertEquals(triple.c, tc);
        Assert.assertEquals(triple, Triple.of(ta, tb, tc));

        Assert.assertTrue(triple.toString().contains("("));
        System.out.println("BasicsTest > t1 triple hashCode=" + triple.hashCode());

        Assert.assertNotEquals(triple, null);
        Assert.assertEquals(triple, triple);
        Assert.assertNotEquals(triple, Triple.of("aDifferent", "", ""));
    }

    @Test(testName = "LatchHolder test")
    public void t2() throws InterruptedException {
        final LatchHolder<Boolean> latchHolder = new LatchHolder<>();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }

                latchHolder.doneWith(true);
            }
        }).run();

        Assert.assertTrue(latchHolder.awaitValue());
    }
}
