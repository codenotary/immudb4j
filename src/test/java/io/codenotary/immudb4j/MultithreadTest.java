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

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.codenotary.immudb4j.exceptions.VerificationException;

public class MultithreadTest extends ImmuClientIntegrationTest {

    @Test(testName = "Multithread without key overlap")
    public void t1() throws InterruptedException, VerificationException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        final int threadCount = 5;
        final int keyCount = 10;

        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger succeeded = new AtomicInteger(0);

        Function<String, Runnable> workerFactory = (uuid) -> (Runnable) () -> {
            Random rnd = new Random();

            for (int i = 0; i < keyCount; i++) {
                byte[] b = new byte[10];
                rnd.nextBytes(b);

                try {
                    immuClient.verifiedSet(uuid + "k" + i, b);
                } catch (Exception e) {
                    latch.countDown();
                    throw new RuntimeException(e);
                }
            }
            succeeded.incrementAndGet();
            latch.countDown();
        };

        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(workerFactory.apply("t" + i));
            t.start();
        }

        latch.await();

        Assert.assertEquals(succeeded.get(), threadCount);

        for (int i = 0; i < threadCount; i++) {
            for (int k = 0; k < keyCount; k++) {
                immuClient.verifiedGet("t" + i + "k" + i);
            }
        }

        immuClient.closeSession();
    }

    @Test(testName = "Multithread with key overlap")
    public void t2() throws InterruptedException, VerificationException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        final int threadCount = 5;
        final int keyCount = 10;

        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger succeeded = new AtomicInteger(0);

        Runnable runnable = () -> {
            Random rnd = new Random();

            for (int i = 0; i < keyCount; i++) {
                byte[] b = new byte[10];
                rnd.nextBytes(b);

                try {
                    immuClient.verifiedSet("k" + i, b);
                } catch (Exception e) {
                    latch.countDown();
                    throw new RuntimeException(e);
                }
            }

            succeeded.incrementAndGet();
            latch.countDown();
        };

        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(runnable);
            t.start();
        }

        latch.await();

        Assert.assertEquals(succeeded.get(), threadCount);

        for (int i = 0; i < threadCount; i++) {
            for (int k = 0; k < keyCount; k++) {
                immuClient.verifiedGet("k" + i);
            }
        }

        immuClient.closeSession();
    }

}
