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

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.codenotary.immudb4j.exceptions.VerificationException;

public class MultithreadTest extends ImmuClientIntegrationTest {

  @Test
  public void testMultithredWithoutKeyOverlap() throws InterruptedException, VerificationException {
    immuClient.login("immudb", "immudb");
    immuClient.useDatabase("defaultdb");

    final int threadCount = 10;
    final int keyCount = 1000;

    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger succeeded = new AtomicInteger(0);

    Function<String, Runnable> workerFactory = (uuid) -> (Runnable) () -> {
      Random rnd = new Random();

      for (int i = 0; i < keyCount; i++) {
        byte[] b = new byte[10];
        rnd.nextBytes(b);

        try {
          immuClient.safeSet(uuid + "k" + i, b);
        } catch (VerificationException e) {
          latch.countDown();
          throw new RuntimeException(e);
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
        immuClient.safeGet("t" + i + "k" + i);
      }
    }

  }

  @Test
  public void testMultithredWithKeyOverlap() throws InterruptedException, VerificationException {
    immuClient.login("immudb", "immudb");
    immuClient.useDatabase("defaultdb");

    final int threadCount = 10;
    final int keyCount = 1000;

    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger succeeded = new AtomicInteger(0);

    Runnable runnable = () -> {
      Random rnd = new Random();

      for (int i = 0; i < keyCount; i++) {
        byte[] b = new byte[10];
        rnd.nextBytes(b);

        try {
          immuClient.safeSet("k" + i, b);
        } catch (VerificationException e) {
          latch.countDown();
          throw new RuntimeException(e);
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
        immuClient.safeGet("k" + i);
      }
    }

  }
}
