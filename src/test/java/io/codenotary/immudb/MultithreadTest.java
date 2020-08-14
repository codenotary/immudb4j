/*
Copyright 2019-2020 vChain, Inc.

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
package io.codenotary.immudb;

import io.codenotary.immudb.crypto.VerificationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MultithreadTest extends ImmuClientIntegrationTest {

  @Test
  public void testMultithredRW() throws InterruptedException {
    immuClient.login("immudb", "");

    AtomicInteger suceeded = new AtomicInteger(0);

    Runnable runnable = () -> {
      Random rnd = new Random();

      long threadId = Thread.currentThread().getId();

      for (int i = 0; i < 100; i++) {
        byte[] b = new byte[10];
        rnd.nextBytes(b);
        b[0] = 1;

        try {
          immuClient.safeSet("k" + "_" + threadId + "_" + i, b);
        } catch (VerificationException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        try {
          Thread.sleep(rnd.nextInt(100));
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      suceeded.incrementAndGet();
    };

    //TODO: investigate concurrency issues in immudb and increase thread count

    ExecutorService es = Executors.newCachedThreadPool();
    int threads = 1;
    for(int i=0;i<threads;i++) {
      es.execute(runnable);
    }
    es.shutdown();
    es.awaitTermination(10, TimeUnit.SECONDS);

    Assert.assertEquals(threads, suceeded.get());
  }
}
