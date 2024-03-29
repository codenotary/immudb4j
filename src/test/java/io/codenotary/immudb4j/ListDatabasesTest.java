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

import org.testng.annotations.Test;

import java.util.List;

public class ListDatabasesTest extends ImmuClientIntegrationTest {

  @Test(testName = "databases without open session", expectedExceptions = IllegalStateException.class)
    public void t1() {
        immuClient.databases();
  }

  @Test(testName = "databases")
  public void t2() {
    immuClient.openSession("defaultdb", "immudb", "immudb");

    List<Database> databases = immuClient.databases();
    if (databases.size() > 0) {
        System.out.print(">>> The databases are");
        for (Database db : databases) {
            System.out.printf(" '%s'(loaded: %b)", db.getName(), db.isLoaded());
        }
    } else {
        System.out.print(">>> There are no databases.");
    }

    immuClient.closeSession();
  }

}
