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
package io.codenotary.immudb4j;

import org.testng.annotations.Test;


public class CountTest extends ImmuClientIntegrationTest {

    @Test(enabled = false, testName = "count")
    public void t1() {

        immuClient.login("immudb", "immudb");

        String prefix = "sga";
//        System.out.printf(">>> There are %d entries with keys having the prefix '%s'.\n",
//                immuClient.count(prefix), prefix);

        immuClient.logout();
    }

    @Test(enabled = false, testName = "countAll")
    public void t2() {

        immuClient.login("immudb", "immudb");

//        System.out.printf(">>> There are %d entries.\n", immuClient.countAll());

        immuClient.logout();
    }

}
