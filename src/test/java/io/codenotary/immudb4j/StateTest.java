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

import org.testng.Assert;
import org.testng.annotations.Test;

public class StateTest extends ImmuClientIntegrationTest {

    @Test(testName = "state")
    public void t1() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        ImmuState state = immuClient.state();

        Assert.assertNotNull(state);
        System.out.println(">>> t1 > state: " + state.toString());

        immuClient.logout();
    }

    @Test(testName = "currentState")
    public void t2() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        ImmuState currState = immuClient.currentState();

        Assert.assertNotNull(currState);
        System.out.println(">>> t2 > currState: " + currState.toString());

        immuClient.logout();
    }

}
