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

import io.grpc.StatusRuntimeException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LoginAndHealthCheckAndCleanIndexTest extends ImmuClientIntegrationTest {

    @Test(testName = "login (with default credentials), healthCheck, logout")
    public void t1() {

        immuClient.login("immudb", "immudb");

        boolean isHealthy = immuClient.healthCheck();
        Assert.assertTrue(isHealthy);

        immuClient.compactIndex();

        immuClient.logout();
    }

    @Test(testName = "login (with wrong credentials)", expectedExceptions = StatusRuntimeException.class)
    public void t2() {

        immuClient.login("immudb", "incorrect_password");
    }

}
