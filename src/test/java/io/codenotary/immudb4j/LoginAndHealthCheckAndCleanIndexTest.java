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

import io.grpc.StatusRuntimeException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LoginAndHealthCheckAndCleanIndexTest extends ImmuClientIntegrationTest {

    @Test(testName = "openSession (with default credentials), healthCheck, logout")
    public void t1() {
        immuClient.openSession("immudb", "immudb", "defaultdb");

        boolean isHealthy = immuClient.healthCheck();
        Assert.assertTrue(isHealthy);

        immuClient.flushIndex(10.0f, true);

        immuClient.closeSession();
    }

    @Test(testName = "openSession (with wrong credentials)", expectedExceptions = StatusRuntimeException.class)
    public void t2() {
        immuClient.openSession("immudb", "incorrect_password", "defaultdb");
    }

}
