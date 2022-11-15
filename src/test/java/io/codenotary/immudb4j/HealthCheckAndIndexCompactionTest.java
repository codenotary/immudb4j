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

import java.rmi.UnexpectedException;

import org.testng.Assert;
import org.testng.annotations.Test;

public class HealthCheckAndIndexCompactionTest extends ImmuClientIntegrationTest {

    @Test(testName = "openSession (with default credentials), healthCheck, logout")
    public void t1() throws UnexpectedException {
        try {
            immuClient.openSession("defaultdb", "immudb", "immudb");
        } catch (Exception e) {
            throw new UnexpectedException(e.getMessage());
        }

        Assert.assertTrue(immuClient.healthCheck());

        immuClient.flushIndex(10.0f);

        immuClient.closeSession();
    }

    @Test(testName = "openSession (with wrong credentials)", expectedExceptions = StatusRuntimeException.class)
    public void t2() {
        immuClient.openSession("defaultdb", "immudb", "incorrect_password");
    }

    @Test(testName = "openSession (with wrong credentials)", expectedExceptions = StatusRuntimeException.class)
    public void t3() {
        immuClient.openSession("defaultdb");
    }

    @Test(testName = "openSession with session already open", expectedExceptions = IllegalStateException.class)
    public void t4() throws UnexpectedException {
        try {
            immuClient.openSession("defaultdb", "immudb", "immudb");
        } catch (Exception e) {
            throw new UnexpectedException(e.getMessage());
        }

        try {
            immuClient.openSession("defaultdb", "immudb", "immudb");
        } finally {
            immuClient.closeSession();
        }
    }

    @Test(testName = "openSession with no open session", expectedExceptions = IllegalStateException.class)
    public void t5() {
        immuClient.closeSession();
    }

}
