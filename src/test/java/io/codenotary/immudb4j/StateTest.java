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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Objects;

public class StateTest extends ImmuClientIntegrationTest {

    @Test(testName = "state")
    public void t1() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        ImmuState state = immuClient.state();

        Assert.assertNotNull(state);
        // System.out.println(">>> t1 > state: " + state.toString());

        String stateStr = state.toString();
        Assert.assertTrue(stateStr.contains("ImmuState{"));
        Assert.assertTrue(stateStr.contains("txHash(base64)"));
        Assert.assertTrue(stateStr.contains("signature(base64)"));

        immuClient.logout();
    }

    @Test(testName = "currentState")
    public void t2() {

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        ImmuState currState = immuClient.currentState();

        Assert.assertNotNull(currState);
        // System.out.println(">>> t2 > currState: " + currState.toString());

        immuClient.logout();
    }

    @Test(testName = "currentState with server signature checking, but only on the client side")
    public void t3() {

        // Provisioning the client side with the public key file.
        String publicKeyResource = "test_public_key.pem";
        ClassLoader classLoader = getClass().getClassLoader();
        File publicKeyFile = new File(Objects.requireNonNull(classLoader.getResource(publicKeyResource)).getFile());

        // Recreating an client instance with the server signing key.
        try {
            immuClient = ImmuClient.newBuilder()
                    .withServerUrl("localhost")
                    .withServerPort(3322)
                    .withServerSigningKey(publicKeyFile.getAbsolutePath())
                    .build();
        } catch (Exception e) {
            // This is not a test failure, so just printing the issue and ending the test.
            System.err.println("StateTest > t3 > Ending the test since could not load the server signing key. Reason: "
                    + e.getMessage());
            return;
        }

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        try {
            ImmuState state = immuClient.currentState();
            Assert.fail("Did not fail as it should in this case when the signingKey is provisioned only on the client side");
        } catch (RuntimeException ignored) {
            // Expected this since in the current tests setup, immudb does not have that state signature feature active.
            // (this feature is active when starting it like: `immudb --signingKey test_private_key.pem`).
        }

        immuClient.logout();
    }

    @Test(testName = "currentState with server signature checking",
            description = "Testing `checkSignature` (indirectly, through `currentState`), " +
                    "the (state signing) feature being set up on both server and client side. " +
                    "This could remain a manual test, that's why it is disabled." +
                    "Of course, it must be `enabled = true`, if you want to run it from IDE or cli.",
            enabled = false)
    public void t4() {

        // Provisioning the client side with the public key file.
        String publicKeyResource = "test_public_key.pem";
        ClassLoader classLoader = getClass().getClassLoader();
        File publicKeyFile = new File(Objects.requireNonNull(classLoader.getResource(publicKeyResource)).getFile());

        // Recreating an client instance with the server signing key.
        try {
            immuClient = ImmuClient.newBuilder()
                    .withServerUrl("localhost")
                    .withServerPort(3322)
                    .withServerSigningKey(publicKeyFile.getAbsolutePath())
                    .build();
        } catch (Exception e) {
            // This is not a test failure, so just printing the issue and ending the test.
            System.err.println("StateTest > t4 > Ending the test since could not load the server signing key. Reason: "
                    + e.getMessage());
            return;
        }

        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        try {
            ImmuState state = immuClient.currentState();
            // In this case, it should be ok as long as the immudb server has been started accordingly
            // from `immudb` directory (on this repo root) using: `./immudb --signingKey test_private_key.pem`
            Assert.assertNotNull(state);
        } catch (RuntimeException e) {
            Assert.fail(e.getMessage(), e.getCause());
        }

        immuClient.logout();
    }

}
