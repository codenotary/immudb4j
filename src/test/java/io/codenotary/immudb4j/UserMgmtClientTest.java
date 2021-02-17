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

import io.codenotary.immudb4j.user.Permission;
import io.codenotary.immudb4j.user.User;
import io.grpc.StatusRuntimeException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

public class UserMgmtClientTest extends ImmuClientIntegrationTest {

    @Test(priority = 100)
    public void testCreateAndListUser() {
        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");

        // Should not contain testUser
        List<User> users = immuClient.listUsers();
        users.forEach(user -> Assert.assertNotEquals(user.getUser(), "testCreateUser"));

        immuClient.createUser("testCreateUser", "testTest123!", Permission.PERMISSION_ADMIN, "defaultdb");

        // Should contain testUser
        users = immuClient.listUsers();
        Optional<User> createdUser = users.stream().filter(u -> u.getUser().equals("testCreateUser")).findFirst();
        Assert.assertTrue(createdUser.isPresent());

        User user = createdUser.get();
        Assert.assertNotEquals(user.getCreatedAt(), "");
        Assert.assertEquals(user.getCreatedBy(), "immudb");
        Assert.assertEquals(user.getPermissions().get(0), Permission.PERMISSION_ADMIN);

        immuClient.logout();
    }

    @Test(priority = 101)
    public void testChangePassword() {
        immuClient.login("immudb", "immudb");
        immuClient.useDatabase("defaultdb");
        try {
            immuClient.createUser("testUser", "testTest123!", Permission.PERMISSION_ADMIN, "defaultdb");
        } catch (StatusRuntimeException e) {
            // The user could already exist, ignoring this
        }

        immuClient.changePassword("testUser", "testTest123!", "newTestTest123!");
        immuClient.logout();

        // This should fail
        try {
            immuClient.login("testUser", "testTest123!");
            Assert.fail("Login should have failed");
        } catch (StatusRuntimeException e) {
            // Login failed, everything's fine
        }

        immuClient.login("testUser", "newTestTest123!");
        immuClient.logout();
    }
}
