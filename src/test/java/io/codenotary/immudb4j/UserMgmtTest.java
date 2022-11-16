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

import io.codenotary.immudb4j.user.Permission;
import io.codenotary.immudb4j.user.User;
import io.grpc.StatusRuntimeException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

public class UserMgmtTest extends ImmuClientIntegrationTest {

    @Test(testName = "createUser, listUsers", priority = 100)
    public void t1() {
        String database = "defaultdb";
        String username = "testCreateUser";
        String password = "testTest123!";
        Permission permission = Permission.PERMISSION_RW;

        immuClient.openSession(database, "immudb", "immudb");

        // Should not contain testCreateUser. Skipping it as not valid for the current unit tests setup
        // (where a clean immudb server is started for each Test class).
        // immuClient.listUsers().forEach(user -> Assert.assertNotEquals(user.getUser(), username));

        try {
            immuClient.createUser(username, password, permission, database);
        } catch (StatusRuntimeException e) {
            // The user could already exist, ignoring this.
            System.out.println(">>> UserMgmtTest > t1 > createUser exception: " + e.getMessage());
        }

        // Should contain testCreateUser.
        System.out.println(">>> listUsers:");
        List<User> users = immuClient.listUsers();
        users.forEach(user -> System.out.println("\t- " + user));

        // TODO: Temporary commented since currently there's a bug on immudb's side.
        //       The next release will include the fix of 'listUsers'. This commit includes the fix:
        //       https://github.com/codenotary/immudb/commit/2d7e4c2fd901389020f42a2e7f4458bc073a8641
//        Optional<User> createdUser = users.stream().filter(u -> u.getUser().equals(username)).findFirst();
//        Assert.assertTrue(createdUser.isPresent(), "Newly created user is not present in the listing.");
//
//        User user = createdUser.get();
//        Assert.assertEquals(user.getUser(), username);
//        Assert.assertTrue(user.isActive());
//        Assert.assertNotEquals(user.getCreatedAt(), "");
//        Assert.assertEquals(user.getCreatedBy(), "immudb");
//        Assert.assertEquals(user.getPermissions().get(0), permission);

        immuClient.closeSession();
    }

    @Test(testName = "createUser, changePassword", priority = 101)
    public void t2() {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        try {
            immuClient.createUser("testUser", "testTest123!", Permission.PERMISSION_ADMIN, "defaultdb");
        } catch (StatusRuntimeException e) {
            // The user could already exist, ignoring this.
            System.out.println(">>> UserMgmtTest > t2 > createUser exception: " + e.getMessage());
        }

        immuClient.changePassword("testUser", "testTest123!", "newTestTest123!");

        immuClient.closeSession();

        // This must fail.
        try {
            immuClient.openSession("testUser", "testTest123", "defaultdb");
            Assert.fail("should fail with wrong (old) password must fail.");
        } catch (StatusRuntimeException e) {
            // Login failed, everything's fine.
        }

        immuClient.openSession("defaultdb", "testUser", "newTestTest123!");

        immuClient.closeSession();

        // Some basic test to temporary (until t1 test above can be used) increase the code coverage.
        User myUser = new User.UserBuilder().setUser("myUsername").setCreatedAt("someTimestamp").setCreatedBy("me")
                .setActive(true).setPermissions(Collections.singletonList(Permission.PERMISSION_R))
                .build();
        Assert.assertEquals(myUser.getUser(), "myUsername", "Usernames are different");
        Assert.assertEquals(myUser.getCreatedAt(), "someTimestamp", "CreatedAt values are different");
        Assert.assertEquals(myUser.getCreatedBy(), "me", "CreatedBy values are different");
        Assert.assertTrue(myUser.isActive(), "User is not active, as expected");
        Assert.assertEquals(myUser.getPermissions(), Collections.singletonList(Permission.PERMISSION_R), "Permissions are different");
    }

}
