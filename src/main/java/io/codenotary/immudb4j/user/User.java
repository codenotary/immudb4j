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
package io.codenotary.immudb4j.user;

import java.util.List;

public class User {

    private final String user;
    private final String createdAt;
    private final String createdBy;
    private final boolean active;
    private final List<Permission> permissions;

    public static UserBuilder getBuilder() {
        return new UserBuilder();
    }

    private User(UserBuilder userBuilder) {
        user = userBuilder.user;
        createdAt = userBuilder.createdAt;
        createdBy = userBuilder.createdBy;
        active = userBuilder.active;
        permissions = userBuilder.permissions;
    }

    public String getUser() {
        return user;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public boolean isActive() {
        return active;
    }

    public List<Permission> getPermissions() {
        return permissions;
    }

    @Override
    public String toString() {
        return String.format("User{user='%s', createdAt='%s', createdBy='%s', active=%s, permissions=%s}",
                user, createdAt, createdBy, active, permissions);
    }

    public static class UserBuilder {
        private String user;
        private String createdAt;
        private String createdBy;
        private boolean active;
        private List<Permission> permissions;

        public UserBuilder setUser(String user) {
            this.user = user;
            return this;
        }

        public UserBuilder setCreatedAt(String createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public UserBuilder setCreatedBy(String createdBy) {
            this.createdBy = createdBy;
            return this;
        }

        public UserBuilder setActive(boolean active) {
            this.active = active;
            return this;
        }

        public UserBuilder setPermissions(List<Permission> permissions) {
            this.permissions = permissions;
            return this;
        }

        public User build() {
            return new User(this);
        }
    }

}
