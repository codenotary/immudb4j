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

import java.util.HashMap;
import java.util.Map;

public enum Permission {
    PERMISSION_SYS_ADMIN(255),
    PERMISSION_ADMIN(254),
    PERMISSION_NONE(0),
    PERMISSION_R(1),
    PERMISSION_RW(2);

    private static final Map<Integer, Permission> BY_PERMISSION_CODE = new HashMap<>();

    static {
        for (Permission p : values()) {
            BY_PERMISSION_CODE.put(p.permissionCode, p);
        }
    }

    public final int permissionCode;

    Permission(int permissionCode) {
        this.permissionCode = permissionCode;
    }

    public static Permission valueOfPermissionCode(int permissionCode) {
        return BY_PERMISSION_CODE.get(permissionCode);
    }

}
