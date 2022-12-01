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

import io.codenotary.immudb.ImmudbProto;

public class Database {

    private String name;
    private boolean loaded;

    private Database() {}

    public static Database valueOf(ImmudbProto.DatabaseWithSettings pdb) {
        final Database db = new Database();

        db.name = pdb.getName();
        db.loaded = pdb.getLoaded();

        return db;
    }

    public String getName() {
        return name;
    }

    public boolean isLoaded() {
        return loaded;
    }
}
