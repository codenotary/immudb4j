/*
Copyright 2019-2020 vChain, Inc.

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
package io.codenotary.immudb;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.codenotary.immudb.crypto.Root;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class SerializableRootHolder implements RootHolder {

  private Map<String,Root> rootMap = new HashMap<>();

  public void readFrom(InputStream is) {
    Gson gson = new Gson();
    Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);

    Type type = new TypeToken<HashMap<String, Root>>(){}.getType();
    rootMap = gson.fromJson(reader, type);
  }

  public void writeTo(OutputStream os) throws IOException {
    Gson gson = new Gson();
    os.write(gson.toJson(rootMap).getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public Root getRoot(String database) {
    return this.rootMap.get(database);
  }

  @Override
  public void SetRoot(Root root) {
    this.rootMap.put(root.getDatabase(),root);
  }

}
