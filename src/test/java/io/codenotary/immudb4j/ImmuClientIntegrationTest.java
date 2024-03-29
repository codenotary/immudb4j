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

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public abstract class ImmuClientIntegrationTest {

  protected static ImmuClient immuClient;
  protected static File statesDir;

  @BeforeClass
  public static void beforeClass() throws IOException {
    statesDir = Files.createTempDirectory("immudb_states").toFile();
    statesDir.deleteOnExit();

    FileImmuStateHolder stateHolder = FileImmuStateHolder.newBuilder()
            .withStatesFolder(statesDir.getAbsolutePath())
            .build();

    immuClient = ImmuClient.newBuilder()
            .withStateHolder(stateHolder)
            .withServerUrl("localhost")
            .withServerPort(3322)
            .build();
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
      immuClient.shutdown();
  }

}
