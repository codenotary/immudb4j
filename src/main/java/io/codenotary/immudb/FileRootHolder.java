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

import io.codenotary.immudb.crypto.Root;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileRootHolder implements RootHolder {

  private Path rootsFolder;
  private Path currentRootFile;
  private Path rootHolderFile;

  private SerializableRootHolder rootHolder;

  private FileRootHolder(FileRootHolderBuilder builder) throws IOException {
    rootsFolder = Paths.get(builder.getRootsFolder());

    if (Files.notExists(rootsFolder)) {
      Files.createDirectory(rootsFolder);
    }

    currentRootFile = rootsFolder.resolve("current_root");

    if (Files.notExists(currentRootFile)) {
      Files.createFile(currentRootFile);
    }

    rootHolder = new SerializableRootHolder();

    String lastRootFilename = new String(Files.readAllBytes(currentRootFile));

    if (!lastRootFilename.isEmpty()) {
      rootHolderFile = rootsFolder.resolve(lastRootFilename);

      if (Files.notExists(rootHolderFile)) {
        throw new RuntimeException("Inconsistent current root file");
      }

      rootHolder.readFrom(Files.newInputStream(rootHolderFile));
    }
  }

  @Override
  public Root getRoot(String database) {
    return rootHolder.getRoot(database);
  }

  @Override
  public synchronized void SetRoot(Root root) {
    Root currentRoot = rootHolder.getRoot(root.getDatabase());

    if (currentRoot != null && currentRoot.getIndex() >= root.getIndex()) {
      return;
    }

    rootHolder.SetRoot(root);

    Path newRootHolderFile = rootsFolder.resolve("root_" + System.currentTimeMillis());

    if (Files.exists(newRootHolderFile)) {
      throw new RuntimeException("Attempt to create fresh root file failed. Please retry");
    }

    try {
      Files.createFile(newRootHolderFile);
      rootHolder.writeTo(Files.newOutputStream(newRootHolderFile));

      BufferedWriter bufferedWriter = Files.newBufferedWriter(currentRootFile, StandardOpenOption.TRUNCATE_EXISTING);
      bufferedWriter.write(newRootHolderFile.getFileName().toString());
      bufferedWriter.flush();
      bufferedWriter.close();

      if (rootHolderFile != null) {
        Files.delete(rootHolderFile);
      }

      rootHolderFile = newRootHolderFile;
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Unexpected error " + e);
    }
  }

  public static FileRootHolderBuilder newBuilder() {
    return new FileRootHolderBuilder();
  }

  public static class FileRootHolderBuilder{

    private String rootsFolder;

    private FileRootHolderBuilder() {
      rootsFolder = "immudb_roots";
    }

    public FileRootHolder build() throws IOException {
      return new FileRootHolder(this);
    }

    public FileRootHolderBuilder setCurrentRootsFolder(String rootsFolder) {
      this.rootsFolder = rootsFolder;
      return this;
    }

    public String getRootsFolder() {
      return this.rootsFolder;
    }
  }

}
