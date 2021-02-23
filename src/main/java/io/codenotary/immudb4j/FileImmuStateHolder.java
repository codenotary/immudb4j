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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileImmuStateHolder implements ImmuStateHolder {

    private final Path statesFolder;
    private final Path currentStateFile;
    private Path stateHolderFile;

    private final SerializableImmuStateHolder stateHolder;

    private FileImmuStateHolder(Builder builder) throws IOException, IllegalStateException {
        statesFolder = Paths.get(builder.getStatesFolder());

        if (Files.notExists(statesFolder)) {
            Files.createDirectory(statesFolder);
        }

        currentStateFile = statesFolder.resolve("current_state");

        if (Files.notExists(currentStateFile)) {
            Files.createFile(currentStateFile);
        }

        stateHolder = new SerializableImmuStateHolder();

        String lastRootFilename = new String(Files.readAllBytes(currentStateFile));

        if (!lastRootFilename.isEmpty()) {
            stateHolderFile = statesFolder.resolve(lastRootFilename);

            if (Files.notExists(stateHolderFile)) {
                throw new IllegalStateException("Inconsistent current state file");
            }

            stateHolder.readFrom(Files.newInputStream(stateHolderFile));
        }
    }

    @Override
    public synchronized ImmuState getState(String database) {
        return stateHolder.getState(database);
    }

    @Override
    public synchronized void setState(ImmuState state) throws IllegalStateException {
        ImmuState currentState = stateHolder.getState(state.database);

        if (currentState != null && currentState.txId >= state.txId) {
            return;
        }

        stateHolder.setState(state);

        Path newStateHolderFile = statesFolder.resolve("state_" + System.nanoTime());

        if (Files.exists(newStateHolderFile)) {
            throw new RuntimeException("Failed attempting to create fresh state file. Please retry.");
        }

        try {
            Files.createFile(newStateHolderFile);
            stateHolder.writeTo(Files.newOutputStream(newStateHolderFile));

            BufferedWriter bufferedWriter = Files.newBufferedWriter(currentStateFile, StandardOpenOption.TRUNCATE_EXISTING);
            bufferedWriter.write(newStateHolderFile.getFileName().toString());
            bufferedWriter.flush();
            bufferedWriter.close();

            if (stateHolderFile != null) {
                Files.delete(stateHolderFile);
            }

            stateHolderFile = newStateHolderFile;
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalStateException("Unexpected error " + e);
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String statesFolder;

        private Builder() {
            statesFolder = "states";
        }

        public FileImmuStateHolder build() throws IOException, IllegalStateException {
            return new FileImmuStateHolder(this);
        }

        public Builder setStatesFolder(String statesFolder) {
            this.statesFolder = statesFolder;
            return this;
        }

        public String getStatesFolder() {
            return this.statesFolder;
        }
    }

}
