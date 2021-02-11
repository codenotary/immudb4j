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

import io.codenotary.immudb4j.crypto.ImmutableState;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileImmutableStateHolder implements ImmutableStateHolder {

    private final Path statesFolder;
    private final Path currentStateFile;
    private Path stateHolderFile;

    private final SerializableImmutableStateHolder stateHolder;

    private FileImmutableStateHolder(Builder builder) throws IOException {
        statesFolder = Paths.get(builder.getStatesFolder());

        if (Files.notExists(statesFolder)) {
            Files.createDirectory(statesFolder);
        }

        currentStateFile = statesFolder.resolve("current_state");

        if (Files.notExists(currentStateFile)) {
            Files.createFile(currentStateFile);
        }

        stateHolder = new SerializableImmutableStateHolder();

        String lastRootFilename = new String(Files.readAllBytes(currentStateFile));

        if (!lastRootFilename.isEmpty()) {
            stateHolderFile = statesFolder.resolve(lastRootFilename);

            if (Files.notExists(stateHolderFile)) {
                throw new RuntimeException("Inconsistent current root file");
            }

            stateHolder.readFrom(Files.newInputStream(stateHolderFile));
        }
    }

    @Override
    public synchronized ImmutableState getState(String database) {
        return stateHolder.getState(database);
    }

    @Override
    public synchronized void setState(ImmutableState state) {
        ImmutableState currentState = stateHolder.getState(state.getDatabase());

        if (currentState != null && currentState.getTxId() >= state.getTxId()) {
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
            throw new RuntimeException("Unexpected error " + e);
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

        public FileImmutableStateHolder build() throws IOException {
            return new FileImmutableStateHolder(this);
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
