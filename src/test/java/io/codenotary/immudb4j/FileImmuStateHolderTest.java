package io.codenotary.immudb4j;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileImmuStateHolderTest {

    @Test(testName = "Testing IllegalStateException case")
    public void t1() {

        File statesDir = null;
        try {
            String tempDir = System.getProperty("java.io.tmpdir");
            statesDir = new File(tempDir, "FileImmuStateHolderTest_states/");
            File currStateFile = new File(statesDir, "current_state");

            // Write some fake "state_..." into "current_state" file.
            Files.write(currStateFile.toPath(), "state_fake".getBytes(StandardCharsets.UTF_8));

            FileImmuStateHolder.newBuilder().withStatesFolder(statesDir.getAbsolutePath()).build();

            cleanupDir(statesDir);
            Assert.fail("stateHolder creation must have fail, but it didn't.");
        } catch (IOException e) {
            // If that would be the case, it's not the test's fault.
            System.out.println(">>> Got IO ex (expected): " + e.getMessage());
            if (e.getCause() != null) {
                System.out.println(">>> Got IO ex (expected) cause: " + e.getCause().getMessage());
            }
        } catch (IllegalStateException ignored) {
            // This should actually happen with that "fake" state entry.
        }
        cleanupDir(statesDir);
    }



    private static void cleanupDir(File dir) {
        if (dir == null) {
            return;
        }
        System.out.println(">>> Cleaning up dir: " + dir.getAbsolutePath());
        try {
            //noinspection ResultOfMethodCallIgnored
            Files.walk(dir.toPath()).sorted().map(Path::toFile).forEach(File::delete);
        } catch (IOException ignored) {
        }
    }

}
