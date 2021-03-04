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

import io.codenotary.immudb4j.crypto.CryptoUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.security.PublicKey;
import java.util.Objects;


public class CryptoUtilsTest {

    @Test(testName = "getDERPublicKey loading a resource/test public key file")
    public void t1() {

        String publicKeyResource = "test_public_key.pem";
        ClassLoader classLoader = getClass().getClassLoader();
        File publicKeyFile = new File(Objects.requireNonNull(classLoader.getResource(publicKeyResource)).getFile());

        try {
            PublicKey publicKey = CryptoUtils.getDERPublicKey(publicKeyFile.getAbsolutePath());
            Assert.assertNotNull(publicKey);
            // System.out.println(publicKey);
        } catch (Exception e) {
            Assert.fail("Could not load the public key file.", e);
        }

    }

}
