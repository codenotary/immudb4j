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

import com.google.protobuf.ByteString;

import java.util.List;

public class Utils {

    public static int countBits(int number) {

        if (number == 0) {
            return 0;
        }
        // log function in base 2 & take only integer part.
        return (int) (Math.log(number) / Math.log(2) + 1);
    }

    public static boolean haveSameEntries(byte[] a, byte[] b) {

        if (a == null || b == null || a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    private final static char[] HEX = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
            'E', 'F' };

    /** Convert the bytes to a base 16 string. */
    public static String convertBase16(byte[] byteArray) {

        StringBuilder hexBuffer = new StringBuilder(byteArray.length * 2);
        for (byte b : byteArray)
            for (int j = 1; j >= 0; j--)
                hexBuffer.append(HEX[(b >> (j * 4)) & 0xF]);
        return hexBuffer.toString();
    }

    public static String toString(byte[] data) {

        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < data.length; i++) {
            sb.append(data[i]);
            if (i < data.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /** Convert the list of SHA256 (32-length) bytes to a primitive byte[][]. */
    public static byte[][] convertSha256ListToBytesArray(List<ByteString> data) {

        if (data == null) {
            return null;
        }
        int size = data.size();
        byte[][] result = new byte[size][32];
        for (int i = 0; i < size; i++) {
            byte[] item = data.get(i).toByteArray();
            System.arraycopy(item, 0, result[i], 0, item.length);
        }
        return result;
    }

}
