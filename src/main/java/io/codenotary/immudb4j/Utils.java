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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.List;

public class Utils {

    public static int countBits(int number) {

        if (number == 0) {
            return 0;
        }
        // log function in base 2 & take only integer part.
        return (int) (Math.log(number) / Math.log(2) + 1);
    }

    private final static char[] HEX = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D',
            'E', 'F'};

    /**
     * Convert the bytes to a base 16 string.
     */
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

    /**
     * Convert the list of SHA256 (32-length) bytes to a primitive byte[][].
     */
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


    public static void putUint32(int value, byte[] dest, int destPos) {
        // Considering gRPC's generated code that maps Go's uint32 and int32 to Java's int,
        // this is basically the Java version of this Go code:
        // binary.BigEndian.PutUint32(target[targetIdx:], value)
        byte[] valueBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
        System.arraycopy(valueBytes, 0, dest, destPos, valueBytes.length);
    }

    public static void putUint64(long value, byte[] dest) {
        putUint64(value, dest, 0);
    }

    public static void putUint64(long value, byte[] dest, int destPos) {
        // Considering gRPC's generated code that maps Go's uint64 and int64 to Java's long,
        // this is basically the Java version of this Go code:
        // binary.BigEndian.PutUint64(target[targetIdx:], value)
        byte[] valueBytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
        System.arraycopy(valueBytes, 0, dest, destPos, valueBytes.length);
    }

    public static String toStringAsBase64Values(byte[][] data) {
        if (data == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < data.length; i++) {
            sb.append(Base64.getEncoder().encodeToString(data[i]));
            if (i < data.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public static String asBase64(byte[] data) {
        return Base64.getEncoder().encodeToString(data);
    }

    public static void copy(byte[] src, byte[] dest) {
        System.arraycopy(src, 0, dest, 0, src.length);
    }

    public static void copy(byte[] src, byte[] dest, int destPos) {
        System.arraycopy(src, 0, dest, destPos, src.length);
    }

    public static void copy(byte[] src, int srcPos, int length, byte[] dest) {
        System.arraycopy(src, srcPos, dest, 0, length);
    }

    public static void copy(byte[] src, int srcPos, int length, byte[] dest, int destPos) {
        System.arraycopy(src, srcPos, dest, destPos, length);
    }

}
