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

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Utils {

    public static ByteString toByteString(String str) {
        if (str == null) {
            return com.google.protobuf.ByteString.EMPTY;
        }

        return ByteString.copyFrom(str, StandardCharsets.UTF_8);
    }

    public static ByteString toByteString(byte[] b) {
        if (b == null) {
            return com.google.protobuf.ByteString.EMPTY;
        }

        return ByteString.copyFrom(b);
    }

    public static byte[] toByteArray(String str) {
        if (str == null) {
            return null;
        }

        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] wrapWithPrefix(byte[] b, byte prefix) {
        if (b == null) {
            return null;
        }
        byte[] wb = new byte[b.length + 1];
        wb[0] = prefix;
        System.arraycopy(b, 0, wb, 1, b.length);
        return wb;
    }

    public static byte[] wrapReferenceValueAt(byte[] key, long atTx) {
        byte[] refVal = new byte[1 + 8 + key.length];
        refVal[0] = Consts.REFERENCE_VALUE_PREFIX;

        Utils.putUint64(atTx, refVal, 1);

        System.arraycopy(key, 0, refVal, 1 + 8, key.length);
        return refVal;
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

    public static void copy(byte[] src, byte[] dest) {
        System.arraycopy(src, 0, dest, 0, src.length);
    }

    public static void copy(byte[] src, byte[] dest, int destPos) {
        copy(src, 0, src.length, dest, destPos);
    }

    public static void copy(byte[] src, int srcPos, int length, byte[] dest) {
        copy(src, 0, length, dest, 0);
    }

    public static void copy(byte[] src, int srcPos, int length, byte[] dest, int destPos) {
        System.arraycopy(src, srcPos, dest, destPos, length);
    }

}
