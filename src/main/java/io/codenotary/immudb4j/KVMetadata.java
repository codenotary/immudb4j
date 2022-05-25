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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;

public class KVMetadata {

    static final byte deletedAttrCode = 0;
    static final byte expiresAtAttrCode = 1;
    static final byte nonIndexableAttrCode = 2;

    private static final int deletedAttrSize = 0;
    private static final int expiresAtAttrSize = 8;
    private static final int nonIndexableAttrSize = 0;

    private static final int maxKVMetadataLen = (MetadataAttribute.AttrCodeSize + deletedAttrSize) + 
                                                (MetadataAttribute.AttrCodeSize + expiresAtAttrSize) + 
                                                (MetadataAttribute.AttrCodeSize + nonIndexableAttrSize);

    private HashMap<Byte,MetadataAttribute> attributes;

    public KVMetadata() {
        attributes  = new HashMap<Byte,MetadataAttribute>();
    }

    public void asDeleted(boolean deleted) {
        if (!deleted) {
            attributes.remove(deletedAttrCode);
            return;
        }

        if (!attributes.containsKey(deletedAttrCode)) {
            attributes.put(deletedAttrCode, new DeletedAttribute());
        }

        return;
    }

    public boolean deleted() {
        return attributes.containsKey(deletedAttrCode);
    }

    public void asNonIndexable(boolean nonIndexable) {
        if (!nonIndexable) {
            attributes.remove(nonIndexableAttrCode);
            return;
        }

        if (!attributes.containsKey(nonIndexableAttrCode)) {
            attributes.put(nonIndexableAttrCode, new NonIndexableAttribute());
        }

        return;
    }

    public boolean nonIndexable() {
        return attributes.containsKey(nonIndexableAttrCode);
    }

    public void expiresAt(long expirationTime) {
        ExpiresAtAttribute expiresAt;

        if (attributes.containsKey(expiresAtAttrCode)) {
            expiresAt = (ExpiresAtAttribute) attributes.get(expiresAtAttrCode);
            expiresAt.expiresAt = expirationTime;
        } else {
            expiresAt = new ExpiresAtAttribute(expirationTime);
            attributes.put(expiresAtAttrCode, expiresAt);
        }
    }

    public boolean hasExpirationTime() {
        return attributes.containsKey(expiresAtAttrCode);
    }

    public long expirationTime() {
        if (!attributes.containsKey(expiresAtAttrCode)){
            throw new RuntimeException("no expiration time set");
        }

        return ((ExpiresAtAttribute) attributes.get(expiresAtAttrCode)).expiresAt;
    }

    public byte[] serialize() {
        ByteBuffer bytes =  ByteBuffer.allocate(maxKVMetadataLen);

        for (byte attrCode : new byte[]{deletedAttrCode, expiresAtAttrCode, nonIndexableAttrCode}) {
            if (attributes.containsKey(attrCode)) {
                bytes.put(attrCode);
                bytes.put(attributes.get(attrCode).serialize());
            }
        }

        return bytes.array();
    }
}

class DeletedAttribute implements MetadataAttribute {

    @Override
    public byte code() {
        return KVMetadata.deletedAttrCode;
    }

    @Override
	public byte[] serialize() {
        return null;
    }

}

class ExpiresAtAttribute implements MetadataAttribute {

    long expiresAt;

    ExpiresAtAttribute(long expiresAt) {
        this.expiresAt = expiresAt;
    }

    @Override
    public byte code() {
        return KVMetadata.expiresAtAttrCode;
    }

    @Override
	public byte[] serialize() {
        ByteBuffer bytes =  ByteBuffer.allocate(Long.BYTES);
        bytes.order(ByteOrder.BIG_ENDIAN).putLong(expiresAt);
        return bytes.array();
    }

}

class NonIndexableAttribute implements MetadataAttribute {
    
    @Override
    public byte code() {
        return KVMetadata.nonIndexableAttrCode;
    }

    @Override
	public byte[] serialize() {
        return null;
    }

}