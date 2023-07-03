
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
package io.codenotary.immudb4j.sql;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import io.codenotary.immudb.ImmudbProto;
import io.codenotary.immudb4j.Utils;

public class SQLValue {
    
    private ImmudbProto.SQLValue value;

    public SQLValue(Boolean value) {
        final ImmudbProto.SQLValue.Builder builder = ImmudbProto.SQLValue.newBuilder();

        if (value == null) {
            builder.setNull(null);
        } else {
            builder.setB(value);
        }

        this.value = builder.build();   
    }

    public SQLValue(Integer value) {
        final ImmudbProto.SQLValue.Builder builder = ImmudbProto.SQLValue.newBuilder();

        if (value == null) {
            builder.setNull(null);
        } else {
            builder.setN(value);
        }

        this.value = builder.build();   
    }

    public SQLValue(Long value) {
        final ImmudbProto.SQLValue.Builder builder = ImmudbProto.SQLValue.newBuilder();

        if (value == null) {
            builder.setNull(null);
        } else {
            builder.setN(value);
        }

        this.value = builder.build();   
    }

    public SQLValue(String value) {
        final ImmudbProto.SQLValue.Builder builder = ImmudbProto.SQLValue.newBuilder();

        if (value == null) {
            builder.setNull(null);
        } else {
            builder.setS(value);
        }

        this.value = builder.build();   
    }

    public SQLValue(Date value) {
        final ImmudbProto.SQLValue.Builder builder = ImmudbProto.SQLValue.newBuilder();

        if (value == null) {
            builder.setNull(null);
        } else {
            builder.setTs(TimeUnit.MILLISECONDS.toMicros(value.getTime()));
        }

        this.value = builder.build();   
    }

    public SQLValue(byte[] value) {
        final ImmudbProto.SQLValue.Builder builder = ImmudbProto.SQLValue.newBuilder();

        if (value == null) {
            builder.setNull(null);
        } else {
            builder.setBs(Utils.toByteString(value));
        }

        this.value = builder.build();   
    }

    public ImmudbProto.SQLValue asProtoSQLValue() {
        return value;
    }

}
