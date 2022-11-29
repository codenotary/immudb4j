
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

public class SQLQueryResult {
    
    private ImmudbProto.SQLQueryResult res;
    private int currRow = -1;

    private boolean closed;

    public SQLQueryResult(ImmudbProto.SQLQueryResult res) {
        if (res == null) {
            throw new RuntimeException("illegal arguments");
        }

        this.res = res;
    }

    public synchronized void close() throws SQLException {
        closed = true;
    }

    public synchronized boolean next() throws SQLException {
        if (closed) {
            throw new SQLException("already closed");
        }

        if (currRow + 1 >= res.getRowsCount()) {
            return false;
        }

        currRow++;
        
        return true;
    }

    private void validateReadingAt(int col) throws SQLException {
        if (closed) {
            throw new SQLException("already closed");
        }

        if (currRow < 0) {
            throw new SQLException("no row was read");
        }

        if (res.getRowsCount() == currRow) {
            throw new SQLException("no more rows");
        }

        if (res.getColumnsCount() < col) {
            throw new SQLException("invalid column");
        }
    }

    public synchronized int getColumnsCount() throws SQLException {
        if (closed) {
            throw new SQLException("already closed");
        }
        
        return res.getColumnsCount();
    }

    public synchronized String getColumnName(int i) throws SQLException {
        if (closed) {
            throw new SQLException("already closed");
        }

        final String fullColName = res.getColumns(i).getName();

        return fullColName.substring(fullColName.lastIndexOf(".")+1, fullColName.length()-1);
    }

    public synchronized String getColumnType(int i) throws SQLException {
        if (closed) {
            throw new SQLException("already closed");
        }

        return res.getColumns(i).getType();
    }

    public synchronized boolean getBoolean(int i) throws SQLException {
        validateReadingAt(i);

        return res.getRows(currRow).getValues(i).getB();
    }

    public synchronized int getInt(int i) throws SQLException {
        validateReadingAt(i);

        return (int)res.getRows(currRow).getValues(i).getN();
    }

    public synchronized long getLong(int i) throws SQLException {
        validateReadingAt(i);

        return res.getRows(currRow).getValues(i).getN();
    }

    public synchronized String getString(int i) throws SQLException {
        validateReadingAt(i);

        return res.getRows(currRow).getValues(i).getS();
    }

    public synchronized byte[] getBytes(int i) throws SQLException {
        validateReadingAt(i);

        return res.getRows(currRow).getValues(i).getBs().toByteArray();
    }

    public synchronized Date getDate(int i) throws SQLException {
        validateReadingAt(i);

        return new Date(TimeUnit.MICROSECONDS.toMillis(res.getRows(currRow).getValues(i).getTs()));
    }
}
