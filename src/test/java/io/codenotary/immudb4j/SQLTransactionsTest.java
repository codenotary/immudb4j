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

import io.codenotary.immudb4j.exceptions.VerificationException;
import io.codenotary.immudb4j.sql.SQLException;
import io.codenotary.immudb4j.sql.SQLQueryResult;
import io.codenotary.immudb4j.sql.SQLValue;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SQLTransactionsTest extends ImmuClientIntegrationTest {

    @Test(testName = "simple sql transaction")
    public void t1() throws VerificationException, InterruptedException, SQLException {
        immuClient.openSession("defaultdb", "immudb", "immudb");

        immuClient.beginTransaction();

        immuClient.sqlExec(
                "CREATE TABLE IF NOT EXISTS mytable(id INTEGER, title VARCHAR[256], active BOOLEAN, PRIMARY KEY id)");

        final int rows = 10;

        for (int i = 0; i < rows; i++) {
            immuClient.sqlExec("UPSERT INTO mytable(id, title, active) VALUES (?, ?, ?)",
                    new SQLValue(i),
                    new SQLValue(String.format("title%d", i)),
                    new SQLValue(i % 2 == 0));
        }

        SQLQueryResult res = immuClient.sqlQuery("SELECT id, title, active FROM mytable");

        Assert.assertEquals(res.getColumnsCount(), 3);

        Assert.assertEquals(res.getColumnName(0), "id");
        Assert.assertEquals(res.getColumnType(0), "INTEGER");

        Assert.assertEquals(res.getColumnName(1), "title");
        Assert.assertEquals(res.getColumnType(1), "VARCHAR");

        Assert.assertEquals(res.getColumnName(2), "active");
        Assert.assertEquals(res.getColumnType(2), "BOOLEAN");

        int i = 0;

        while (res.next()) {
            Assert.assertEquals(i, res.getInt(0));
            Assert.assertEquals(String.format("title%d", i), res.getString(1));
            Assert.assertEquals(i % 2 == 0, res.getBoolean(2));

            i++;
        }

        Assert.assertEquals(rows, i);

        res.close();

        immuClient.commitTransaction();

        immuClient.closeSession();
    }

}
