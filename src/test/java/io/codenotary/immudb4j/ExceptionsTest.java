package io.codenotary.immudb4j;

import io.codenotary.immudb4j.exceptions.CorruptedDataException;
import io.codenotary.immudb4j.exceptions.VerificationException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExceptionsTest {

    @Test
    public void t1() {
        CorruptedDataException cdex = new CorruptedDataException();
        Assert.assertNull(cdex.getMessage());
    }

    @Test
    public void t2() {
        String errorMsg = "data is corrupted";
        VerificationException vex = new VerificationException(errorMsg);
        Assert.assertEquals(errorMsg, vex.getMessage());

        Throwable cause = new Throwable();
        vex = new VerificationException(errorMsg, cause);
        Assert.assertEquals(cause, vex.getCause());
    }

}
