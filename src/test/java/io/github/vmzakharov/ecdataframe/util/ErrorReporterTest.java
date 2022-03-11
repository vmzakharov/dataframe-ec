package io.github.vmzakharov.ecdataframe.util;

import io.github.vmzakharov.ecdataframe.dataframe.ErrorReporter;
import org.junit.Assert;
import org.junit.Test;

public class ErrorReporterTest
{
    @Test
    public void defaultExceptionType()
    {
        try
        {
            ErrorReporter.initialize();
            ErrorReporter.reportAndThrow("Hello");
            Assert.fail("didn't throw");
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals(RuntimeException.class, e.getClass());
            Assert.assertEquals("Hello", e.getMessage());
        }
    }

    @Test
    public void defaultExceptionTypeWithCauseArg()
    {
        Throwable cause = new RuntimeException("Boom!");

        try
        {
            ErrorReporter.initialize();
            ErrorReporter.reportAndThrow("Hello", cause);
            Assert.fail("didn't throw");
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals(RuntimeException.class, e.getClass());
            Assert.assertEquals("Hello", e.getMessage());
            Assert.assertEquals(cause, e.getCause());
        }
    }

    @Test
    public void defaultUnsupported()
    {
        try
        {
            ErrorReporter.initialize();
            ErrorReporter.unsupported("Do it!");
            Assert.fail("didn't throw");
        }
        catch (UnsupportedOperationException e)
        {
            Assert.assertEquals(UnsupportedOperationException.class, e.getClass());
            Assert.assertEquals("Do it!", e.getMessage());
        }
    }

    @Test
    public void overrideExceptionType()
    {
        try
        {
            ErrorReporter.exceptionFactories(VerySpecialException::new, VerySpecialException::new, DontWanna::new);
            ErrorReporter.reportAndThrow("Hello");
            Assert.fail("didn't throw");
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals(VerySpecialException.class, e.getClass());
            Assert.assertEquals("Hello", e.getMessage());
        }
    }

    @Test
    public void overrideUnsupported()
    {
        try
        {
            ErrorReporter.exceptionFactories(VerySpecialException::new, VerySpecialException::new, DontWanna::new);
            ErrorReporter.unsupported("Do it!");
            Assert.fail("didn't throw");
        }
        catch (UnsupportedOperationException e)
        {
            Assert.assertEquals(DontWanna.class, e.getClass());
            Assert.assertEquals("Do it!", e.getMessage());
        }
    }

    @Test
    public void overrideExceptionTypeWithCauseArg()
    {
        Throwable cause = new RuntimeException("Boom!");

        try
        {
            ErrorReporter.exceptionFactories(VerySpecialException::new, VerySpecialException::new, DontWanna::new);
            ErrorReporter.reportAndThrow("Hello", cause);
            Assert.fail("didn't throw");
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals(VerySpecialException.class, e.getClass());
            Assert.assertEquals("Hello", e.getMessage());
            Assert.assertEquals(cause, e.getCause());
        }
    }

    private static class VerySpecialException
    extends RuntimeException
    {
        public VerySpecialException(String message)
        {
            super(message);
        }

        public VerySpecialException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }

    private static class DontWanna
    extends UnsupportedOperationException
    {
        public DontWanna(String message)
        {
            super(message);
        }
    }
}
