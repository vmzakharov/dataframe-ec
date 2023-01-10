package io.github.vmzakharov.ecdataframe.util;

import org.eclipse.collections.api.factory.Maps;
import org.junit.Assert;
import org.junit.Test;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exception;
import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionFromKey;

public class ExceptionFactoryTest
{
    @Test
    public void defaultExceptionType()
    {
        try
        {
            ExceptionFactory.initialize();
            exception("Hello").fire();
            Assert.fail("didn't throw");
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals(RuntimeException.class, e.getClass());
            Assert.assertEquals("Hello", e.getMessage());
        }
    }

    @Test
    public void exceptionMessageByKey()
    {
        try
        {
            FormatWithPlaceholders.addMessagesFromMap(Maps.mutable.with("HELLO", "Hello, ${Name}"));
            ExceptionFactory.initialize();
            exceptionFromKey("HELLO").with("Name", "Bob").fire();
            Assert.fail("didn't throw");
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals(RuntimeException.class, e.getClass());
            Assert.assertEquals("Hello, Bob", e.getMessage());
        }
    }

    @Test
    public void defaultExceptionTypeWithCauseArg()
    {
        Throwable cause = new RuntimeException("Boom!");

        try
        {
            ExceptionFactory.initialize();
            exception("Hello").fire(cause);
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
            ExceptionFactory.initialize();
            throw exception("Do it!").getUnsupported();
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
            ExceptionFactory.exceptionFactories(VerySpecialException::new, VerySpecialException::new, DontWanna::new);
            exception("Hello").fire();
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
            ExceptionFactory.exceptionFactories(VerySpecialException::new, VerySpecialException::new, DontWanna::new);
            throw exception("Do it!").getUnsupported();
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
            ExceptionFactory.exceptionFactories(VerySpecialException::new, VerySpecialException::new, DontWanna::new);
            exception("Hello").fire(cause);
            Assert.fail("didn't throw");
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals(VerySpecialException.class, e.getClass());
            Assert.assertEquals("Hello", e.getMessage());
            Assert.assertEquals(cause, e.getCause());
        }
    }

    @Test
    public void errorPrinter()
    {
        CollectingPrinter printer = new CollectingPrinter();
        ExceptionFactory.setPrintedMessagePrefix("Boo-boo: ");
        ExceptionFactory.setErrorPrinter(printer);

        try
        {
            exception("ouch").fire();
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals("Boo-boo: ouch\n", printer.toString());
        }

        printer.clear();

        try
        {
            exception("ow-ow").fire(new RuntimeException("Nothing to see here"));
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals("Boo-boo: ow-ow\n", printer.toString());
        }

        printer.clear();

        try
        {
            throw exception("oh, well").getUnsupported();
        }
        catch (UnsupportedOperationException e)
        {
            Assert.assertEquals("Boo-boo: oh, well\n", printer.toString());
        }

        ExceptionFactory.initialize();
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
