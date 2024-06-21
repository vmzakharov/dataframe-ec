package io.github.vmzakharov.ecdataframe.util;

import org.eclipse.collections.api.factory.Maps;

import org.junit.jupiter.api.Test;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exception;
import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExceptionFactoryTest
{
    @Test
    public void defaultExceptionType()
    {
        ExceptionFactory.initialize();

        Exception e = assertThrows(RuntimeException.class, () -> exception("Hello").fire());

        assertEquals("Hello", e.getMessage());
    }

    @Test
    public void exceptionMessageByKey()
    {
        FormatWithPlaceholders.addMessagesFromMap(Maps.mutable.with("HELLO", "Hello, ${Name}"));
        ExceptionFactory.initialize();

        Exception e = assertThrows(RuntimeException.class, () -> exceptionByKey("HELLO").with("Name", "Bob").fire());

        assertEquals("Hello, Bob", e.getMessage());
    }

    @Test
    public void defaultExceptionTypeWithCauseArg()
    {
        Throwable cause = new RuntimeException("Boom!");

        ExceptionFactory.initialize();
        Exception e = assertThrows(RuntimeException.class, () -> exception("Hello").fire(cause));

        assertEquals("Hello", e.getMessage());
        assertEquals(cause, e.getCause());
    }

    @Test
    public void defaultUnsupported()
    {
        ExceptionFactory.initialize();

        Exception e = assertThrows(
                UnsupportedOperationException.class,
                () -> {
                    throw exception("Do it!").getUnsupported();
                });

        assertEquals("Do it!", e.getMessage());
    }

    @Test
    public void overrideExceptionType()
    {
        ExceptionFactory.exceptionFactories(VerySpecialException::new, VerySpecialException::new, DontWanna::new);

        Exception e = assertThrows(VerySpecialException.class, () -> exception("Hello").fire());

        assertEquals("Hello", e.getMessage());
    }

    @Test
    public void overrideUnsupported()
    {
        ExceptionFactory.exceptionFactories(VerySpecialException::new, VerySpecialException::new, DontWanna::new);

        Exception e = assertThrows(DontWanna.class, () -> {
            throw exception("Do it!").getUnsupported();
        });

        assertEquals("Do it!", e.getMessage());
    }

    @Test
    public void overrideExceptionTypeWithCauseArg()
    {
        Throwable cause = new RuntimeException("Boom!");

        ExceptionFactory.exceptionFactories(VerySpecialException::new, VerySpecialException::new, DontWanna::new);

        Exception e = assertThrows(VerySpecialException.class, () -> exception("Hello").fire(cause));

        assertEquals("Hello", e.getMessage());
        assertEquals(cause, e.getCause());
    }

    @Test
    public void errorPrinter()
    {
        CollectingPrinter printer = new CollectingPrinter();
        ExceptionFactory.setPrintedMessagePrefix("Boo-boo: ");
        ExceptionFactory.setErrorPrinter(printer);

        assertThrows(RuntimeException.class, () -> exception("ouch").fire());

        assertEquals("Boo-boo: ouch\n", printer.toString());

        printer.clear();

        assertThrows(
                RuntimeException.class,
                () -> exception("ow-ow").fire(new RuntimeException("Nothing to see here")));

        assertEquals("Boo-boo: ow-ow\n", printer.toString());

        printer.clear();

        assertThrows(
                UnsupportedOperationException.class,
                () -> {
                    throw exception("oh, well").getUnsupported();
                });

        assertEquals("Boo-boo: oh, well\n", printer.toString());

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
