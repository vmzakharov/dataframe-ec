package io.github.vmzakharov.ecdataframe.dsl.value;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ValueTest
{
    @Test
    public void booleansMakeSense()
    {
        assertTrue(BooleanValue.TRUE.isTrue());
        assertFalse(BooleanValue.TRUE.isFalse());

        assertTrue(BooleanValue.FALSE.isFalse());
        assertFalse(BooleanValue.FALSE.isTrue());

        assertSame(BooleanValue.TRUE, BooleanValue.valueOf(true));
        assertSame(BooleanValue.FALSE, BooleanValue.valueOf(false));
    }

    @Test
    public void voidValueMakesSense()
    {
        assertTrue(Value.VOID.isVoid());
    }

    @Test
    public void stringValuesWrapNulls()
    {
        assertThrows(RuntimeException.class, () -> new StringValue(null));
    }

    @Test
    public void decimalValuesCannotWrapNulls()
    {
        assertThrows(RuntimeException.class, () -> new DecimalValue(null));
    }

    @Test
    public void dateValuesCannotWrapNulls()
    {
        assertThrows(RuntimeException.class, () -> new DateValue(null));
    }

    @Test
    public void dateTimeValuesCannotWrapNulls()
    {
        assertThrows(RuntimeException.class, () -> new DateTimeValue(null));
    }

    @Test
    public void dataFrameValuesCannotWrapNulls()
    {
        assertThrows(RuntimeException.class, () -> new DataFrameValue(null));
    }
}
