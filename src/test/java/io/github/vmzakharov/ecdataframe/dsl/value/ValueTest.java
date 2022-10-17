package io.github.vmzakharov.ecdataframe.dsl.value;

import org.junit.Assert;
import org.junit.Test;

public class ValueTest
{
    @Test
    public void booleansMakeSense()
    {
        Assert.assertTrue(BooleanValue.TRUE.isTrue());
        Assert.assertFalse(BooleanValue.TRUE.isFalse());

        Assert.assertTrue(BooleanValue.FALSE.isFalse());
        Assert.assertFalse(BooleanValue.FALSE.isTrue());

        Assert.assertSame(BooleanValue.TRUE, BooleanValue.valueOf(true));
        Assert.assertSame(BooleanValue.FALSE, BooleanValue.valueOf(false));
    }

    @Test
    public void voidValueMakesSense()
    {
        Assert.assertTrue(Value.VOID.isVoid());
    }

    @Test(expected = RuntimeException.class)
    public void StringValuesWrapNulls()
    {
        new StringValue(null);
    }

    @Test(expected = RuntimeException.class)
    public void DecimalValuesCannotWrapNulls()
    {
        new DecimalValue(null);
    }

    @Test(expected = RuntimeException.class)
    public void DateValuesCannotWrapNulls()
    {
        new DateValue(null);
    }

    @Test(expected = RuntimeException.class)
    public void DateTimeValuesCannotWrapNulls()
    {
        new DateTimeValue(null);
    }

    @Test(expected = RuntimeException.class)
    public void DataFrameValuesCannotWrapNulls()
    {
        new DataFrameValue(null);
    }
}
