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
}
