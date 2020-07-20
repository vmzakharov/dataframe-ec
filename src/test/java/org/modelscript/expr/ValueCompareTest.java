package org.modelscript.expr;

import org.junit.Assert;
import org.junit.Test;
import org.modelscript.expr.value.DoubleValue;
import org.modelscript.expr.value.LongValue;
import org.modelscript.expr.value.StringValue;
import org.modelscript.expr.value.Value;

public class ValueCompareTest
{
    @Test
    public void longValueCompare()
    {
        LongValue lv1 = new LongValue(1);
        LongValue lv2 = new LongValue(2);
        LongValue lv1more = new LongValue(1);

        Assert.assertEquals(0, lv1.compareTo(lv1more));
        Assert.assertEquals(0, lv2.compareTo(lv2));
        Assert.assertTrue(lv1.compareTo(lv2) < 0);
        Assert.assertTrue(lv2.compareTo(lv1) > 0);

        Assert.assertTrue(lv1.compareTo(Value.VOID) > 0);
        Assert.assertTrue(Value.VOID.compareTo(lv1) < 0);
    }

    @Test
    public void doubleValueCompare()
    {
        DoubleValue dv1 = new DoubleValue(1.0);
        DoubleValue dv2 = new DoubleValue(2.0);
        DoubleValue dv1more = new DoubleValue(1.0);

        Assert.assertEquals(0, dv1.compareTo(dv1more));
        Assert.assertEquals(0, dv2.compareTo(dv2));
        Assert.assertTrue(dv1.compareTo(dv2) < 0);
        Assert.assertTrue(dv2.compareTo(dv1) > 0);

        Assert.assertTrue(dv1.compareTo(Value.VOID) > 0);
        Assert.assertTrue(Value.VOID.compareTo(dv1) < 0);
    }

    @Test
    public void stringValueCompare()
    {
        StringValue abc = new StringValue("abc");
        StringValue def = new StringValue("def");
        StringValue abcMore = new StringValue("abc");

        Assert.assertEquals(0, abc.compareTo(abcMore));
        Assert.assertEquals(0, def.compareTo(def));
        Assert.assertTrue(abc.compareTo(def) < 0);
        Assert.assertTrue(def.compareTo(abc) > 0);

        Assert.assertTrue(abc.compareTo(Value.VOID) > 0);
        Assert.assertTrue(Value.VOID.compareTo(abc) < 0);
    }

    @Test(expected = RuntimeException.class)
    public void compareToNull()
    {
        new StringValue("abc").compareTo(null);
        Assert.fail("Shouldn't get here");
    }
}
