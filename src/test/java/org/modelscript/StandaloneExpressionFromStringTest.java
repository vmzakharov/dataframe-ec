package org.modelscript;

import org.junit.Assert;
import org.junit.Test;

public class StandaloneExpressionFromStringTest
{
    private static final double TOLERANCE = 0.00000001;

    @Test
    public void operationOrder()
    {
        Assert.assertEquals(13, ExpressionTestUtil.evaluateToInt("3 + 5 * 2"));
        Assert.assertEquals(17, ExpressionTestUtil.evaluateToInt("3 * 5 + 2"));
        Assert.assertEquals(16, ExpressionTestUtil.evaluateToInt("(3 + 5) * 2"));
        Assert.assertEquals(-8, ExpressionTestUtil.evaluateToInt("(3 + 5) * (2 - 4) / 2"));
        Assert.assertEquals( 6, ExpressionTestUtil.evaluateToInt("(3 + 5) / 4 * 3"));
        Assert.assertEquals( 6, ExpressionTestUtil.evaluateToInt("(3 + 5) * 3 / 4"));

        Assert.assertEquals(-8, ExpressionTestUtil.evaluateToInt("(3+ 5) * (2 -4)/(1+1)"));
    }

    @Test
    public void intsAndDoubles()
    {
        Assert.assertEquals( 4.0, ExpressionTestUtil.evaluateToDouble("3.0 + 1"), TOLERANCE);
        Assert.assertEquals(13.0, ExpressionTestUtil.evaluateToDouble("3 + 5 * 2.0"), TOLERANCE);
        Assert.assertEquals(-8.0, ExpressionTestUtil.evaluateToDouble("(3.0 + 5.0) * (2 - 4) / 2"), TOLERANCE);
    }

    @Test
    public void numberComparisonOperation()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("3 > 1"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("3 > 5"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("7 <= (4 + 3)"));
    }

    @Test
    public void numberComparisonOperationMixedTypes()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("3.1 > 1"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("3 > 5.2"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("7 <= (4.2 + 2.8)"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("7 == (4.2 + 2.8)"));
    }

    @Test
    public void stringComparisonOperations()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("\"B\" > \"A\""));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("\"abc\" == \"abc\""));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("\"abc\" == \"xyz\""));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("\"less\" <= \"more\""));
    }

    @Test
    public void booleanOperation()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("(3 > 1) and (5 >= 5)"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("3 > 5) and (1 == 1)"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("(3 > 5) or (1 == 1)"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("7 > (3 + 1) and (5 <= 6)"));
    }


    @Test
    public void inOperation()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("\"foo\" in [\"foo\", \"bar\", \"baz\"]"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("\"foo\" in [\"qux\", \"bar\", \"baz\"]"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("123 in [456, 567, 123]"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("123 in [456, 567, 789]"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("123 in []"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("\"abc\" in []"));
    }


}
