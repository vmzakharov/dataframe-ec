package io.github.vmzakharov.ecdataframe;

import org.junit.Assert;
import org.junit.Test;

public class StandaloneExpressionFromStringTest
{
    private static final double TOLERANCE = 0.00000001;

    @Test
    public void numbers()
    {
        Assert.assertEquals(1, ExpressionTestUtil.evaluateToLong("1"));
        Assert.assertEquals(-1, ExpressionTestUtil.evaluateToLong("-1"));
        Assert.assertEquals(10.12, ExpressionTestUtil.evaluateToDouble("10.12"), TOLERANCE);
        Assert.assertEquals(-11.13, ExpressionTestUtil.evaluateToDouble("-11.13"), TOLERANCE);
    }

    @Test
    public void operationOrder()
    {
        Assert.assertEquals(13, ExpressionTestUtil.evaluateToLong("3 + 5 * 2"));
        Assert.assertEquals(17, ExpressionTestUtil.evaluateToLong("3 * 5 + 2"));
        Assert.assertEquals(16, ExpressionTestUtil.evaluateToLong("(3 + 5) * 2"));
        Assert.assertEquals(-8, ExpressionTestUtil.evaluateToLong("(3 + 5) * (2 - 4) / 2"));
        Assert.assertEquals(6, ExpressionTestUtil.evaluateToLong("(3 + 5) / 4 * 3"));
        Assert.assertEquals(6, ExpressionTestUtil.evaluateToLong("(3 + 5) * 3 / 4"));

        Assert.assertEquals(-8, ExpressionTestUtil.evaluateToLong("(3+ 5) * (2 -4)/(1+1)"));

        // looks bad, but parses...
        Assert.assertEquals(-7, ExpressionTestUtil.evaluateToLong("3 + -5 * 2"));
    }

    @Test
    public void intsAndDoubles()
    {
        Assert.assertEquals(4.0, ExpressionTestUtil.evaluateToDouble("3.0 + 1"), TOLERANCE);
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
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("1 < 1.23"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("7 <= (4.2 + 2.8)"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("7 == (4.2 + 2.8)"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("3 != 1.0"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("1.0 == 1"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("3.0 >= 3"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("2.0 != 2"));
    }

    @Test
    public void stringParsing()
    {
        Assert.assertEquals("ABC", ExpressionTestUtil.evaluateToString("\"ABC\""));
        Assert.assertEquals("A'B'C", ExpressionTestUtil.evaluateToString("\"A'B'C\""));

        Assert.assertEquals("ABC", ExpressionTestUtil.evaluateToString("'ABC'"));
        Assert.assertEquals("A\"B\"C", ExpressionTestUtil.evaluateToString("'A\"B\"C'"));

        Assert.assertEquals("A'B'CA\"B\"C", ExpressionTestUtil.evaluateToString("\"A'B'C\" + 'A\"B\"C'"));
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
    public void notOperation()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("not (1 > 2)"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("not (123 not in ())"));
    }

    @Test
    public void inOperation()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("\"foo\" in (\"foo\", \"bar\", \"baz\")"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("\"foo\" in (\"qux\", \"bar\", \"baz\")"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("'foo' in ('qux', 'bar', 'baz')"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("'foo' in (\"qux\", 'ba\"r', 'baz', \"wal'do\")"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("123 in (456, 567, 123)"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("123 in (456, 567, 789)"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("123 in ()"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("\"abc\" in ()"));
    }

    @Test
    public void inOperationMixedNumericTypes()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("123 in (456, 567, 123.0)"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("123.0 in (456, 123, 789)"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("123.0 in (456, 567, 789)"));
    }

    @Test
    public void notInOperation()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("\"foo\" not in (\"foo\", \"bar\", \"baz\")"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("\"foo\" not in (\"qux\", \"bar\", \"baz\")"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("123 not in (456, 567, 123)"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("123 not in (456, 567, 789)"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("123 not in ()"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("\"abc\" not in ()"));
    }

    @Test
    public void notInOperationForStrings()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("'abc' in 'foo abc bar'"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("'abc' not in 'foo abc bar'"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("\"XYZ\" in \"abracadabra\""));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("\"XYZ\" not in \"abracadabra\""));
    }

    @Test
    public void ternaryOperator()
    {
        Assert.assertEquals("Ah", ExpressionTestUtil.evaluateToString("5 > 6 ? 'Huh' : 'Ah'"));
        Assert.assertEquals(110, ExpressionTestUtil.evaluateToLong("toUpper('Times') == 'TIMES' ? 10 * 11 : 10 + 11"));
    }

    @Test(expected = RuntimeException.class)
    public void incompatibleComparison()
    {
        ExpressionTestUtil.evaluateToBoolean("'abc' ==  1");
    }
}
