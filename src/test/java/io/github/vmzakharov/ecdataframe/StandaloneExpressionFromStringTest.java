package io.github.vmzakharov.ecdataframe;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class StandaloneExpressionFromStringTest
{
    private static final double TOLERANCE = 0.00000001;

    @Test
    public void numbers()
    {
        assertEquals(1, ExpressionTestUtil.evaluateToLong("1"));
        assertEquals(-1, ExpressionTestUtil.evaluateToLong("-1"));
        assertEquals(10.12, ExpressionTestUtil.evaluateToDouble("10.12"), TOLERANCE);
        assertEquals(-11.13, ExpressionTestUtil.evaluateToDouble("-11.13"), TOLERANCE);
    }

    @Test
    public void operationOrder()
    {
        assertEquals(13, ExpressionTestUtil.evaluateToLong("3 + 5 * 2"));
        assertEquals(17, ExpressionTestUtil.evaluateToLong("3 * 5 + 2"));
        assertEquals(16, ExpressionTestUtil.evaluateToLong("(3 + 5) * 2"));
        assertEquals(-8, ExpressionTestUtil.evaluateToLong("(3 + 5) * (2 - 4) / 2"));
        assertEquals(6, ExpressionTestUtil.evaluateToLong("(3 + 5) / 4 * 3"));
        assertEquals(6, ExpressionTestUtil.evaluateToLong("(3 + 5) * 3 / 4"));

        assertEquals(-8, ExpressionTestUtil.evaluateToLong("(3+ 5) * (2 -4)/(1+1)"));

        // looks bad, but parses...
        assertEquals(-7, ExpressionTestUtil.evaluateToLong("3 + -5 * 2"));
    }

    @Test
    public void intsAndDoubles()
    {
        assertEquals(4.0, ExpressionTestUtil.evaluateToDouble("3.0 + 1"), TOLERANCE);
        assertEquals(13.0, ExpressionTestUtil.evaluateToDouble("3 + 5 * 2.0"), TOLERANCE);
        assertEquals(-8.0, ExpressionTestUtil.evaluateToDouble("(3.0 + 5.0) * (2 - 4) / 2"), TOLERANCE);
    }

    @Test
    public void numberComparisonOperation()
    {
        assertTrue(ExpressionTestUtil.evaluateToBoolean("3 > 1"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("3 > 5"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("7 <= (4 + 3)"));
    }

    @Test
    public void numberComparisonOperationMixedTypes()
    {
        assertTrue(ExpressionTestUtil.evaluateToBoolean("3.1 > 1"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("3 > 5.2"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("1 < 1.23"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("7 <= (4.2 + 2.8)"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("7 == (4.2 + 2.8)"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("3 != 1.0"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("1.0 == 1"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("3.0 >= 3"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("2.0 != 2"));
    }

    @Test
    public void stringParsing()
    {
        assertEquals("ABC", ExpressionTestUtil.evaluateToString("\"ABC\""));
        assertEquals("A'B'C", ExpressionTestUtil.evaluateToString("\"A'B'C\""));

        assertEquals("ABC", ExpressionTestUtil.evaluateToString("'ABC'"));
        assertEquals("A\"B\"C", ExpressionTestUtil.evaluateToString("'A\"B\"C'"));

        assertEquals("A'B'CA\"B\"C", ExpressionTestUtil.evaluateToString("\"A'B'C\" + 'A\"B\"C'"));
    }

    @Test
    public void stringComparisonOperations()
    {
        assertTrue(ExpressionTestUtil.evaluateToBoolean("\"B\" > \"A\""));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("\"abc\" == \"abc\""));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("\"abc\" == \"xyz\""));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("\"less\" <= \"more\""));
    }

    @Test
    public void booleanOperation()
    {
        assertTrue(ExpressionTestUtil.evaluateToBoolean("(3 > 1) and (5 >= 5)"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("3 > 5) and (1 == 1)"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("(3 > 5) or (1 == 1)"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("7 > (3 + 1) and (5 <= 6)"));
    }

    @Test
    public void notOperation()
    {
        assertTrue(ExpressionTestUtil.evaluateToBoolean("not (1 > 2)"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("not (123 not in ())"));
    }

    @Test
    public void inOperation()
    {
        assertTrue(ExpressionTestUtil.evaluateToBoolean("\"foo\" in (\"foo\", \"bar\", \"baz\")"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("\"foo\" in (\"qux\", \"bar\", \"baz\")"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("'foo' in ('qux', 'bar', 'baz')"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("'foo' in (\"qux\", 'ba\"r', 'baz', \"wal'do\")"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("123 in (456, 567, 123)"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("123 in (456, 567, 789)"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("123 in ()"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("\"abc\" in ()"));
    }

    @Test
    public void inOperationMixedNumericTypes()
    {
        assertTrue(ExpressionTestUtil.evaluateToBoolean("123 in (456, 567, 123.0)"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("123.0 in (456, 123, 789)"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("123.0 in (456, 567, 789)"));
    }

    @Test
    public void notInOperation()
    {
        assertFalse(ExpressionTestUtil.evaluateToBoolean("\"foo\" not in (\"foo\", \"bar\", \"baz\")"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("\"foo\" not in (\"qux\", \"bar\", \"baz\")"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("123 not in (456, 567, 123)"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("123 not in (456, 567, 789)"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("123 not in ()"));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("\"abc\" not in ()"));
    }

    @Test
    public void notInOperationForStrings()
    {
        assertTrue(ExpressionTestUtil.evaluateToBoolean("'abc' in 'foo abc bar'"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("'abc' not in 'foo abc bar'"));
        assertFalse(ExpressionTestUtil.evaluateToBoolean("\"XYZ\" in \"abracadabra\""));
        assertTrue(ExpressionTestUtil.evaluateToBoolean("\"XYZ\" not in \"abracadabra\""));
    }

    @Test
    public void ternaryOperator()
    {
        assertEquals("Ah", ExpressionTestUtil.evaluateToString("5 > 6 ? 'Huh' : 'Ah'"));
        assertEquals(110, ExpressionTestUtil.evaluateToLong("toUpper('Times') == 'TIMES' ? 10 * 11 : 10 + 11"));
    }

    @Test
    public void incompatibleComparison()
    {
        assertThrows(
            RuntimeException.class,
            () -> ExpressionTestUtil.evaluateToBoolean("'abc' ==  1")
        );
    }
}
