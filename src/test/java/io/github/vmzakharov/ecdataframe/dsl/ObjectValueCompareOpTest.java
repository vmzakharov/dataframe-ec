package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

public class ObjectValueCompareOpTest
{
    @Test
    public void stringEq()
    {
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.EQ, new StringValue("Foo"), new StringValue("Foo")));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.EQ, Value.VOID, new StringValue("Foo")));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.EQ, new StringValue("Foo"), Value.VOID));
    }

    @Test
    public void stringGt()
    {
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GT, new StringValue("Foo"), new StringValue("Bar")));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, new StringValue("Bar"), new StringValue("Foo")));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, Value.VOID, new StringValue("Foo")));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GT, new StringValue("Foo"), Value.VOID));
    }

    @Test
    public void stringLt()
    {
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, new StringValue("Foo"), new StringValue("Bar")));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LT, new StringValue("Bar"), new StringValue("Foo")));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LT, Value.VOID, new StringValue("Foo")));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, new StringValue("Foo"), Value.VOID));
    }

    @Test
    public void stringGte()
    {
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue("Foo"), new StringValue("Bar")));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue("Foo"), new StringValue("Foo")));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue("Bar"), new StringValue("Foo")));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GTE, Value.VOID, new StringValue("Foo")));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue("Foo"), Value.VOID));
    }

    @Test
    public void stringLte()
    {
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue("Foo"), new StringValue("Bar")));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue("Bar"), new StringValue("Bar")));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue("Bar"), new StringValue("Foo")));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, Value.VOID, new StringValue("Foo")));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue("Foo"), Value.VOID));
    }

    @Test
    public void dateEq()
    {
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.EQ, this.dateValue(2020, 9, 1), this.dateValue(2020, 9, 1)));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.EQ, Value.VOID, this.dateValue(2020, 9, 1)));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.EQ, this.dateValue(2020, 9, 1), Value.VOID));
    }

    @Test
    public void dateGt()
    {
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GT, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, Value.VOID, this.dateValue(2020, 9, 1)));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GT, this.dateValue(2020, 9, 1), Value.VOID));
    }

    @Test
    public void dateLt()
    {
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LT, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LT, Value.VOID, this.dateValue(2020, 9, 1)));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, this.dateValue(2020, 9, 1), Value.VOID));
    }

    @Test
    public void dateGte()
    {
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, this.dateValue(2020, 9, 1), this.dateValue(2020, 9, 1)));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GTE, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GTE, Value.VOID, this.dateValue(2020, 9, 1)));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, this.dateValue(2020, 9, 1), Value.VOID));
    }

    @Test
    public void dateLte()
    {
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LTE, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, this.dateValue(2020, 9, 1), this.dateValue(2020, 9, 1)));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, Value.VOID, this.dateValue(2020, 9, 1)));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LTE, this.dateValue(2020, 9, 1), Value.VOID));
    }

    @Test
    public void voidValuesComparisons()
    {
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.EQ, Value.VOID, Value.VOID));

        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, Value.VOID, Value.VOID));
        assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, Value.VOID, Value.VOID));

        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, Value.VOID, Value.VOID));
        assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, Value.VOID, Value.VOID));
    }

    private DateValue dateValue(int year, int month, int dayOfMonth)
    {
        return new DateValue(LocalDate.of(year, month, dayOfMonth));        
    }
}
