package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class ObjectValueCompareOpTest
{
    @Test
    public void stringEq()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.EQ, new StringValue("Foo"), new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.EQ, Value.VOID, new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.EQ, new StringValue("Foo"), Value.VOID));
    }

    @Test
    public void stringGt()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GT, new StringValue("Foo"), new StringValue("Bar")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, new StringValue("Bar"), new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, Value.VOID, new StringValue("Foo")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GT, new StringValue("Foo"), Value.VOID));
    }

    @Test
    public void stringLt()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, new StringValue("Foo"), new StringValue("Bar")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LT, new StringValue("Bar"), new StringValue("Foo")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LT, Value.VOID, new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, new StringValue("Foo"), Value.VOID));
    }

    @Test
    public void stringGte()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue("Foo"), new StringValue("Bar")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue("Foo"), new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue("Bar"), new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GTE, Value.VOID, new StringValue("Foo")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue("Foo"), Value.VOID));
    }

    @Test
    public void stringLte()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue("Foo"), new StringValue("Bar")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue("Bar"), new StringValue("Bar")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue("Bar"), new StringValue("Foo")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, Value.VOID, new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue("Foo"), Value.VOID));
    }

    @Test
    public void dateEq()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.EQ, this.dateValue(2020, 9, 1), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.EQ, Value.VOID, this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.EQ, this.dateValue(2020, 9, 1), Value.VOID));
    }

    @Test
    public void dateGt()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GT, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, Value.VOID, this.dateValue(2020, 9, 1)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GT, this.dateValue(2020, 9, 1), Value.VOID));
    }

    @Test
    public void dateLt()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LT, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LT, Value.VOID, this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, this.dateValue(2020, 9, 1), Value.VOID));
    }

    @Test
    public void dateGte()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, this.dateValue(2020, 9, 1), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GTE, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GTE, Value.VOID, this.dateValue(2020, 9, 1)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, this.dateValue(2020, 9, 1), Value.VOID));
    }

    @Test
    public void dateLte()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LTE, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, this.dateValue(2020, 9, 1), this.dateValue(2020, 9, 1)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, Value.VOID, this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LTE, this.dateValue(2020, 9, 1), Value.VOID));
    }

    @Test
    public void voidValuesComparisons()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.EQ, Value.VOID, Value.VOID));

        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, Value.VOID, Value.VOID));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, Value.VOID, Value.VOID));

        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, Value.VOID, Value.VOID));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, Value.VOID, Value.VOID));
    }

    private DateValue dateValue(int year, int month, int dayOfMonth)
    {
        return new DateValue(LocalDate.of(year, month, dayOfMonth));        
    }
}
