package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class ObjectValueCompareOpTest
{
    @Test
    public void stringEq()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.EQ, new StringValue("Foo"), new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.EQ, new StringValue(null), new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.EQ, new StringValue("Foo"), new StringValue(null)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.EQ, new StringValue(null), new StringValue(null)));
    }

    @Test
    public void stringGt()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GT, new StringValue("Foo"), new StringValue("Bar")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, new StringValue("Bar"), new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, new StringValue(null), new StringValue("Foo")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GT, new StringValue("Foo"), new StringValue(null)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, new StringValue(null), new StringValue(null)));
    }

    @Test
    public void stringLt()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, new StringValue("Foo"), new StringValue("Bar")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LT, new StringValue("Bar"), new StringValue("Foo")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LT, new StringValue(null), new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, new StringValue("Foo"), new StringValue(null)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, new StringValue(null), new StringValue(null)));
    }

    @Test
    public void stringGte()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue("Foo"), new StringValue("Bar")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue("Bar"), new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue(null), new StringValue("Foo")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue("Foo"), new StringValue(null)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new StringValue(null), new StringValue(null)));
    }

    @Test
    public void stringLte()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue("Foo"), new StringValue("Bar")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue("Bar"), new StringValue("Foo")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue(null), new StringValue("Foo")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue("Foo"), new StringValue(null)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new StringValue(null), new StringValue(null)));
    }

    @Test
    public void dateEq()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.EQ, this.dateValue(2020, 9, 1), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.EQ, new DateValue(null), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.EQ, this.dateValue(2020, 9, 1), new DateValue(null)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.EQ, new DateValue(null), new DateValue(null)));
    }

    @Test
    public void dateGt()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GT, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, new DateValue(null), this.dateValue(2020, 9, 1)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GT, this.dateValue(2020, 9, 1), new DateValue(null)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GT, new DateValue(null), new DateValue(null)));
    }

    @Test
    public void dateLt()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LT, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LT, new DateValue(null), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, this.dateValue(2020, 9, 1), new DateValue(null)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LT, new DateValue(null), new DateValue(null)));
    }

    @Test
    public void dateGte()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GTE, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new DateValue(null), this.dateValue(2020, 9, 1)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, this.dateValue(2020, 9, 1), new DateValue(null)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.GTE, new DateValue(null), new DateValue(null)));
    }

    @Test
    public void dateLte()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LTE, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new DateValue(null), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ComparisonOp.LTE, this.dateValue(2020, 9, 1), new DateValue(null)));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ComparisonOp.LTE, new DateValue(null), new DateValue(null)));
    }
    
    private DateValue dateValue(int year, int month, int dayOfMonth)
    {
        return new DateValue(LocalDate.of(year, month, dayOfMonth));        
    }
}
