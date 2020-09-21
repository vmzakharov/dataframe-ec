package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class ObjectValueComparisonTest
{
    @Test
    public void stringEq()
    {
        Assert.assertTrue( this.evaluate(ComparisonOp.EQ, new StringValue("Foo"), new StringValue("Foo")));
        Assert.assertFalse(this.evaluate(ComparisonOp.EQ, new StringValue(null), new StringValue("Foo")));
        Assert.assertFalse(this.evaluate(ComparisonOp.EQ, new StringValue("Foo"), new StringValue(null)));
        Assert.assertTrue( this.evaluate(ComparisonOp.EQ, new StringValue(null), new StringValue(null)));
    }

    @Test
    public void stringGt()
    {
        Assert.assertTrue( this.evaluate(ComparisonOp.GT, new StringValue("Foo"), new StringValue("Bar")));
        Assert.assertFalse(this.evaluate(ComparisonOp.GT, new StringValue("Bar"), new StringValue("Foo")));
        Assert.assertFalse(this.evaluate(ComparisonOp.GT, new StringValue(null), new StringValue("Foo")));
        Assert.assertTrue( this.evaluate(ComparisonOp.GT, new StringValue("Foo"), new StringValue(null)));
        Assert.assertFalse(this.evaluate(ComparisonOp.GT, new StringValue(null), new StringValue(null)));
    }

    @Test
    public void stringLt()
    {
        Assert.assertFalse(this.evaluate(ComparisonOp.LT, new StringValue("Foo"), new StringValue("Bar")));
        Assert.assertTrue( this.evaluate(ComparisonOp.LT, new StringValue("Bar"), new StringValue("Foo")));
        Assert.assertTrue( this.evaluate(ComparisonOp.LT, new StringValue(null), new StringValue("Foo")));
        Assert.assertFalse(this.evaluate(ComparisonOp.LT, new StringValue("Foo"), new StringValue(null)));
        Assert.assertFalse(this.evaluate(ComparisonOp.LT, new StringValue(null), new StringValue(null)));
    }

    @Test
    public void stringGte()
    {
        Assert.assertTrue( this.evaluate(ComparisonOp.GTE, new StringValue("Foo"), new StringValue("Bar")));
        Assert.assertFalse(this.evaluate(ComparisonOp.GTE, new StringValue("Bar"), new StringValue("Foo")));
        Assert.assertFalse(this.evaluate(ComparisonOp.GTE, new StringValue(null), new StringValue("Foo")));
        Assert.assertTrue( this.evaluate(ComparisonOp.GTE, new StringValue("Foo"), new StringValue(null)));
        Assert.assertTrue( this.evaluate(ComparisonOp.GTE, new StringValue(null), new StringValue(null)));
    }

    @Test
    public void stringLte()
    {
        Assert.assertFalse(this.evaluate(ComparisonOp.LTE, new StringValue("Foo"), new StringValue("Bar")));
        Assert.assertTrue( this.evaluate(ComparisonOp.LTE, new StringValue("Bar"), new StringValue("Foo")));
        Assert.assertTrue( this.evaluate(ComparisonOp.LTE, new StringValue(null), new StringValue("Foo")));
        Assert.assertFalse(this.evaluate(ComparisonOp.LTE, new StringValue("Foo"), new StringValue(null)));
        Assert.assertTrue( this.evaluate(ComparisonOp.LTE, new StringValue(null), new StringValue(null)));
    }

    @Test
    public void dateEq()
    {
        Assert.assertTrue( this.evaluate(ComparisonOp.EQ, this.dateValue(2020, 9, 1), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(this.evaluate(ComparisonOp.EQ, new DateValue(null), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(this.evaluate(ComparisonOp.EQ, this.dateValue(2020, 9, 1), new DateValue(null)));
        Assert.assertTrue( this.evaluate(ComparisonOp.EQ, new DateValue(null), new DateValue(null)));
    }

    @Test
    public void dateGt()
    {
        Assert.assertTrue( this.evaluate(ComparisonOp.GT, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        Assert.assertFalse(this.evaluate(ComparisonOp.GT, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(this.evaluate(ComparisonOp.GT, new DateValue(null), this.dateValue(2020, 9, 1)));
        Assert.assertTrue( this.evaluate(ComparisonOp.GT, this.dateValue(2020, 9, 1), new DateValue(null)));
        Assert.assertFalse(this.evaluate(ComparisonOp.GT, new DateValue(null), new DateValue(null)));
    }

    @Test
    public void dateLt()
    {
        Assert.assertFalse(this.evaluate(ComparisonOp.LT, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        Assert.assertTrue( this.evaluate(ComparisonOp.LT, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        Assert.assertTrue( this.evaluate(ComparisonOp.LT, new DateValue(null), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(this.evaluate(ComparisonOp.LT, this.dateValue(2020, 9, 1), new DateValue(null)));
        Assert.assertFalse(this.evaluate(ComparisonOp.LT, new DateValue(null), new DateValue(null)));
    }

    @Test
    public void dateGte()
    {
        Assert.assertTrue( this.evaluate(ComparisonOp.GTE, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        Assert.assertFalse(this.evaluate(ComparisonOp.GTE, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(this.evaluate(ComparisonOp.GTE, new DateValue(null), this.dateValue(2020, 9, 1)));
        Assert.assertTrue( this.evaluate(ComparisonOp.GTE, this.dateValue(2020, 9, 1), new DateValue(null)));
        Assert.assertTrue( this.evaluate(ComparisonOp.GTE, new DateValue(null), new DateValue(null)));
    }

    @Test
    public void dateLte()
    {
        Assert.assertFalse(this.evaluate(ComparisonOp.LTE, this.dateValue(2020, 9, 1), this.dateValue(2020, 8, 17)));
        Assert.assertTrue( this.evaluate(ComparisonOp.LTE, this.dateValue(2020, 8, 17), this.dateValue(2020, 9, 1)));
        Assert.assertTrue( this.evaluate(ComparisonOp.LTE, new DateValue(null), this.dateValue(2020, 9, 1)));
        Assert.assertFalse(this.evaluate(ComparisonOp.LTE, this.dateValue(2020, 9, 1), new DateValue(null)));
        Assert.assertTrue( this.evaluate(ComparisonOp.LTE, new DateValue(null), new DateValue(null)));
    }

    private boolean evaluate(ComparisonOp op, Value value1, Value value2)
    {
        InMemoryEvaluationVisitor evaluationVisitor = new InMemoryEvaluationVisitor();
        Expression expression = new BinaryExpr(new ConstExpr(value1), new ConstExpr(value2), op);
        return ((BooleanValue) expression.evaluate(evaluationVisitor)).isTrue();
    }
    
    private DateValue dateValue(int year, int month, int dayOfMonth)
    {
        return new DateValue(LocalDate.of(year, month, dayOfMonth));        
    }
}
