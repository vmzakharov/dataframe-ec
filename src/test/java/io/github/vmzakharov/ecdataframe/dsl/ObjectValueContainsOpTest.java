package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.VectorValue;
import org.eclipse.collections.impl.utility.ArrayIterate;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class ObjectValueContainsOpTest
{
    @Test
    public void stringIn()
    {
        Assert.assertTrue( ExpressionTestUtil.evaluate(ContainsOp.IN, new StringValue("Foo"), this.vectorOfStrings("Foo", "Bar", null, "Baz")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.IN, new StringValue("Foo"), this.vectorOfStrings("Bar", null, "Baz")));
        Assert.assertTrue( ExpressionTestUtil.evaluate(ContainsOp.IN, new StringValue(null), this.vectorOfStrings("Foo", "Bar", null, "Baz")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.IN, new StringValue(null), this.vectorOfStrings("Foo", "Bar", "Baz")));
    }

    @Test
    public void stringNotIn()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, new StringValue("Foo"), this.vectorOfStrings("Foo", "Bar", null, "Baz")));
        Assert.assertTrue( ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, new StringValue("Foo"), this.vectorOfStrings("Bar", null, "Baz")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, new StringValue(null), this.vectorOfStrings("Foo", "Bar", null, "Baz")));
        Assert.assertTrue( ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, new StringValue(null), this.vectorOfStrings("Foo", "Bar", "Baz")));
    }

    @Test
    public void dateIn()
    {
        Assert.assertTrue( ExpressionTestUtil.evaluate(ContainsOp.IN, this.dateValue(2020, 8, 15), this.vectorOfDates(LocalDate.of(2020, 8, 15), LocalDate.of(2020, 7, 15), null, LocalDate.of(2020, 8, 2))));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.IN, this.dateValue(2020, 8, 15), this.vectorOfDates(LocalDate.of(2020, 7, 15), null, LocalDate.of(2020, 8, 2))));
        Assert.assertTrue( ExpressionTestUtil.evaluate(ContainsOp.IN, new DateValue(null), this.vectorOfDates(LocalDate.of(2020, 8, 15), LocalDate.of(2020, 7, 15), null, LocalDate.of(2020, 8, 2))));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.IN, new DateValue(null), this.vectorOfDates(LocalDate.of(2020, 8, 15), LocalDate.of(2020, 7, 15), LocalDate.of(2020, 8, 2))));
    }

    @Test
    public void dateNotIn()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, this.dateValue(2020, 8, 15), this.vectorOfDates(LocalDate.of(2020, 8, 15), LocalDate.of(2020, 7, 15), null, LocalDate.of(2020, 8, 2))));
        Assert.assertTrue( ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, this.dateValue(2020, 8, 15), this.vectorOfDates(LocalDate.of(2020, 7, 15), null, LocalDate.of(2020, 8, 2))));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, new DateValue(null), this.vectorOfDates(LocalDate.of(2020, 8, 15), LocalDate.of(2020, 7, 15), null, LocalDate.of(2020, 8, 2))));
        Assert.assertTrue( ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, new DateValue(null), this.vectorOfDates(LocalDate.of(2020, 8, 15), LocalDate.of(2020, 7, 15), LocalDate.of(2020, 8, 2))));
    }

    public VectorValue vectorOfStrings(String... items)
    {
        return new VectorValue(ArrayIterate.collect(items, StringValue::new));
    }

    public VectorValue vectorOfDates(LocalDate... items)
    {
        return new VectorValue(ArrayIterate.collect(items, DateValue::new));
    }

    private DateValue dateValue(int year, int month, int dayOfMonth)
    {
        return new DateValue(LocalDate.of(year, month, dayOfMonth));
    }
}
