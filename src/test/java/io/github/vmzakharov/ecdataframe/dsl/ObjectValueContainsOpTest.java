package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.VectorValue;
import org.eclipse.collections.impl.utility.ArrayIterate;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class ObjectValueContainsOpTest
{
    @Test
    public void stringVectorIn()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ContainsOp.IN, new StringValue("Foo"), this.vectorOfStrings("Foo", "Bar", null, "Baz")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.IN, new StringValue("Foo"), this.vectorOfStrings("Bar", null, "Baz")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ContainsOp.IN, Value.VOID, this.vectorOfStrings("Foo", "Bar", null, "Baz")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.IN, Value.VOID, this.vectorOfStrings("Foo", "Bar", "Baz")));
    }

    @Test
    public void stringVectorNotIn()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, new StringValue("Foo"), this.vectorOfStrings("Foo", "Bar", null, "Baz")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, new StringValue("Foo"), this.vectorOfStrings("Bar", null, "Baz")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, Value.VOID, this.vectorOfStrings("Foo", "Bar", null, "Baz")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, Value.VOID, this.vectorOfStrings("Foo", "Bar", "Baz")));
    }

    @Test
    public void dateVectorIn()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ContainsOp.IN, this.dateValue(2020, 8, 15), this.vectorOfDates(LocalDate.of(2020, 8, 15), LocalDate.of(2020, 7, 15), null, LocalDate.of(2020, 8, 2))));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.IN, this.dateValue(2020, 8, 15), this.vectorOfDates(LocalDate.of(2020, 7, 15), null, LocalDate.of(2020, 8, 2))));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ContainsOp.IN, Value.VOID, this.vectorOfDates(LocalDate.of(2020, 8, 15), LocalDate.of(2020, 7, 15), null, LocalDate.of(2020, 8, 2))));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.IN, Value.VOID, this.vectorOfDates(LocalDate.of(2020, 8, 15), LocalDate.of(2020, 7, 15), LocalDate.of(2020, 8, 2))));
    }

    @Test
    public void dateVectorNotIn()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, this.dateValue(2020, 8, 15), this.vectorOfDates(LocalDate.of(2020, 8, 15), LocalDate.of(2020, 7, 15), null, LocalDate.of(2020, 8, 2))));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, this.dateValue(2020, 8, 15), this.vectorOfDates(LocalDate.of(2020, 7, 15), null, LocalDate.of(2020, 8, 2))));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, Value.VOID, this.vectorOfDates(LocalDate.of(2020, 8, 15), LocalDate.of(2020, 7, 15), null, LocalDate.of(2020, 8, 2))));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, Value.VOID, this.vectorOfDates(LocalDate.of(2020, 8, 15), LocalDate.of(2020, 7, 15), LocalDate.of(2020, 8, 2))));
    }

    @Test
    public void stringIn()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluate(ContainsOp.IN, new StringValue("Hello"), new StringValue("Hello, there!")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ContainsOp.IN, new StringValue(""),      new StringValue("Hello, there!")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.IN, new StringValue("Thar"), new StringValue("Hello, there!")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.IN, new StringValue("Hello"), new StringValue("")));
    }

    @Test
    public void stringNotIn()
    {
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, new StringValue("Hello"), new StringValue("Hello, there!")));
        Assert.assertFalse(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, new StringValue(""),      new StringValue("Hello, there!")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, new StringValue("Thar"), new StringValue("Hello, there!")));
        Assert.assertTrue(ExpressionTestUtil.evaluate(ContainsOp.NOT_IN, new StringValue("Hello"), new StringValue("")));
    }

    public VectorValue vectorOfStrings(String... items)
    {
        return new VectorValue(ArrayIterate.collect(items, item -> item == null ? Value.VOID : new StringValue(item)));
    }

    public VectorValue vectorOfDates(LocalDate... items)
    {
        return new VectorValue(ArrayIterate.collect(items, item -> item == null ? Value.VOID : new DateValue(item)));
    }

    private DateValue dateValue(int year, int month, int dayOfMonth)
    {
        return new DateValue(LocalDate.of(year, month, dayOfMonth));
    }
}
