package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.list.primitive.IntInterval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataFrameExpressionTest
{
    private DataFrame df;

    @Before
    public void initializeDataFrame()
    {
        this.df = new DataFrame("this.df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value");

        this.df
                .addRow("Alice",  5, 23.45)
                .addRow("Bob",   10, 12.34)
                .addRow("Carol", 11, 56.78)
                .addRow("Dan",    0,  7.89);
    }

    @Test
    public void arithmeticExpression()
    {
        Expression expression = ExpressionTestUtil.toExpression("Count * 2");

        MutableLongList values = LongLists.mutable.of();

        IntInterval.zeroTo(3)
                .collectLong(i -> ((LongValue) this.df.evaluateExpression(expression, i)).longValue(), values);
        Assert.assertEquals(LongLists.immutable.of(10, 20, 22, 0), values);
    }

    @Test
    public void stringExpression()
    {
        Expression expression = ExpressionTestUtil.toExpression("substr(Name, 2) + \"X\"");

        ImmutableList<Object> values = IntInterval.zeroTo(3).collect(i -> this.df.evaluateExpression(expression, i).stringValue());

        Assert.assertEquals(Lists.immutable.of("iceX", "bX", "rolX", "nX"), values);
    }
}
