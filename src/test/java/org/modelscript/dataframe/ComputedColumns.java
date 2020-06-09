package org.modelscript.dataframe;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.DoubleLists;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.modelscript.expr.SimpleEvalContext;
import org.modelscript.expr.value.LongValue;

public class ComputedColumns
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
    public void constantComputedColumn()
    {
        this.df.addLongColumn("Number Two", "2");

        Assert.assertEquals(4, this.df.columnCount());
        Assert.assertEquals(4, this.df.rowCount());

        for (int i = 0; i < 4; i++)
        {
            Assert.assertEquals(2, this.df.getLong("Number Two", i));
        }
    }

    @Test
    public void simpleColumnDependentExpression()
    {
        this.df.addLongColumn("Double Count", "Count * 2").addLongColumn("Two More", "Count + 2");

        Assert.assertEquals(5, this.df.columnCount());
        Assert.assertEquals(4, this.df.rowCount());

        Assert.assertEquals(LongLists.immutable.of(10L, 20L, 22L, 0L), this.df.getLongColumn("Double Count").toLongList());
        Assert.assertEquals( 7L, this.df.getLongColumn("Two More").getLong(0));
        Assert.assertEquals(12L, this.df.getLongColumn("Two More").getLong(1));
        Assert.assertEquals(13L, this.df.getLongColumn("Two More").getLong(2));
        Assert.assertEquals( 2L, this.df.getLongColumn("Two More").getLong(3));
    }

    @Test
    public void computedColumnDependentExpression()
    {
        this.df
            .addLongColumn("CountTwiceAndTwo", "CountTwice + 2")
            .addLongColumn("AllTogetherNow", "CountTwice + CountTwiceAndTwo")
            .addLongColumn("CountTwice", "Count * 2");

        Assert.assertEquals(LongLists.immutable.of(12L, 22L, 24L, 2L), this.df.getLongColumn("CountTwiceAndTwo").toLongList());
        Assert.assertEquals(LongLists.immutable.of(22L, 42L, 46L, 2L), this.df.getLongColumn("AllTogetherNow").toLongList());
    }

    @Test
    public void externalEvalContext()
    {
        SimpleEvalContext evalContext = new SimpleEvalContext();
        evalContext.setVariable("Three", new LongValue(3));

        this.df.addLongColumn("CountMore", "Count * Three");

        this.df.setExternalEvalContext(evalContext);

        Assert.assertEquals(LongLists.immutable.of(15L, 30L, 33L, 0L), this.df.getLongColumn("CountMore").toLongList());
    }
    
    @Test
    public void doubleComputedColumn()
    {
        this.df.addDoubleColumn("ValuePlus", "Value + 1.01");

        Assert.assertEquals(DoubleLists.immutable.of(24.46, 13.35, 57.79, 8.90), this.df.getDoubleColumn("ValuePlus").toDoubleList());
    }

    @Test
    public void stringComputedColumnSimple()
    {
        this.df.addStringColumn("Greeting", "\"Dear \" + Name");

        Assert.assertEquals(Lists.immutable.of("Dear Alice", "Dear Bob", "Dear Carol", "Dear Dan"), this.df.getStringColumn("Greeting").toList());
    }

    @Test
    public void stringComputedColumnFunction()
    {
        this.df.addStringColumn("Nickname", "substr(Name, 0, 3) + \"ka\"");

        Assert.assertEquals(Lists.immutable.of("Alika", "Bobka", "Carka", "Danka"), this.df.getStringColumn("Nickname").toList());
    }
}
