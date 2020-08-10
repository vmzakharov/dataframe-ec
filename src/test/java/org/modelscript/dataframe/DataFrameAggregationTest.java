package org.modelscript.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Test;

public class DataFrameAggregationTest
{
    private static final double TOLERANCE = 0.0;

    @Test
    public void sumItAll()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc",  123L, 10.0, 20.0)
                .addRow("Bob",   "Def",  456L, 12.0, 25.0)
                .addRow("Carol", "Xyz",  789L, 15.0, 40.0);

        DataFrame summed = dataFrame.sum(Lists.immutable.of("Bar", "Baz", "Qux"));

        Assert.assertEquals(1368L, summed.getLongColumn("Bar").getLong(0));
        Assert.assertEquals( 37.0, summed.getDoubleColumn("Baz").getDouble(0), TOLERANCE);
        Assert.assertEquals( 85.0, summed.getDoubleColumn("Qux").getDouble(0), TOLERANCE);
    }

    @Test(expected = RuntimeException.class)
    public void sumNonNumericTriggersError()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc",  123L, 10.0, 20.0)
                .addRow("Bob",   "Def",  456L, 12.0, 25.0)
                .addRow("Carol", "Xyz",  789L, 15.0, 40.0);

        DataFrame summed = dataFrame.sum(Lists.immutable.of("Foo", "Bar", "Baz"));

        Assert.fail("Shouldn't get to this line");
    }

    @Test
    public void sumGroupingTrivial()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc",  123L, 10.0, 20.0);

        DataFrame summed = dataFrame.sumBy(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        Assert.assertEquals(1, summed.rowCount());

        Assert.assertEquals("Alice", summed.getString("Name", 0));
        Assert.assertEquals(   123L, summed.getLong("Bar", 0));
        Assert.assertEquals(   10.0, summed.getDouble("Baz", 0), TOLERANCE);
        Assert.assertEquals(   20.0, summed.getDouble("Qux", 0), TOLERANCE);
    }

    @Test
    public void sumGroupingSimple()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux");

        dataFrame.addRow("Alice", "Abc",  123L, 10.0, 20.0);
        dataFrame.addRow("Alice", "Xyz",  456L, 11.0, 22.0);

        DataFrame summed = dataFrame.sumBy(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", 579, 21.0, 42.0);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void sumWithGrouping()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux");

        dataFrame.addRow("Bob",   "Def",  456L, 12.0, 25.0);
        dataFrame.addRow("Alice", "Abc",  123L, 10.0, 20.0);
        dataFrame.addRow("Carol", "Rrr",  789L, 15.0, 40.0);
        dataFrame.addRow("Bob",   "Def",  111L, 12.0, 25.0);
        dataFrame.addRow("Carol", "Qqq",  789L, 15.0, 40.0);
        dataFrame.addRow("Carol", "Zzz",  789L, 15.0, 40.0);

        DataFrame summed = dataFrame.sumBy(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        Assert.assertEquals(3, summed.rowCount());

        DataFrame expected = new DataFrame("Expected")
            .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Bob",	  567, 24,  50)
                .addRow("Alice",  123, 10,  20)
                .addRow("Carol", 2367, 45, 120);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void sumWithGroupingByTwoColumns()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux");

        dataFrame.addRow("Bob",   "Def",  456L, 12.0, 25.0);
        dataFrame.addRow("Bob",   "Abc",  123L, 44.0, 33.0);
        dataFrame.addRow("Alice", "Qqq",  123L, 10.0, 20.0);
        dataFrame.addRow("Carol", "Rrr",  789L, 15.0, 40.0);
        dataFrame.addRow("Bob",   "Def",  111L, 12.0, 25.0);
        dataFrame.addRow("Carol", "Qqq",   10L, 55.0, 22.0);
        dataFrame.addRow("Carol", "Rrr",  789L, 16.0, 41.0);

        DataFrame summed = dataFrame.sumBy(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name", "Foo"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Bob",	 "Def",   567L, 24.0,  50.0)
                .addRow("Bob",	 "Abc",   123L, 44.0,  33.0)
                .addRow("Alice", "Qqq",   123L, 10.0,  20.0)
                .addRow("Carol", "Rrr",  1578L, 31.0,  81.0)
                .addRow("Carol", "Qqq",    10L, 55.0,  22.0);

        DataFrameUtil.assertEquals(expected, summed);
    }
}
