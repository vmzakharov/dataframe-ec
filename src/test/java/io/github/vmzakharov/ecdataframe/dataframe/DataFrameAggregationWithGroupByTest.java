package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.*;

public class DataFrameAggregationWithGroupByTest
{
    private static final double TOLERANCE = 0.00001;

    private DataFrame dataFrame;
    private DataFrame smallDataFrame;

    @Before
    public void initialiseDataFrame()
    {
        this.dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Bob",   "Def",  456L, 12.0, 25.0)
                .addRow("Alice", "Abc",  123L, 10.0, 20.0)
                .addRow("Carol", "Rrr",  789L, 15.0, 40.0)
                .addRow("Bob",   "Def",  111L, 12.0, 25.0)
                .addRow("Carol", "Qqq",  789L, 15.0, 40.0)
                .addRow("Carol", "Zzz",  789L, 15.0, 40.0)
                ;

        this.smallDataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc",  123L, 10.0, 22.0)
                .addRow("Alice", "Xyz",  456L, 11.0, 20.0)
                ;
    }

    @Test
    public void sumGroupingOneRow()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc",  123L, 10.0, 20.0);

        DataFrame summed = df.sumBy(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        Assert.assertEquals(1, summed.rowCount());

        Assert.assertEquals("Alice", summed.getString("Name", 0));
        Assert.assertEquals(123L, summed.getLong("Bar", 0));
        Assert.assertEquals(10.0, summed.getDouble("Baz", 0), TOLERANCE);
        Assert.assertEquals(20.0, summed.getDouble("Qux", 0), TOLERANCE);
    }

    @Test
    public void sumGroupingSimple()
    {
        DataFrame summed = this.smallDataFrame.sumBy(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", 579, 21.0, 42.0);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void minGroupingSimple()
    {
        DataFrame summed = this.smallDataFrame.aggregateBy(Lists.immutable.of(min("Bar"), min("Baz"), min("Qux")), Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", 123L, 10.0, 20.0);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void maxGroupingSimple()
    {
        DataFrame summed = this.smallDataFrame.aggregateBy(Lists.immutable.of(max("Bar"), max("Baz"), max("Qux")), Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", 456L, 11.0, 22.0);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void mixedGroupingSimple()
    {
        DataFrame summed = this.smallDataFrame.aggregateBy(Lists.immutable.of(sum("Bar"), min("Baz"), max("Qux")), Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", 579L, 10.0, 22.0);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void multiAggWithGroupingSimple()
    {
        DataFrame summed = this.smallDataFrame.aggregateBy(
                Lists.immutable.of(sum("Bar", "BarSum"), min("Bar", "BarMin"), sum("Qux", "QuxSum"), max("Qux", "QuxMax"), count("Qux", "QuxCount")),
                Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("BarSum").addLongColumn("BarMin").addDoubleColumn("QuxSum").addDoubleColumn("QuxMax").addLongColumn("QuxCount")
                .addRow("Alice", 579L, 123L, 42.0, 22.0, 2);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void sumWithGrouping()
    {
        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Bob",    567, 24,  50)
                .addRow("Alice",  123, 10,  20)
                .addRow("Carol", 2367, 45, 120);

        DataFrameUtil.assertEquals(expected,
                this.dataFrame.sumBy(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name")));

        DataFrameUtil.assertEquals(expected,
                this.dataFrame.aggregateBy(Lists.immutable.of(sum("Bar"), sum("Baz"), sum("Qux")), Lists.immutable.of("Name")));
    }

    @Test
    public void averageWithGrouping()
    {
        DataFrame expected = new DataFrame("Expected")
            .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Bob",    283, 12, 25)
                .addRow("Alice",  123, 10, 20)
                .addRow("Carol",  789, 15, 40);

        DataFrameUtil.assertEquals(expected,
                this.dataFrame.aggregateBy(Lists.immutable.of(avg("Bar"), avg("Baz"), avg("Qux")), Lists.immutable.of("Name")));
    }

    @Test
    public void multiAggWithGrouping()
    {
        DataFrame summed = this.dataFrame.aggregateBy(
                Lists.immutable.of(
                        count("Name", "NameCount"), sum("Bar", "BarSum"), max("Bar", "BarMax"), sum("Qux", "QuxSum"), min("Qux", "QuxMin"), count("Qux", "QuxCount")),
                Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name")
                .addLongColumn("NameCount").addLongColumn("BarSum").addLongColumn("BarMax").addDoubleColumn("QuxSum").addDoubleColumn("QuxMin").addLongColumn("QuxCount")
                .addRow("Bob",   2,  567, 456,  50.0, 25.0, 2)
                .addRow("Alice", 1,  123, 123,  20.0, 20.0, 1)
                .addRow("Carol", 3, 2367, 789, 120.0, 40.0, 3);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void differentAggregationsWithGrouping()
    {
        DataFrame summed = this.dataFrame.aggregateBy(
                Lists.immutable.of(sum("Bar"), min("Baz"), max("Qux"), count("Name", "NameCount"), count("Baz", "BazCount")),
                Lists.immutable.of("Name")
        );

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addLongColumn("NameCount").addLongColumn("BazCount")
                .addRow("Bob",    567, 12.0, 25.0, 2, 2)
                .addRow("Alice",  123, 10.0, 20.0, 1, 1)
                .addRow("Carol", 2367, 15.0, 40.0, 3, 3);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void sumWithGroupingByTwoColumns()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux");

        df.addRow("Bob",   "Def",  456L, 12.0, 25.0);
        df.addRow("Bob",   "Abc",  123L, 44.0, 33.0);
        df.addRow("Alice", "Qqq",  123L, 10.0, 20.0);
        df.addRow("Carol", "Rrr",  789L, 15.0, 40.0);
        df.addRow("Bob",   "Def",  111L, 12.0, 25.0);
        df.addRow("Carol", "Qqq",   10L, 55.0, 22.0);
        df.addRow("Carol", "Rrr",  789L, 16.0, 41.0);

        DataFrame summed = df.sumBy(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name", "Foo"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Bob",   "Def",   567L, 24.0,  50.0)
                .addRow("Bob",   "Abc",   123L, 44.0,  33.0)
                .addRow("Alice", "Qqq",   123L, 10.0,  20.0)
                .addRow("Carol", "Rrr",  1578L, 31.0,  81.0)
                .addRow("Carol", "Qqq",    10L, 55.0,  22.0);

        DataFrameUtil.assertEquals(expected, summed);
    }
        
    @Test
    public void sumOfAndByCalculatedColumns()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux");

        df.addRow("Bob",   "Def",  456L, 12.0, 25.0);
        df.addRow("Bob",   "Abc",  123L, 44.0, 33.0);
        df.addRow("Alice", "Qqq",  123L, 10.0, 20.0);
        df.addRow("Carol", "Rrr",  789L, 15.0, 40.0);
        df.addRow("Bob",   "Def",  111L, 12.0, 25.0);
        df.addRow("Carol", "Qqq",   10L, 55.0, 22.0);
        df.addRow("Carol", "Rrr",  789L, 16.0, 41.0);

        df.addStringColumn("aFoo", "'a' + Foo");
        df.addLongColumn("BarBar", "Bar * 2");
        df.addDoubleColumn("BazBaz", "Baz * 2");

        DataFrame summed = df.sumBy(Lists.immutable.of("BarBar", "BazBaz"), Lists.immutable.of("Name", "aFoo"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("BarBar").addDoubleColumn("BazBaz")
                .addRow("Bob",   "aDef",  1134L,  48.0)
                .addRow("Bob",   "aAbc",   246L,  88.0)
                .addRow("Alice", "aQqq",   246L,  20.0)
                .addRow("Carol", "aRrr",  3156L,  62.0)
                .addRow("Carol", "aQqq",    20L, 110.0);

        DataFrameUtil.assertEquals(expected, summed);
    }
}
