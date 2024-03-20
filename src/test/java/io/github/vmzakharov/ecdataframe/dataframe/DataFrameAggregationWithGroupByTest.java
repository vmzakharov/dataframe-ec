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
                .addStringColumn("Name").addStringColumn("Foo")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addIntColumn("Waldo")
                .addRow("Bob",   "Def",  456L, 12.0, 25.0, 46)
                .addRow("Bob",   "Abc",  123L, 44.0, 33.0, 10)
                .addRow("Alice", "Qqq",  123L, 10.0, 20.0, 23)
                .addRow("Carol", "Rrr",  789L, 15.0, 40.0, 89)
                .addRow("Bob",   "Def",  111L, 12.0, 25.0, 11)
                .addRow("Carol", "Qqq",   10L, 55.0, 22.0, 89)
                .addRow("Carol", "Rrr",  789L, 16.0, 41.0, 89)
                ;

        this.smallDataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addIntColumn("Waldo")
                .addRow("Alice", "Abc",  123L, 10.0, 22.0, 10)
                .addRow("Alice", "Xyz",  456L, 11.0, 20.0, 14)
                ;
    }

    @Test
    public void sumGroupingOneRow()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addIntColumn("Waldo")
                .addRow("Alice", "Abc",  123L, 10.0, 20.0, 48);

        DataFrame summed = df.sumBy(Lists.immutable.of("Bar", "Baz", "Qux", "Waldo"), Lists.immutable.of("Name"));

        Assert.assertEquals(1, summed.rowCount());

        Assert.assertEquals("Alice", summed.getString("Name", 0));
        Assert.assertEquals(123L, summed.getLong("Bar", 0));
        Assert.assertEquals(10.0, summed.getDouble("Baz", 0), TOLERANCE);
        Assert.assertEquals(20.0, summed.getDouble("Qux", 0), TOLERANCE);
        Assert.assertEquals(48, summed.getLong("Waldo", 0));
    }

    @Test
    public void sumGroupingSimple()
    {
        DataFrame summed = this.smallDataFrame.sumBy(Lists.immutable.of("Bar", "Baz", "Qux", "Waldo"), Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addLongColumn("Waldo")
                .addRow("Alice", 579, 21.0, 42.0, 24);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void minGroupingSimple()
    {
        DataFrame summed = this.smallDataFrame.aggregateBy(
                Lists.immutable.of(min("Bar"), min("Baz"), min("Qux"), min("Waldo")),
                Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addIntColumn("Waldo")
                .addRow("Alice", 123L, 10.0, 20.0, 10);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void maxGroupingSimple()
    {
        DataFrame summed = this.smallDataFrame.aggregateBy(
                Lists.immutable.of(max("Bar"), max("Baz"), max("Qux"), max("Waldo")),
                Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addIntColumn("Waldo")
                .addRow("Alice", 456L, 11.0, 22.0, 14);

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
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addLongColumn("Waldo")
                .addRow("Bob",    690, 68,  83,  67)
                .addRow("Alice",  123, 10,  20,  23)
                .addRow("Carol", 1588, 86, 103, 267);

        DataFrameUtil.assertEquals(expected,
                this.dataFrame.sumBy(
                        Lists.immutable.of("Bar", "Baz", "Qux", "Waldo"),
                        Lists.immutable.of("Name"))
        );

        DataFrameUtil.assertEquals(expected,
                this.dataFrame.aggregateBy(
                        Lists.immutable.of(sum("Bar"), sum("Baz"), sum("Qux"), sum("Waldo")),
                        Lists.immutable.of("Name"))
        );
    }

    @Test
    public void averageWithGrouping()
    {
        DataFrame expected = new DataFrame("Expected")
            .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addIntColumn("Waldo")
                .addRow("Bob",   230L, 22.666666, 27.666666, 22)
                .addRow("Alice", 123L, 10.0,      20.0,      23)
                .addRow("Carol", 529L, 28.666666, 34.333333, 89);

        DataFrameUtil.assertEquals(
                expected,
                this.dataFrame.aggregateBy(
                        Lists.immutable.of(avg("Bar"), avg("Baz"), avg("Qux"), avg("Waldo")),
                        Lists.immutable.of("Name")), TOLERANCE
        );
    }

    @Test
    public void mixOfAggregations()
    {
        DataFrame summed = this.dataFrame.aggregateBy(
                Lists.immutable.of(
                        count("Name", "NameCount"), sum("Bar", "BarSum"), max("Bar", "BarMax"), sum("Qux", "QuxSum"), min("Qux", "QuxMin"), count("Qux", "QuxCount")),
                Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name")
                .addLongColumn("NameCount").addLongColumn("BarSum").addLongColumn("BarMax").addDoubleColumn("QuxSum").addDoubleColumn("QuxMin").addLongColumn("QuxCount")
                .addRow("Bob",   3,  690, 456,  83.0, 25.0, 3)
                .addRow("Alice", 1,  123, 123,  20.0, 20.0, 1)
                .addRow("Carol", 3, 1588, 789, 103.0, 22.0, 3);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void mixOfAggregations2()
    {
        DataFrame summed = this.dataFrame.aggregateBy(
                Lists.immutable.of(
                        sum("Bar"), min("Baz"), max("Qux"), count("Name", "NameCount"), count("Baz", "BazCount"), count("Waldo", "Waldos")
                ),
                Lists.immutable.of("Name")
        );

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addLongColumn("NameCount").addLongColumn("BazCount").addLongColumn("Waldos")
                .addRow("Bob",    690, 12.0, 33.0, 3, 3, 3)
                .addRow("Alice",  123, 10.0, 20.0, 1, 1, 1)
                .addRow("Carol", 1588, 15.0, 41.0, 3, 3, 3);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void sumWithGroupingByTwoColumns()
    {
        DataFrame summed = this.dataFrame.sumBy(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name", "Foo"));

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
        this.dataFrame.addStringColumn("aFoo", "'a' + Foo");
        this.dataFrame.addLongColumn("BarBar", "Bar * 2");
        this.dataFrame.addDoubleColumn("BazBaz", "Baz * 2");

        DataFrame summed = this.dataFrame.sumBy(Lists.immutable.of("BarBar", "BazBaz"), Lists.immutable.of("Name", "aFoo"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addStringColumn("aFoo").addLongColumn("BarBar").addDoubleColumn("BazBaz")
                .addRow("Bob",   "aDef",  1134L,  48.0)
                .addRow("Bob",   "aAbc",   246L,  88.0)
                .addRow("Alice", "aQqq",   246L,  20.0)
                .addRow("Carol", "aRrr",  3156L,  62.0)
                .addRow("Carol", "aQqq",    20L, 110.0);

        DataFrameUtil.assertEquals(expected, summed);
    }
}
