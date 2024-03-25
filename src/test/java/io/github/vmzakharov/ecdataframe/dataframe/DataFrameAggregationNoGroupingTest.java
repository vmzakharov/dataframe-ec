package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.Before;
import org.junit.Test;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.*;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.sum;

public class DataFrameAggregationNoGroupingTest
{
    private DataFrame dataFrame;

    @Before
    public void initialiseDataFrame()
    {
        this.dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz")
                .addDoubleColumn("Qux").addIntColumn("Waldo").addFloatColumn("Thud")
                .addRow("Alice", "Abc",  123L, 10.0, 100.0,  1, -10.5f)
                .addRow("Bob",   "Def",  456L, 12.0, -25.0, -2,  20.5f)
                .addRow("Carol", "Xyz", -789L, 17.0,  42.0,  5,  12.5f);
    }

    @Test
    public void sumAll()
    {
        DataFrame expected = new DataFrame("sum")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addLongColumn("Waldo").addDoubleColumn("Thud")
                .addRow(-210L, 39.0, 117.0, 4L, 22.5);

        DataFrameUtil.assertEquals(expected,
                this.dataFrame.sum(Lists.immutable.of("Bar", "Baz", "Qux", "Waldo", "Thud")));

        DataFrameUtil.assertEquals(expected,
                this.dataFrame.aggregate(Lists.immutable.of(sum("Bar"), sum("Baz"), sum("Qux"), sum("Waldo"), sum("Thud"))));
    }

    @Test
    public void minAll()
    {
        DataFrame expected = new DataFrame("min")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addIntColumn("Waldo").addFloatColumn("Thud")
                .addRow(-789L, 10.0, -25.0, -2, -10.5f);

        DataFrameUtil.assertEquals(
                expected,
                this.dataFrame.aggregate(Lists.immutable.of(min("Bar"), min("Baz"), min("Qux"), min("Waldo"), min("Thud"))));
    }

    @Test
    public void maxAll()
    {
        DataFrame expected = new DataFrame("max")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addIntColumn("Waldo").addFloatColumn("Thud")
                .addRow(456L, 17.0, 100.0, 5, 20.5f);

        DataFrameUtil.assertEquals(
                expected,
                this.dataFrame.aggregate(Lists.immutable.of(max("Bar"), max("Baz"), max("Qux"), max("Waldo"), max("Thud"))));
    }

    @Test
    public void averageAll()
    {
        DataFrame expected = new DataFrame("sum")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addIntColumn("Waldo").addFloatColumn("Thud")
                .addRow(-70L, 13.0, 39.0, 1, 7.5f);

        DataFrameUtil.assertEquals(
                expected,
                this.dataFrame.aggregate(Lists.immutable.of(avg("Bar"), avg("Baz"), avg("Qux"), avg("Waldo"), avg("Thud"))));
    }

    @Test
    public void countAll()
    {
        DataFrame expected = new DataFrame("sum")
                .addLongColumn("Name").addLongColumn("Bar").addLongColumn("Baz").addLongColumn("Qux").addLongColumn("Waldo").addLongColumn("Thud")
                .addRow(3L, 3L, 3L, 3L, 3L, 3L);

        DataFrameUtil.assertEquals(
                expected,
                this.dataFrame.aggregate(Lists.immutable.of(count("Name"), count("Bar"), count("Baz"), count("Qux"), count("Waldo"), count("Thud"))));
    }

    @Test
    public void differentAggregations()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("variety")
                        .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                        .addRow(-210L, 10.0, 100.0),
                this.dataFrame.aggregate(Lists.immutable.of(sum("Bar"), min("Baz"), max("Qux"))));

        DataFrameUtil.assertEquals(
                new DataFrame("variety")
                        .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                        .addRow(456L, 13.0, 117.0),
                this.dataFrame.aggregate(Lists.immutable.of(max("Bar"), avg("Baz"), sum("Qux"))));
    }

    @Test
    public void sameColumnDifferentAggregations()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("variety")
                        .addLongColumn("BarSum").addLongColumn("BarMin").addLongColumn("BarMax")
                        .addRow(-210L, -789, 456),
                this.dataFrame.aggregate(Lists.immutable.of(sum("Bar", "BarSum"), min("Bar", "BarMin"), max("Bar", "BarMax"))));

        DataFrameUtil.assertEquals(
                new DataFrame("more variety")
                        .addDoubleColumn("BazSum1").addDoubleColumn("BazSum2").addDoubleColumn("QuxMin").addDoubleColumn("QuxMax").addDoubleColumn("QuxAvg")
                        .addRow(39.0, 39.0, -25.0, 100.0, 39.0),
                this.dataFrame.aggregate(Lists.immutable.of(
                        sum("Baz", "BazSum1"), sum("Baz", "BazSum2"), min("Qux", "QuxMin"), max("Qux", "QuxMax"), avg("Qux", "QuxAvg"))));
    }

    @Test
    public void aggregationsAllWithCalculatedColumns()
    {
        this.dataFrame.addLongColumn("BarBar", "Bar * 2");
        this.dataFrame.addDoubleColumn("BazBaz", "Baz * 2");

        DataFrame expectedSum = new DataFrame("Sum of FrameOfData")
                .addLongColumn("Bar").addDoubleColumn("Baz").addLongColumn("BarBar").addDoubleColumn("BazBaz")
                .addRow(-210L, 39.0, -420L, 78.0);

        DataFrameUtil.assertEquals(expectedSum, this.dataFrame.sum(Lists.immutable.of("Bar", "Baz", "BarBar", "BazBaz")));
        DataFrameUtil.assertEquals(expectedSum, this.dataFrame.aggregate(Lists.immutable.of(sum("Bar"), sum("Baz"), sum("BarBar"), sum("BazBaz"))));

        DataFrame expectedMin = new DataFrame("Min of FrameOfData")
                .addLongColumn("Bar").addDoubleColumn("Baz").addLongColumn("BarBar").addDoubleColumn("BazBaz")
                .addRow(-789L, 10.0, -1578L, 20.0);

        DataFrameUtil.assertEquals(expectedMin, this.dataFrame.aggregate(Lists.immutable.of(min("Bar"), min("Baz"), min("BarBar"), min("BazBaz"))));

        DataFrame expectedMax = new DataFrame("Max of FrameOfData")
                .addLongColumn("Bar").addDoubleColumn("Baz").addLongColumn("BarBar").addDoubleColumn("BazBaz")
                .addRow(456L, 17.0, 912L, 34.0);

        DataFrameUtil.assertEquals(expectedMax, this.dataFrame.aggregate(Lists.immutable.of(max("Bar"), max("Baz"), max("BarBar"), max("BazBaz"))));

        DataFrame expectedAvg = new DataFrame("Avg of FrameOfData")
                .addLongColumn("Bar").addDoubleColumn("Baz").addLongColumn("BarBar").addDoubleColumn("BazBaz")
                .addRow(-70L, 13.0, -140L, 26.0);

        DataFrameUtil.assertEquals(expectedAvg, this.dataFrame.aggregate(Lists.immutable.of(avg("Bar"), avg("Baz"), avg("BarBar"), avg("BazBaz"))));
    }
}
