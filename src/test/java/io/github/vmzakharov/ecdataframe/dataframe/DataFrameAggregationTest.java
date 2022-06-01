package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.avg;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.count;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.max;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.min;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.same;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.sum;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DATE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DATE_TIME;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DOUBLE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.LONG;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;

public class DataFrameAggregationTest
{
    private static final double TOLERANCE = 0.00001;

    private DataFrame dataFrame;

    @Before
    public void initialiseDataFrame()
    {
        this.dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc", 123L, 10.0, 100.0)
                .addRow("Bob", "Def", 456L, 12.0, -25.0)
                .addRow("Carol", "Xyz", -789L, 17.0, 42.0);
    }

    @Test
    public void sumAll()
    {
        DataFrame expected = new DataFrame("sum")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow(-210L, 39.0, 117.0);

        DataFrameUtil.assertEquals(expected, this.dataFrame.sum(Lists.immutable.of("Bar", "Baz", "Qux")));
        DataFrameUtil.assertEquals(expected, this.dataFrame.aggregate(Lists.immutable.of(sum("Bar"), sum("Baz"), sum("Qux"))));
    }

    @Test
    public void minAll()
    {
        DataFrame expected = new DataFrame("sum")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow(-789L, 10.0, -25.0);

        DataFrameUtil.assertEquals(expected, this.dataFrame.aggregate(Lists.immutable.of(min("Bar"), min("Baz"), min("Qux"))));
    }

    @Test
    public void maxAll()
    {
        DataFrame expected = new DataFrame("sum")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow(456L, 17.0, 100.0);

        DataFrameUtil.assertEquals(expected, this.dataFrame.aggregate(Lists.immutable.of(max("Bar"), max("Baz"), max("Qux"))));
    }

    @Test
    public void averageAll()
    {
        DataFrame expected = new DataFrame("sum")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow(-70L, 13.0, 39.0);

        DataFrameUtil.assertEquals(expected, this.dataFrame.aggregate(Lists.immutable.of(avg("Bar"), avg("Baz"), avg("Qux"))));
    }

    @Test
    public void countAll()
    {
        DataFrame expected = new DataFrame("sum")
                .addLongColumn("Name").addLongColumn("Bar").addLongColumn("Baz").addLongColumn("Qux")
                .addRow(3L, 3L, 3L, 3L);

        DataFrameUtil.assertEquals(expected, this.dataFrame.aggregate(Lists.immutable.of(count("Name"), count("Bar"), count("Baz"), count("Qux"))));
    }

    @Test
    public void differentAggregationsAll()
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
    public void maxOfNegativeNumbersWithGrouping()
    {
        DataFrame df = new DataFrame("Frame-o-data")
                .addStringColumn("Key").addLongColumn("long").addDoubleColumn("double")
                .addRow("A", -1, -20.2)
                .addRow("A", -10, -15.5)
                .addRow("B", -15, -25.25)
                .addRow("B", -17, -45.45);

        DataFrameUtil.assertEquals(
                new DataFrame("expected maxes")
                        .addStringColumn("Key").addLongColumn("long").addDoubleColumn("double")
                        .addRow("A", -1, -15.5)
                        .addRow("B", -15, -25.25),
                df.aggregateBy(Lists.immutable.of(max("long"), max("double")), Lists.immutable.of("Key")));
    }

    @Test
    public void sameColumnDifferentAggregationsAll()
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
    public void sumEmpty()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux");

        DataFrame summed = df.sum(Lists.immutable.of("Bar", "Baz", "Qux"));

        Assert.assertEquals(0L, summed.getLongColumn("Bar").getLong(0));
        Assert.assertEquals(0.0, summed.getDoubleColumn("Baz").getDouble(0), TOLERANCE);
        Assert.assertEquals(0.0, summed.getDoubleColumn("Qux").getDouble(0), TOLERANCE);
    }

    @Test(expected = RuntimeException.class)
    public void averageEmptyThrowsException()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux");

        df.aggregate(Lists.immutable.of(avg("Bar"), avg("Baz"), avg("Qux")));
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

    @Test
    public void sumEmptyWithCalculatedColumns()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz")
                .addDoubleColumn("BazBaz", "Baz * 2").addLongColumn("BarBar", "Bar * 2");

        DataFrame summed = df.sum(Lists.immutable.of("Bar", "Baz", "BazBaz", "BarBar"));

        DataFrame expected = new DataFrame("Sum of FrameOfData")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("BazBaz").addLongColumn("BarBar")
                .addRow(0L, 0.0, 0.0, 0L);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test(expected = RuntimeException.class)
    public void sumNonNumericTriggersError()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc", 123L, 10.0, 20.0)
                .addRow("Bob", "Def", 456L, 12.0, 25.0)
                .addRow("Carol", "Xyz", 789L, 15.0, 40.0);

        DataFrame summed = df.sum(Lists.immutable.of("Foo", "Bar", "Baz"));

        Assert.fail("Shouldn't get to this line");
    }

    @Test
    public void sumWithGroupingByTwoColumns()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux");

        df.addRow("Bob", "Def", 456L, 12.0, 25.0);
        df.addRow("Bob", "Abc", 123L, 44.0, 33.0);
        df.addRow("Alice", "Qqq", 123L, 10.0, 20.0);
        df.addRow("Carol", "Rrr", 789L, 15.0, 40.0);
        df.addRow("Bob", "Def", 111L, 12.0, 25.0);
        df.addRow("Carol", "Qqq", 10L, 55.0, 22.0);
        df.addRow("Carol", "Rrr", 789L, 16.0, 41.0);

        DataFrame summed = df.sumBy(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name", "Foo"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Bob", "Def", 567L, 24.0, 50.0)
                .addRow("Bob", "Abc", 123L, 44.0, 33.0)
                .addRow("Alice", "Qqq", 123L, 10.0, 20.0)
                .addRow("Carol", "Rrr", 1578L, 31.0, 81.0)
                .addRow("Carol", "Qqq", 10L, 55.0, 22.0);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void sumOfAndByCalculatedColumns()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux");

        df.addRow("Bob", "Def", 456L, 12.0, 25.0);
        df.addRow("Bob", "Abc", 123L, 44.0, 33.0);
        df.addRow("Alice", "Qqq", 123L, 10.0, 20.0);
        df.addRow("Carol", "Rrr", 789L, 15.0, 40.0);
        df.addRow("Bob", "Def", 111L, 12.0, 25.0);
        df.addRow("Carol", "Qqq", 10L, 55.0, 22.0);
        df.addRow("Carol", "Rrr", 789L, 16.0, 41.0);

        df.addStringColumn("aFoo", "'a' + Foo");
        df.addLongColumn("BarBar", "Bar * 2");
        df.addDoubleColumn("BazBaz", "Baz * 2");

        DataFrame summed = df.sumBy(Lists.immutable.of("BarBar", "BazBaz"), Lists.immutable.of("Name", "aFoo"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addStringColumn("aFoo").addLongColumn("BarBar").addDoubleColumn("BazBaz")
                .addRow("Bob", "aDef", 1134L, 48.0)
                .addRow("Bob", "aAbc", 246L, 88.0)
                .addRow("Alice", "aQqq", 246L, 20.0)
                .addRow("Carol", "aRrr", 3156L, 62.0)
                .addRow("Carol", "aQqq", 20L, 110.0);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void builtInAggregationFunctionNames()
    {
        Assert.assertEquals(
            Lists.immutable.of("Avg", "Count", "Max", "Min", "Same", "Sum"),
            Lists.immutable.of(avg("NA"), count("NA"), max("NA"), min("NA"), same("NA"), sum("NA"))
                    .collect(AggregateFunction::getName)
        );
    }

    @Test
    public void builtInAggregationSupportedTypes()
    {
        Assert.assertEquals(Lists.immutable.of(LONG, DOUBLE), avg("NA").supportedSourceTypes());
        Assert.assertEquals(Lists.immutable.of(LONG, DOUBLE, STRING, DATE, DATE_TIME), count("NA").supportedSourceTypes());
        Assert.assertEquals(Lists.immutable.of(LONG, DOUBLE), max("NA").supportedSourceTypes());
        Assert.assertEquals(Lists.immutable.of(LONG, DOUBLE), min("NA").supportedSourceTypes());
        Assert.assertEquals(Lists.immutable.of(LONG, DOUBLE, STRING, DATE, DATE_TIME), same("NA").supportedSourceTypes());
        Assert.assertEquals(Lists.immutable.of(LONG, DOUBLE), sum("NA").supportedSourceTypes());
    }
}
