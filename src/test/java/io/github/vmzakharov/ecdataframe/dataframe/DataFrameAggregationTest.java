package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Test;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.*;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*;

public class DataFrameAggregationTest
{
    private static final double TOLERANCE = 0.00001;

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
    public void sumEmpty()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux");

        DataFrame summed = df.sum(Lists.immutable.of("Bar", "Baz", "Qux"));

        Assert.assertEquals(0L, summed.getLongColumn("Bar").getLong(0));
        Assert.assertEquals(0.0, summed.getDoubleColumn("Baz").getDouble(0), TOLERANCE);
        Assert.assertEquals(0.0, summed.getDoubleColumn("Qux").getDouble(0), TOLERANCE);
    }

    @Test
    public void countEmpty()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addIntColumn("Waldo");

        DataFrame counted = df.aggregate(Lists.immutable.of(count("Name"), count("Bar"), count("Baz"), count("Waldo")));

        DataFrameUtil.assertEquals(
                new DataFrame("Counted")
                        .addLongColumn("Name").addLongColumn("Bar").addLongColumn("Baz").addLongColumn("Waldo")
                        .addRow(0L, 0L, 0L, 0L)
                , counted
        );
    }

    @Test(expected = RuntimeException.class)
    public void averageEmptyLongThrowsException()
    {
        new DataFrame("FrameOfData").addStringColumn("Name").addLongColumn("Bar")
                                    .aggregate(Lists.immutable.of(avg("Bar")));
    }

    @Test(expected = RuntimeException.class)
    public void averageEmptyDoubleThrowsException()
    {
        new DataFrame("FrameOfData").addStringColumn("Name").addDoubleColumn("Baz").addDoubleColumn("Qux")
                                    .aggregate(Lists.immutable.of(avg("Baz"), avg("Qux")));
    }

    @Test(expected = RuntimeException.class)
    public void averageEmptyIntThrowsException()
    {
        new DataFrame("FrameOfData").addStringColumn("Name").addIntColumn("Waldo")
                                    .aggregate(Lists.immutable.of(avg("Waldo")));
    }

    @Test(expected = RuntimeException.class)
    public void maxEmptyLongThrowsException()
    {
        new DataFrame("FrameOfData").addStringColumn("Name").addLongColumn("Bar")
                                    .aggregate(Lists.immutable.of(max("Bar")));
    }

    @Test(expected = RuntimeException.class)
    public void maxEmptyDoubleThrowsException()
    {
        new DataFrame("FrameOfData").addStringColumn("Name").addDoubleColumn("Baz").addDoubleColumn("Qux")
                                    .aggregate(Lists.immutable.of(max("Baz"), max("Qux")));
    }

    @Test(expected = RuntimeException.class)
    public void maxEmptyIntThrowsException()
    {
        new DataFrame("FrameOfData").addStringColumn("Name").addIntColumn("Waldo")
                                    .aggregate(Lists.immutable.of(max("Waldo")));
    }

    @Test(expected = RuntimeException.class)
    public void minEmptyLongThrowsException()
    {
        new DataFrame("FrameOfData").addStringColumn("Name").addLongColumn("Bar")
                                    .aggregate(Lists.immutable.of(min("Bar")));
    }

    @Test(expected = RuntimeException.class)
    public void minEmptyDoubleThrowsException()
    {
        new DataFrame("FrameOfData").addStringColumn("Name").addDoubleColumn("Baz").addDoubleColumn("Qux")
                                    .aggregate(Lists.immutable.of(min("Baz"), min("Qux")));
    }

    @Test(expected = RuntimeException.class)
    public void minEmptyIntThrowsException()
    {
        new DataFrame("FrameOfData").addStringColumn("Name").addIntColumn("Waldo")
                                    .aggregate(Lists.immutable.of(min("Waldo")));
    }

    @Test
    public void sumEmptyWithCalculatedColumns()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz")
                .addDoubleColumn("BazBaz", "Baz * 2").addLongColumn("BarBar", "Bar * 2").addIntColumn("Waldo");

        DataFrame summed = df.sum(Lists.immutable.of("Bar", "Baz", "BazBaz", "BarBar", "Waldo"));

        DataFrame expected = new DataFrame("Sum of FrameOfData")
                .addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("BazBaz").addLongColumn("BarBar").addLongColumn("Waldo")
                .addRow(0L, 0.0, 0.0, 0L, 0L);

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
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar")
                .addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addIntColumn("Waldo");

        df.addRow("Bob",   "Def", 456L, 12.0, 25.0, 4);
        df.addRow("Bob",   "Abc", 123L, 44.0, 33.0, 2);
        df.addRow("Alice", "Qqq", 123L, 10.0, 20.0, 3);
        df.addRow("Carol", "Rrr", 789L, 15.0, 40.0, 1);
        df.addRow("Bob",   "Def", 111L, 12.0, 25.0, 7);
        df.addRow("Carol", "Qqq",  10L, 55.0, 22.0, 6);
        df.addRow("Carol", "Rrr", 789L, 16.0, 41.0, 8);

        df.addStringColumn("aFoo", "'a' + Foo");
        df.addLongColumn("BarBar", "Bar * 2");
        df.addDoubleColumn("BazBaz", "Baz * 2");
        df.addLongColumn("TwoWaldos", "Waldo + Waldo");

        DataFrame summed = df.sumBy(
                Lists.immutable.of("BarBar", "BazBaz", "TwoWaldos"),
                Lists.immutable.of("Name", "aFoo"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addStringColumn("aFoo")
                .addLongColumn("BarBar").addDoubleColumn("BazBaz").addLongColumn("TwoWaldos")
                .addRow("Bob",   "aDef", 1134L, 48.0, 22)
                .addRow("Bob",   "aAbc",  246L, 88.0,  4)
                .addRow("Alice", "aQqq",  246L, 20.0,  6)
                .addRow("Carol", "aRrr", 3156L, 62.0, 18)
                .addRow("Carol", "aQqq",  20L, 110.0, 12);

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
        Assert.assertEquals(Lists.immutable.of(LONG, DOUBLE, DECIMAL), avg("NA").supportedSourceTypes());
        Assert.assertEquals(Lists.immutable.of(INT, LONG, DOUBLE, STRING, DATE, DATE_TIME, DECIMAL), count("NA").supportedSourceTypes());
        Assert.assertEquals(Lists.immutable.of(INT, LONG, DOUBLE, DECIMAL), max("NA").supportedSourceTypes());
        Assert.assertEquals(Lists.immutable.of(INT, LONG, DOUBLE, DECIMAL), min("NA").supportedSourceTypes());
        Assert.assertEquals(Lists.immutable.of(INT, LONG, DOUBLE, STRING, DATE, DATE_TIME, DECIMAL), same("NA").supportedSourceTypes());
        Assert.assertEquals(Lists.immutable.of(INT, LONG, DOUBLE, DECIMAL), sum("NA").supportedSourceTypes());
    }
}
