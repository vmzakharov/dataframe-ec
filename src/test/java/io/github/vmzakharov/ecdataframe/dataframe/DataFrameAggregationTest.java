package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;

import org.junit.jupiter.api.Test;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.*;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*;
import static org.junit.jupiter.api.Assertions.*;

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
    public void sumEmptyDataFrame()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Qux").addFloatColumn("Thud");

        DataFrame summed = df.sum(Lists.immutable.of("Bar", "Qux", "Thud"));

        assertEquals(0L, summed.getLongColumn("Bar").getLong(0));
        assertEquals(0.0, summed.getDoubleColumn("Qux").getDouble(0), TOLERANCE);
        assertEquals(0.0, summed.getDoubleColumn("Thud").getDouble(0), TOLERANCE);
    }

    @Test
    public void sumEmptyDataFrameWithGrouping()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Qux").addFloatColumn("Thud");

        DataFrame summed = df.aggregateBy(
                        Lists.immutable.of(sum("Bar"), sum("Qux"), sum("Thud")),
                        Lists.immutable.of("Name")
                );

        assertTrue(summed.isEmpty());
    }

    @Test
    public void countEmpty()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz")
                .addIntColumn("Waldo").addFloatColumn("Thud");

        DataFrame counted = df.aggregate(Lists.immutable.of(count("Name"), count("Bar"), count("Baz"), count("Waldo"), count("Thud")));

        DataFrameUtil.assertEquals(
                new DataFrame("Counted")
                        .addLongColumn("Name").addLongColumn("Bar").addLongColumn("Baz").addLongColumn("Waldo").addLongColumn("Thud")
                        .addRow(0L, 0L, 0L, 0L, 0L)
                , counted
        );
    }

    @Test
    public void averageEmptyLongThrowsException()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("FrameOfData")
                        .addStringColumn("Name").addLongColumn("Bar")
                        .aggregate(Lists.immutable.of(avg("Bar")))
        );
    }

    @Test
    public void averageEmptyDoubleThrowsException()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("FrameOfData")
                    .addStringColumn("Name").addDoubleColumn("Baz").addDoubleColumn("Qux")
                    .aggregate(Lists.immutable.of(avg("Baz"), avg("Qux")))
        );
    }

    @Test
    public void averageEmptyIntThrowsException()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("FrameOfData")
                        .addStringColumn("Name").addIntColumn("Waldo")
                        .aggregate(Lists.immutable.of(avg("Waldo"))));
    }

    @Test
    public void averageEmptyFloatThrowsException()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("FrameOfData")
                        .addStringColumn("Name").addFloatColumn("Thud")
                        .aggregate(Lists.immutable.of(avg("Thud")))
        );
    }

    @Test
    public void maxEmptyLongThrowsException()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("FrameOfData")
                        .addStringColumn("Name").addLongColumn("Bar")
                        .aggregate(Lists.immutable.of(max("Bar")))
        );
    }

    @Test
    public void maxEmptyDoubleThrowsException()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("FrameOfData")
                        .addStringColumn("Name").addDoubleColumn("Baz").addDoubleColumn("Qux")
                        .aggregate(Lists.immutable.of(max("Baz"), max("Qux")))
        );
    }

    @Test
    public void maxEmptyFloatThrowsException()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("FrameOfData")
                        .addStringColumn("Name").addFloatColumn("Baz")
                        .aggregate(Lists.immutable.of(max("Baz")))
        );
    }

    @Test
    public void maxEmptyIntThrowsException()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("FrameOfData")
                        .addStringColumn("Name").addIntColumn("Waldo")
                        .aggregate(Lists.immutable.of(max("Waldo")))
        );
    }

    @Test
    public void minEmptyLongThrowsException()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("FrameOfData")
                        .addStringColumn("Name").addLongColumn("Bar")
                        .aggregate(Lists.immutable.of(min("Bar")))
        );
    }

    @Test
    public void minEmptyDoubleThrowsException()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("FrameOfData")
                        .addStringColumn("Name").addDoubleColumn("Baz").addDoubleColumn("Qux")
                        .aggregate(Lists.immutable.of(min("Baz"), min("Qux")))
        );
    }

    @Test
    public void minEmptyIntThrowsException()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("FrameOfData")
                        .addStringColumn("Name").addIntColumn("Waldo")
                        .aggregate(Lists.immutable.of(min("Waldo")))
        );
    }

    @Test
    public void minEmptyFloatThrowsException()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("FrameOfData")
                        .addStringColumn("Name").addFloatColumn("Waldo")
                        .aggregate(Lists.immutable.of(min("Waldo")))
        );
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

    @Test
    public void sumNonNumericTriggersError()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc", 123L, 10.0, 20.0)
                .addRow("Bob", "Def", 456L, 12.0, 25.0)
                .addRow("Carol", "Xyz", 789L, 15.0, 40.0);

        assertThrows(RuntimeException.class, () -> df.sum(Lists.immutable.of("Foo", "Bar", "Baz")));
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
                .addIntColumn("Waldo").addFloatColumn("Thud");

        df.addRow("Bob",   "Def", 456L, 12.0, 25.0, 4, 1.25f);
        df.addRow("Bob",   "Abc", 123L, 44.0, 33.0, 2, 2.50f);
        df.addRow("Alice", "Qqq", 123L, 10.0, 20.0, 3, 3.75f);
        df.addRow("Carol", "Rrr", 789L, 15.0, 40.0, 1, 4.25f);
        df.addRow("Bob",   "Def", 111L, 12.0, 25.0, 7, 5.125f);
        df.addRow("Carol", "Qqq",  10L, 55.0, 22.0, 6, 6.00f);
        df.addRow("Carol", "Rrr", 789L, 16.0, 41.0, 8, 8.25f);

        df.addStringColumn("aFoo", "'a' + Foo");
        df.addLongColumn("BarBar", "Bar * 2");
        df.addDoubleColumn("BazBaz", "Baz * 2");
        df.addLongColumn("TwoWaldos", "Waldo + Waldo");
        df.addDoubleColumn("OnePlusThud", "1 + Thud");
        df.addDoubleColumn("ThudThud", "Thud + Thud");

        DataFrame summed = df.sumBy(
                Lists.immutable.of("BarBar", "BazBaz", "TwoWaldos", "OnePlusThud", "ThudThud"),
                Lists.immutable.of("Name", "aFoo"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addStringColumn("aFoo")
                .addLongColumn("BarBar").addDoubleColumn("BazBaz").addLongColumn("TwoWaldos")
                .addDoubleColumn("OnePlusThud").addDoubleColumn("ThudThud")
                .addRow("Bob",   "aDef", 1134L, 48.0, 22,  8.375f, 12.75f)
                .addRow("Bob",   "aAbc",  246L, 88.0,  4,  3.50f,   5.00f)
                .addRow("Alice", "aQqq",  246L, 20.0,  6,  4.75f,   7.50f)
                .addRow("Carol", "aRrr", 3156L, 62.0, 18, 14.50f,  25.00f)
                .addRow("Carol", "aQqq",  20L, 110.0, 12,  7.00f,  12.00f);

        DataFrameUtil.assertEquals(expected, summed);
    }

    @Test
    public void builtInAggregationFunctionNames()
    {
        assertEquals(
            Lists.immutable.of("Avg", "Count", "Max", "Min", "Same", "Sum"),
            Lists.immutable.of(avg("NA"), count("NA"), max("NA"), min("NA"), same("NA"), sum("NA"))
                    .collect(AggregateFunction::getName)
        );
    }

    @Test
    public void builtInAggregationSupportedTypes()
    {
        assertEquals(
                Lists.immutable.of(INT, LONG, DOUBLE, FLOAT, DECIMAL),
                avg("NA").supportedSourceTypes());
        assertEquals(
                Lists.immutable.of(INT, LONG, DOUBLE, FLOAT, STRING, DATE, DATE_TIME, DECIMAL),
                count("NA").supportedSourceTypes());
        assertEquals(
                Lists.immutable.of(INT, LONG, DOUBLE, FLOAT, DECIMAL),
                max("NA").supportedSourceTypes());
        assertEquals(
                Lists.immutable.of(INT, LONG, DOUBLE, FLOAT, DECIMAL),
                min("NA").supportedSourceTypes());
        assertEquals(
                Lists.immutable.of(INT, LONG, DOUBLE, FLOAT, STRING, DATE, DATE_TIME, DECIMAL),
                same("NA").supportedSourceTypes());
        assertEquals(
                Lists.immutable.of(INT, LONG, DOUBLE, FLOAT, DECIMAL),
                sum("NA").supportedSourceTypes());
    }
}
