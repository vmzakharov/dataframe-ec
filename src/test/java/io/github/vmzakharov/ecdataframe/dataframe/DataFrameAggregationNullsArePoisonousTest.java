package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.Test;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.avg;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.count;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.max;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.min;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.sum;

public class DataFrameAggregationNullsArePoisonousTest
{
    @Test
    public void aggregationsAllWithCalculatedColumnsWithNulls()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc",  123L, 10.0, 100.0)
                .addRow("Bob",   "Def",  null, 12.0, -25.0)
                .addRow("Carol", "Xyz", -789L, null,  42.0);

        dataFrame.addLongColumn("BarBar", "Bar * 2");
        dataFrame.addDoubleColumn("BazBaz", "Baz * 2");

        DataFrame expectedSum = new DataFrame("Sum of FrameOfData")
                .addLongColumn("Bar").addDoubleColumn("Baz").addLongColumn("BarBar").addDoubleColumn("BazBaz").addDoubleColumn("Qux")
                .addRow(null, null, null, null, 117.0);

        DataFrameUtil.assertEquals(expectedSum, dataFrame.sum(Lists.immutable.of("Bar", "Baz", "BarBar", "BazBaz", "Qux")));
        DataFrameUtil.assertEquals(
                expectedSum,
                dataFrame.aggregate(Lists.immutable.of(sum("Bar"), sum("Baz"), sum("BarBar"), sum("BazBaz"), sum("Qux"))));

        DataFrameUtil.assertEquals(
            new DataFrame("Min of FrameOfData")
                    .addLongColumn("Bar").addDoubleColumn("Baz").addLongColumn("BarBar").addDoubleColumn("BazBaz").addDoubleColumn("Qux")
                    .addRow(null, null, null, null, -25.0),
            dataFrame.aggregate(Lists.immutable.of(min("Bar"), min("Baz"), min("BarBar"), min("BazBaz"), min("Qux"))));

        DataFrameUtil.assertEquals(
            new DataFrame("Max of FrameOfData")
                .addLongColumn("Bar").addDoubleColumn("Baz").addLongColumn("BarBar").addDoubleColumn("BazBaz").addDoubleColumn("Qux")
                .addRow(null, null, null, null, 100.0),
            dataFrame.aggregate(Lists.immutable.of(max("Bar"), max("Baz"), max("BarBar"), max("BazBaz"), max("Qux"))));

        DataFrameUtil.assertEquals(
            new DataFrame("Avg of FrameOfData")
                .addLongColumn("Bar").addDoubleColumn("Baz").addLongColumn("BarBar").addDoubleColumn("BazBaz").addDoubleColumn("Qux")
                .addRow(null, null, null, null, 39.0),
            dataFrame.aggregate(Lists.immutable.of(avg("Bar"), avg("Baz"), avg("BarBar"), avg("BazBaz"), avg("Qux"))));

        DataFrameUtil.assertEquals(
            new DataFrame("Count of FrameOfData")
                .addLongColumn("Bar").addLongColumn("Baz").addLongColumn("BarBar").addLongColumn("BazBaz").addLongColumn("Qux")
                .addRow(3, 3, 3, 3, 3),
            dataFrame.aggregate(Lists.immutable.of(count("Bar"), count("Baz"), count("BarBar"), count("BazBaz"), count("Qux"))));
    }

    @Test
    public void sumOfAndByCalculatedColumnsWithNulls()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux");

        df.addRow("Bob",   "Def",  456L, null, 25.0);
        df.addRow("Bob",    null,  123L, 45.0, 33.0);
        df.addRow("Bob",    null,  null, 44.0, 33.0);
        df.addRow("Alice", "Qqq",  123L, 10.0, 20.0);
        df.addRow("Carol", "Rrr",  null, 15.0, 40.0);
        df.addRow("Bob",   "Def",  111L, 12.0, 25.0);
        df.addRow("Carol", "Qqq",   10L, 55.0, 22.0);
        df.addRow("Carol", "Rrr",  789L, 16.0, 41.0);

        df.addStringColumn("aFoo", "'a' + Foo");
        df.addLongColumn("BarBar", "Bar * 2");
        df.addDoubleColumn("BazBaz", "Baz * 2");

        DataFrame summed = df.sumBy(Lists.immutable.of("BarBar", "BazBaz"), Lists.immutable.of("Name", "aFoo"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addStringColumn("aFoo").addLongColumn("BarBar").addDoubleColumn("BazBaz")
                .addRow("Bob",   "aDef",  1134L,  null)
                .addRow("Bob",    null,    null, 178.0)
                .addRow("Alice", "aQqq",   246L,  20.0)
                .addRow("Carol", "aRrr",   null,  62.0)
                .addRow("Carol", "aQqq",    20L, 110.0);

        DataFrameUtil.assertEquals(expected, summed);
    }
}
