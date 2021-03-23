package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.junit.Assert;
import org.junit.Test;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.*;

public class DataFrameAggregationWithIndexTest
{
    @Test
    public void sumWithIndex()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc",  123L, 10.0, 20.0)
                .addRow("Bob",   "Def",  456L, 12.0, 25.0)
                .addRow("Alice", "Xyz",  789L, 15.0, 40.0)
                .addRow("Bob",   "Xyz",  789L, 15.0, 40.0)
                ;

        DataFrame summed = dataFrame.sumByWithIndex(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Summed")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",  912L, 25.0, 60.0)
                .addRow("Bob",   1245L, 27.0, 65.0)
                ;

        DataFrameUtil.assertEquals(expected, summed);

        Assert.assertEquals(IntLists.immutable.of(0, 2), summed.getAggregateIndex(0));
        Assert.assertEquals(IntLists.immutable.of(1, 3), summed.getAggregateIndex(1));
    }

    @Test
    public void sumWithIndexOfComputedByComputed()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name1").addStringColumn("Name2").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Al", "ice", "Abc",  123L, 10.0, 20.0)
                .addRow("Bo", "b",   "Def",  456L, 12.0, 25.0)
                .addRow("Al", "ice", "Xyz",  789L, 15.0, 40.0)
                .addRow("Bo", "b",   "Xyz",  789L, 15.0, 40.0)
                ;

        dataFrame.addStringColumn("Name", "Name1 + Name2").addDoubleColumn("Value", "Baz + Qux");

        DataFrame summed = dataFrame.sumByWithIndex(Lists.immutable.of("Bar", "Value"), Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Value")
                .addRow("Alice",  912L, 85.0)
                .addRow("Bob",   1245L, 92.0)
                ;

        DataFrameUtil.assertEquals(expected, summed);

        Assert.assertEquals(IntLists.immutable.of(0, 2), summed.getAggregateIndex(0));
        Assert.assertEquals(IntLists.immutable.of(1, 3), summed.getAggregateIndex(1));
    }

    @Test
    public void aggregateWithIndex()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc",  123L, 10.0, 20.0)
                .addRow("Bob",   "Def",  456L, 12.0, 25.0)
                .addRow("Alice", "Xyz",  789L, 15.0, 40.0)
                .addRow("Bob",   "Xyz",  789L, 15.0, 40.0)
                ;

        DataFrame summed = dataFrame.aggregateByWithIndex(
                Lists.immutable.of(sum("Bar"), sum("Baz"), sum("Qux")), Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Summed")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",  912L, 25.0, 60.0)
                .addRow("Bob",   1245L, 27.0, 65.0)
                ;

        DataFrameUtil.assertEquals(expected, summed);

        Assert.assertEquals(IntLists.immutable.of(0, 2), summed.getAggregateIndex(0));
        Assert.assertEquals(IntLists.immutable.of(1, 3), summed.getAggregateIndex(1));
    }

    @Test
    public void aggregateSameColumnWithIndex()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc",  123L, 10.0, 20.0)
                .addRow("Bob",   "Def",  456L, 12.0, 25.0)
                .addRow("Alice", "Xyz",  789L, 15.0, 40.0)
                .addRow("Bob",   "Xyz",  789L, 15.0, 40.0)
                ;

        DataFrame summed = dataFrame.aggregateByWithIndex(
                Lists.immutable.of(sum("Bar", "BarSum"), min("Bar", "BarMin"), sum("Qux", "QuxSum"), max("Qux", "QuxMax")),
                Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Summed")
                .addStringColumn("Name").addLongColumn("BarSum").addLongColumn("BarMin").addDoubleColumn("QuxSum").addDoubleColumn("QuxMax")
                .addRow("Alice",  912L, 123L, 60.0, 40.0)
                .addRow("Bob",   1245L, 456L, 65.0, 40.0)
                ;

        DataFrameUtil.assertEquals(expected, summed);

        Assert.assertEquals(IntLists.immutable.of(0, 2), summed.getAggregateIndex(0));
        Assert.assertEquals(IntLists.immutable.of(1, 3), summed.getAggregateIndex(1));
    }

    @Test
    public void indexIsSortedWhenAggregateIsSorted()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Bob",   "Def",  456L, 12.0, 25.0)
                .addRow("Alice", "Abc",  123L, 10.0, 20.0)
                .addRow("Bob",   "Xyz",  789L, 15.0, 40.0)
                .addRow("Alice", "Xyz",  789L, 15.0, 40.0)
                ;

        DataFrame summed = dataFrame.sumByWithIndex(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Summed")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Bob",   1245L, 27.0, 65.0)
                .addRow("Alice",  912L, 25.0, 60.0)
                ;

        DataFrameUtil.assertEquals(expected, summed);

        Assert.assertEquals(IntLists.immutable.of(0, 2), summed.getAggregateIndex(0));
        Assert.assertEquals(IntLists.immutable.of(1, 3), summed.getAggregateIndex(1));

        summed.sortBy(Lists.immutable.of("Name"));

        DataFrame expectedSortedAgg = new DataFrame("Summed")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",  912L, 25.0, 60.0)
                .addRow("Bob",   1245L, 27.0, 65.0)
                ;

        DataFrameUtil.assertEquals(expectedSortedAgg, summed);

        Assert.assertEquals(IntLists.immutable.of(1, 3), summed.getAggregateIndex(0));
        Assert.assertEquals(IntLists.immutable.of(0, 2), summed.getAggregateIndex(1));
    }

    @Test
    public void sumWithIndexWhenNothingGetsSummed()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc",  123L, 10.0, 20.0)
                .addRow("Bob",   "Def",  456L, 12.0, 25.0)
                .addRow("Carl",  "Xyz",  789L, 15.0, 40.0)
                .addRow("Doris", "Xyz",  678L, 25.0, 43.0)
                ;

        DataFrame summed = dataFrame.sumByWithIndex(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Summed")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", 123L, 10.0, 20.0)
                .addRow("Bob",   456L, 12.0, 25.0)
                .addRow("Carl",  789L, 15.0, 40.0)
                .addRow("Doris", 678L, 25.0, 43.0)
                ;

        DataFrameUtil.assertEquals(expected, summed);

        Assert.assertEquals(IntLists.immutable.of(0), summed.getAggregateIndex(0));
        Assert.assertEquals(IntLists.immutable.of(1), summed.getAggregateIndex(1));
        Assert.assertEquals(IntLists.immutable.of(2), summed.getAggregateIndex(2));
        Assert.assertEquals(IntLists.immutable.of(3), summed.getAggregateIndex(3));
    }

    @Test
    public void sumWithIndexWhenEverythingIsSummed()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc",  123L, 10.0, 20.0)
                .addRow("Alice", "Def",  456L, 12.0, 25.0)
                .addRow("Alice", "Xyz",  789L, 15.0, 40.0)
                .addRow("Alice", "Xyz",  678L, 25.0, 43.0)
                ;

        DataFrame summed = dataFrame.sumByWithIndex(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("Summed")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", 2046L, 62.0, 128.0)
                ;

        DataFrameUtil.assertEquals(expected, summed);

        Assert.assertEquals(IntLists.immutable.of(0, 1, 2, 3), summed.getAggregateIndex(0));
    }
}
