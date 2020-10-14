package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.junit.Assert;
import org.junit.Test;

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

        Pair<DataFrame, MutableList<MutableIntList>> result = dataFrame.sumByWithIndex(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        DataFrame summed = result.getOne();
        MutableList<MutableIntList> sumIndex = result.getTwo();

        DataFrame expected = new DataFrame("Summed")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",  912L, 25.0, 60.0)
                .addRow("Bob",   1245L, 27.0, 65.0)
                ;

        DataFrameUtil.assertEquals(expected, summed);

        Assert.assertEquals(IntLists.immutable.of(0, 2), sumIndex.get(0));
        Assert.assertEquals(IntLists.immutable.of(1, 3), sumIndex.get(1));
    }

    @Test
    public void sumWithIndexOnSortedDataFrame()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Bob",   "Def",  456L, 12.0, 25.0)
                .addRow("Alice", "Abc",  123L, 10.0, 20.0)
                .addRow("Bob",   "Xyz",  789L, 15.0, 40.0)
                .addRow("Alice", "Xyz",  789L, 15.0, 40.0)
                ;

        dataFrame.sortBy(Lists.immutable.of("Name"));

        Pair<DataFrame, MutableList<MutableIntList>> result = dataFrame.sumByWithIndex(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        DataFrame summed = result.getOne();
        MutableList<MutableIntList> sumIndex = result.getTwo();

        DataFrame expected = new DataFrame("Summed")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",  912L, 25.0, 60.0)
                .addRow("Bob",   1245L, 27.0, 65.0)
                ;

        DataFrameUtil.assertEquals(expected, summed);

        Assert.assertEquals(IntLists.immutable.of(0, 1), sumIndex.get(0));
        Assert.assertEquals(IntLists.immutable.of(2, 3), sumIndex.get(1));
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


        Pair<DataFrame, MutableList<MutableIntList>> result = dataFrame.sumByWithIndex(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        DataFrame summed = result.getOne();
        MutableList<MutableIntList> sumIndex = result.getTwo();

        DataFrame expected = new DataFrame("Summed")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", 123L, 10.0, 20.0)
                .addRow("Bob",   456L, 12.0, 25.0)
                .addRow("Carl",  789L, 15.0, 40.0)
                .addRow("Doris", 678L, 25.0, 43.0)
                ;

        DataFrameUtil.assertEquals(expected, summed);

        Assert.assertEquals(IntLists.immutable.of(0), sumIndex.get(0));
        Assert.assertEquals(IntLists.immutable.of(1), sumIndex.get(1));
        Assert.assertEquals(IntLists.immutable.of(2), sumIndex.get(2));
        Assert.assertEquals(IntLists.immutable.of(3), sumIndex.get(3));
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

        Pair<DataFrame, MutableList<MutableIntList>> result = dataFrame.sumByWithIndex(Lists.immutable.of("Bar", "Baz", "Qux"), Lists.immutable.of("Name"));

        DataFrame summed = result.getOne();
        MutableList<MutableIntList> sumIndex = result.getTwo();

        DataFrame expected = new DataFrame("Summed")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", 2046L, 62.0, 128.0)
                ;

        DataFrameUtil.assertEquals(expected, summed);

        Assert.assertEquals(IntLists.immutable.of(0, 1, 2, 3), sumIndex.get(0));
    }
}
