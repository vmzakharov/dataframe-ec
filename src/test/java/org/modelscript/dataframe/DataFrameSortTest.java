package org.modelscript.dataframe;

import org.eclipse.collections.api.factory.Lists;
import org.junit.Test;

public class DataFrameSortTest
{
    @Test
    public void simpleOneColumnSort()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Abc",  11L, 10.0, 20.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                .addRow("Albert",  "Def",  12L, 12.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0);

        dataFrame.sortBy(Lists.immutable.of("Name"));

        DataFrame expected = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Abigail", "Def",  15L, 15.0, 11.0)
                .addRow("Albert",  "Def",  12L, 12.0, 10.0)
                .addRow("Alice",   "Abc",  11L, 10.0, 20.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                ;

        DataFrameUtil.assertEquals(expected, dataFrame);

        dataFrame.sortBy(Lists.immutable.of("Foo"));

        expected = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Abc",  11L, 10.0, 20.0)
                .addRow("Albert",  "Def",  12L, 12.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                ;

        DataFrameUtil.assertEquals(expected, dataFrame);

        dataFrame.unsort();

        DataFrame expectedUnsorted = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Abc",  11L, 10.0, 20.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                .addRow("Albert",  "Def",  12L, 12.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0);

        DataFrameUtil.assertEquals(expectedUnsorted, dataFrame);
    }

    @Test
    public void multiColumnSort()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Abigail", "456",  11L, 10.0, 20.0)
                .addRow("Carol",   "Xyz",  15L, 28.0, 40.0)
                .addRow("Abigail", "123",  15L, 15.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Zzz",  14L, 14.0, 40.0)
                .addRow("Abigail", "789",  12L, 12.0, 11.0);

        this.printDataFrame("Original", dataFrame);

        dataFrame.sortBy(Lists.immutable.of("Name", "Bar"));

        this.printDataFrame("Sorted", dataFrame);

        DataFrame expected = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Abigail", "456",  11L, 10.0, 20.0)
                .addRow("Abigail", "789",  12L, 12.0, 11.0)
                .addRow("Abigail", "123",  15L, 15.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Zzz",  14L, 14.0, 40.0)
                .addRow("Carol",   "Xyz",  15L, 28.0, 40.0)
                ;

        DataFrameUtil.assertEquals(expected, dataFrame);
    }

    @Test
    public void sortByComputedColumns()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Abigail", "456",  11L, 10.0, 20.0)
                .addRow("Carol",   "Xyz",  15L, 28.0, 40.0)
                .addRow("Abigail", "123",  15L, 15.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Zzz",  14L, 14.0, 40.0)
                .addRow("Abigail", "789",  12L, 12.0, 11.0);
        dataFrame.addDoubleColumn("Quuz", "Bar * 2 + Baz");

        this.printDataFrame("Original", dataFrame);

        dataFrame.sortBy(Lists.immutable.of("Quuz"));

        this.printDataFrame("Sorted", dataFrame);

        DataFrame expected = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux").addDoubleColumn("Quuz")
                .addRow("Abigail", "456",  11L, 10.0, 20.0, 32.0)
                .addRow("Abigail", "789",  12L, 12.0, 11.0, 36.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0, 39.0)
                .addRow("Carol",   "Zzz",  14L, 14.0, 40.0, 42.0)
                .addRow("Abigail", "123",  15L, 15.0, 10.0, 45.0)
                .addRow("Carol",   "Xyz",  15L, 28.0, 40.0, 58.0)
                ;

        DataFrameUtil.assertEquals(expected, dataFrame);
    }

    @Test
    public void sortByComputedComputedColumns()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Abigail", "456",  11L, 10.0, 20.0)
                .addRow("Carol",   "Xyz",  15L, 28.0, 40.0)
                .addRow("Abigail", "123",  15L, 15.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Zzz",  14L, 14.0, 40.0)
                .addRow("Abigail", "789",  12L, 12.0, 11.0);
        dataFrame.addDoubleColumn("Quuz", "Bar * 2 + Baz");
        dataFrame.addDoubleColumn("Waldo", "Quuz + 10.0");

        this.printDataFrame("Original", dataFrame);

        dataFrame.sortBy(Lists.immutable.of("Quuz"));

        this.printDataFrame("Sorted", dataFrame);

        DataFrame expected = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addDoubleColumn("Quuz").addDoubleColumn("Waldo")
                .addRow("Abigail", "456",  11L, 10.0, 20.0, 32.0, 42.0)
                .addRow("Abigail", "789",  12L, 12.0, 11.0, 36.0, 46.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0, 39.0, 49.0)
                .addRow("Carol",   "Zzz",  14L, 14.0, 40.0, 42.0, 52.0)
                .addRow("Abigail", "123",  15L, 15.0, 10.0, 45.0, 55.0)
                .addRow("Carol",   "Xyz",  15L, 28.0, 40.0, 58.0, 68.0)
                ;

        DataFrameUtil.assertEquals(expected, dataFrame);
    }

    @Test
    public void sortByExpression()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addLongColumn("Record").addStringColumn("Foo").addLongColumn("Bar")
                .addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Abigail", 0, "456",  11L, 10.0, 20.0)
                .addRow("Carol",   1, "Xyz",  15L, 28.0, 40.0)
                .addRow("Abigail", 2, "123",  15L, 15.0, 10.0)
                .addRow("Bob",     3, "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   4, "Zzz",  14L, 14.0, 40.0)
                .addRow("Abigail", 5, "789",  12L, 12.0, 11.0);

//        dataFrame.addDoubleColumn("Xxx", "Bar * 2 + Baz");

        this.printDataFrame("Original", dataFrame);

        dataFrame.sortByExpression("Bar * 2 + Baz");

        this.printDataFrame("Sorted", dataFrame);

        DataFrame expected = new DataFrame("FrameOfData")
                .addStringColumn("Name").addLongColumn("Record").addStringColumn("Foo").addLongColumn("Bar")
                .addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Abigail", 0, "456",  11L, 10.0, 20.0)
                .addRow("Abigail", 5, "789",  12L, 12.0, 11.0)
                .addRow("Bob",     3, "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   4, "Zzz",  14L, 14.0, 40.0)
                .addRow("Abigail", 2, "123",  15L, 15.0, 10.0)
                .addRow("Carol",   1, "Xyz",  15L, 28.0, 40.0)
                ;

//        expected.addDoubleColumn("Xxx", "Bar * 2 + Baz");

        DataFrameUtil.assertEquals(expected, dataFrame);
    }

    private void printDataFrame(String title, DataFrame dataFrame)
    {
        System.out.println("-- " + title);
        System.out.println(dataFrame.asCsvString());
    }
}
