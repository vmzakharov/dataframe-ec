package org.modelscript.dataframe;

import org.eclipse.collections.api.list.primitive.BooleanList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.BooleanLists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataFrameFilterTest
{
    private DataFrame dataFrame;

    @Before
    public void setUpDataFrame()
    {
        this.dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Pqr",  11L, 10.0, 20.0)
                .addRow("Albert",  "Abc",  12L, 12.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0)
        ;
    }

    @Test
    public void simpleSelection()
    {
        DataFrame filtered = this.dataFrame.selectBy("Foo == \"Def\" or Foo == \"Abc\"");

        DataFrame expected = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Albert",  "Abc",  12L, 12.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0);

        DataFrameUtil.assertEquals(expected, filtered);
    }

    @Test
    public void selectionWithComputedFields()
    {
        this.dataFrame.addDoubleColumn("BazAndQux", "Baz + Qux");

        DataFrame filtered = this.dataFrame.selectBy("(Foo == \"Def\" or Foo == \"Pqr\") and BazAndQux > 25");

        DataFrame expected = new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Pqr",  11L, 10.0, 20.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0);
        expected.addDoubleColumn("BazAndQux", "Baz + Qux");

        DataFrameUtil.assertEquals(expected, filtered);
    }

    @Test
    public void selectionOfMarkedRows()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Pqr",  11L, 10.0, 20.0)
                .addRow("Albert",  "Abc",  12L, 12.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0)
                ;

        BooleanList marked = BooleanLists.immutable.of(true, false, true, true, false);

        DataFrame filtered = dataFrame.selectMarked(marked);

        DataFrame expected = new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Pqr",  11L, 10.0, 20.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                ;
        DataFrameUtil.assertEquals(expected, filtered);

        BooleanList nothingMarked = BooleanLists.immutable.of(false, false, false);

        DataFrame moreFiltered = filtered.selectMarked(nothingMarked);

        DataFrame nothingExpected = new DataFrame("Nothing Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                ;

        DataFrameUtil.assertEquals(nothingExpected, moreFiltered);
    }

    @Test
    public void selectionOfMarkedRowsOnSortedDataFrame()
    {
        this.dataFrame.sortBy(Lists.immutable.of("Foo"));

        BooleanList marked = BooleanLists.immutable.of(true, false, true, true, false);

        DataFrame filtered = this.dataFrame.selectMarked(marked);

        DataFrame expected = new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Albert",  "Abc",  12L, 12.0, 10.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0)
                .addRow("Alice",   "Pqr",  11L, 10.0, 20.0)
                ;
        DataFrameUtil.assertEquals(expected, filtered);
    }

    @Test
    public void selectionOfNotMarkedRows()
    {
        BooleanList marked = BooleanLists.immutable.of(false, true, false, false, true);

        DataFrame filtered = this.dataFrame.selectNotMarked(marked);

        DataFrame expected = new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Pqr",  11L, 10.0, 20.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                ;
        DataFrameUtil.assertEquals(expected, filtered);

        BooleanList nothingMarked = BooleanLists.immutable.of(true, true, true);

        DataFrame moreFiltered = filtered.selectNotMarked(nothingMarked);

        DataFrame nothingExpected = new DataFrame("Nothing Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                ;

        DataFrameUtil.assertEquals(nothingExpected, moreFiltered);
    }

    @Test
    public void selectionOfNotMarkedRowsOnSortedDataFrame()
    {
        this.dataFrame.sortBy(Lists.immutable.of("Foo"));

        BooleanList marked = BooleanLists.immutable.of(false, true, false, false, true);

        DataFrame filtered = dataFrame.selectNotMarked(marked);

        DataFrame expected = new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Albert",  "Abc",  12L, 12.0, 10.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0)
                .addRow("Alice",   "Pqr",  11L, 10.0, 20.0)
                ;
        DataFrameUtil.assertEquals(expected, filtered);
    }

    @Test
    public void markRowsBasedOnExpression()
    {
        this.dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Pqr",  11L, 10.0, 20.0)
                .addRow("Albert",  "Abc",  12L, 12.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0)
        ;

        BooleanList marked = this.dataFrame.markRowsBy("startsWith(Name, \"A\") and Baz > 11.0");

        Assert.assertEquals(BooleanLists.immutable.of(false, true, false, false, true), marked);
    }
}
