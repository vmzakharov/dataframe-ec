package org.modelscript.dataframe;

import org.eclipse.collections.api.list.primitive.BooleanList;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.BooleanLists;
import org.eclipse.collections.impl.list.primitive.IntInterval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataFrameBitmapTest
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
    public void selectionOfFlaggedRows()
    {
        dataFrame.enableBitmap();

        dataFrame.setFlag(0);
        dataFrame.setFlag(2);
        dataFrame.setFlag(3);

        DataFrame filtered = dataFrame.selectFlagged();

        DataFrame expected = new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Pqr",  11L, 10.0, 20.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                ;
        DataFrameUtil.assertEquals(expected, filtered);

        filtered.enableBitmap();

        DataFrame moreFiltered = filtered.selectFlagged();

        DataFrame nothingExpected = new DataFrame("Nothing Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                ;

        DataFrameUtil.assertEquals(nothingExpected, moreFiltered);
    }

    @Test
    public void selectionOfMarkedRowsOnSortedDataFrame()
    {
        this.dataFrame.sortBy(Lists.immutable.of("Foo"));

        this.dataFrame.enableBitmap();

        this.dataFrame.setFlag(0);
        this.dataFrame.setFlag(2);
        this.dataFrame.setFlag(3);

        DataFrame filtered = this.dataFrame.selectFlagged();

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
        this.dataFrame.enableBitmap();

        this.dataFrame.setFlag(1);
        this.dataFrame.setFlag(4);

        DataFrame filtered = this.dataFrame.selectNotFlagged();

        DataFrame expected = new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Pqr",  11L, 10.0, 20.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                ;
        DataFrameUtil.assertEquals(expected, filtered);

        filtered.enableBitmap();
        filtered.setFlag(0);
        filtered.setFlag(1);
        filtered.setFlag(2);

        DataFrame moreFiltered = filtered.selectNotFlagged();

        DataFrame nothingExpected = new DataFrame("Nothing Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                ;

        DataFrameUtil.assertEquals(nothingExpected, moreFiltered);
    }

    @Test
    public void selectionOfNotMarkedRowsOnSortedDataFrame()
    {
        this.dataFrame.sortBy(Lists.immutable.of("Foo"));

        this.dataFrame.enableBitmap();
        this.dataFrame.setFlag(1);
        this.dataFrame.setFlag(4);

        DataFrame filtered = dataFrame.selectNotFlagged();

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
        this.dataFrame.flagRowsBy("startsWith(Name, \"A\") and Baz > 11.0");

        BooleanList flags = IntInterval.zeroTo(this.dataFrame.rowCount() - 1)
                .collectBoolean(this.dataFrame::isFlagged, BooleanLists.mutable.of());

        Assert.assertEquals(BooleanLists.immutable.of(false, true, false, false, true), flags);
    }
}
