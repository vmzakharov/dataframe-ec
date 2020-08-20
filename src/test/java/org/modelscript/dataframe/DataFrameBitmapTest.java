package org.modelscript.dataframe;

import org.eclipse.collections.api.collection.primitive.MutableBooleanCollection;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.primitive.BooleanList;
import org.eclipse.collections.api.list.primitive.MutableBooleanList;
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

        dataFrame.bitmapSetFlag(0);
        dataFrame.bitmapSetFlag(2);
        dataFrame.bitmapSetFlag(3);

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

        this.dataFrame.bitmapSetFlag(0);
        this.dataFrame.bitmapSetFlag(2);
        this.dataFrame.bitmapSetFlag(3);

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

        this.dataFrame.bitmapSetFlag(1);
        this.dataFrame.bitmapSetFlag(4);

        DataFrame filtered = this.dataFrame.selectNotFlagged();

        DataFrame expected = new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Pqr",  11L, 10.0, 20.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                ;
        DataFrameUtil.assertEquals(expected, filtered);

        filtered.enableBitmap();
        filtered.bitmapSetFlag(0);
        filtered.bitmapSetFlag(1);
        filtered.bitmapSetFlag(2);

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
        this.dataFrame.flagRowsBy("startsWith(Name, \"A\") and Baz > 11.0");

        BooleanList flags = IntInterval.zeroTo(this.dataFrame.rowCount() - 1)
                .collectBoolean(this.dataFrame::bitmapIsFlagged, BooleanLists.mutable.of());

        Assert.assertEquals(BooleanLists.immutable.of(false, true, false, false, true), flags);
    }
}
