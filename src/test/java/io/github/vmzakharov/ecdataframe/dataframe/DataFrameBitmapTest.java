package io.github.vmzakharov.ecdataframe.dataframe;

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
        this.dataFrame.resetBitmap();

        this.dataFrame.setFlag(0);
        this.dataFrame.setFlag(2);
        this.dataFrame.setFlag(3);

        DataFrame filtered = this.dataFrame.selectFlagged();

        DataFrame expected = new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Pqr",  11L, 10.0, 20.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                ;
        DataFrameUtil.assertEquals(expected, filtered);

        filtered.resetBitmap();

        DataFrame moreFiltered = filtered.selectFlagged();

        DataFrame nothingExpected = new DataFrame("Nothing Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                ;

        DataFrameUtil.assertEquals(nothingExpected, moreFiltered);
    }

    @Test
    public void selectionOfFlaggedRowsOnSortedDataFrame()
    {
        this.dataFrame.sortBy(Lists.immutable.of("Foo"));

        this.dataFrame.resetBitmap();

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
    public void selectionOfNotFlaggedRows()
    {
        this.dataFrame.resetBitmap();

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

        filtered.resetBitmap();
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
    public void selectionOfNotFlaggedRowsOnSortedDataFrame()
    {
        this.dataFrame.sortBy(Lists.immutable.of("Foo"));

        this.dataFrame.resetBitmap();
        this.dataFrame.setFlag(1);
        this.dataFrame.setFlag(4);

        DataFrame filtered = this.dataFrame.selectNotFlagged();

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

    @Test
    public void selectionWithNulls()
    {
        this.dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow(null,      "Pqr",  11L, 10.0, 20.0)
                .addRow("Albert",   null,  12L, 12.0, 10.0)
                .addRow("Bob",     "Def", null, 13.0, 25.0)
                .addRow("Carol",   "Xyz",  14L, null, 40.0)
                .addRow("Abigail", "Def",  15L, 15.0, null)
        ;

        this.dataFrame.flagRowsBy("Foo in ('Def', 'Pqr')");

        DataFrame flagged = this.dataFrame.selectFlagged();

        DataFrameUtil.assertEquals(new DataFrame("expected flagged")
                     .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                     .addRow(null,      "Pqr",  11L, 10.0, 20.0)
                     .addRow("Bob",     "Def", null, 13.0, 25.0)
                     .addRow("Abigail", "Def",  15L, 15.0, null)
                , flagged);

        DataFrame notFlagged = this.dataFrame.selectNotFlagged();

        DataFrameUtil.assertEquals(new DataFrame("expected not flagged")
                        .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                        .addRow("Albert",   null,  12L, 12.0, 10.0)
                        .addRow("Carol",   "Xyz",  14L, null, 40.0)
                , notFlagged);
    }
}
