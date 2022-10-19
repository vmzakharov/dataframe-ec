package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DecimalValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import org.eclipse.collections.api.factory.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

import static io.github.vmzakharov.ecdataframe.dataframe.DfColumnSortOrder.ASC;
import static io.github.vmzakharov.ecdataframe.dataframe.DfColumnSortOrder.DESC;

public class DataFrameSortDirectionTest
{
    @Test
    public void simpleOneColumnSortDescending()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice",   "Abc",  11L, 10.0, 20.0)
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                .addRow("Albert",  "Def",  12L, 12.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0);

        DataFrameUtil.assertEquals(new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Alice",   "Abc",  11L, 10.0, 20.0)
                .addRow("Albert",  "Def",  12L, 12.0, 10.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0)
                ,
                dataFrame.sortBy(Lists.immutable.of("Name"), Lists.immutable.of(DESC)));

        DataFrameUtil.assertEquals(new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Carol",   "Xyz",  14L, 14.0, 40.0)
                .addRow("Albert",  "Def",  12L, 12.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Abigail", "Def",  15L, 15.0, 11.0)
                .addRow("Alice",   "Abc",  11L, 10.0, 20.0)
                ,
                dataFrame.sortBy(Lists.immutable.of("Foo"), Lists.immutable.of(DESC)));

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
    public void oneColumnSortWithNullValuesDescending()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz")
                .addRow("Alice",  null,  11L, 10.0)
                .addRow(null,     "Xyz",  14L, 14.0)
                .addRow("Albert", "Def",  12L, 12.0)
                .addRow("Bob",    null,  13L, 13.0)
                .addRow(null,     "Def",  15L, 15.0)
                ;

        dataFrame.sortBy(Lists.immutable.of("Name"), Lists.immutable.of(DESC));

        DataFrameUtil.assertEquals(new DataFrame("Expected Sorted By Name")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz")
                .addRow("Bob",    null,  13L, 13.0)
                .addRow("Alice",  null,  11L, 10.0)
                .addRow("Albert", "Def", 12L, 12.0)
                .addRow(null,     "Xyz", 14L, 14.0)
                .addRow(null,     "Def", 15L, 15.0)
                ,
                dataFrame);

        dataFrame.sortBy(Lists.immutable.of("Foo"), Lists.immutable.of(DESC));

        DataFrameUtil.assertEquals(new DataFrame("Expected Re-Sorted By Foo")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz")
                .addRow(null,     "Xyz", 14L, 14.0)
                .addRow("Albert", "Def", 12L, 12.0)
                .addRow(null,     "Def", 15L, 15.0)
                .addRow("Alice",  null,  11L, 10.0)
                .addRow("Bob",    null,  13L, 13.0)
                ,
                dataFrame);

        dataFrame.unsort();

        DataFrame expectedUnsorted = new DataFrame("Unsorted FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz")
                .addRow("Alice",  null,  11L, 10.0)
                .addRow(null,     "Xyz",  14L, 14.0)
                .addRow("Albert", "Def",  12L, 12.0)
                .addRow("Bob",    null,  13L, 13.0)
                .addRow(null,     "Def",  15L, 15.0)
                ;

        DataFrameUtil.assertEquals(expectedUnsorted, dataFrame);
    }

    @Test
    public void multiColumnSortWithNullValuesMixedOrder()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Abigail", "456", 11L,  10.0, 20.0)
                .addRow("Carol",   "Xyz", 15L,  28.0, 40.0)
                .addRow(null,      "123", 15L,  15.0, 10.0)
                .addRow("Bob",     "Def", 13L,  13.0, 25.0)
                .addRow("Carol",   "Zzz", null, 14.0, 40.0)
                .addRow(null,      "789", null, 12.0, 11.0)
                .addRow(null,      "789", null, 15.0, 11.0)
                .addRow(null,      "Yyy", null, null, 11.0)
                ;

        dataFrame.sortBy(Lists.immutable.of("Name", "Bar", "Baz"), Lists.immutable.of(DESC, ASC, DESC));

        DataFrameUtil.assertEquals(new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Carol",   "Zzz", null, 14.0, 40.0)
                .addRow("Carol",   "Xyz", 15L,  28.0, 40.0)
                .addRow("Bob",     "Def", 13L,  13.0, 25.0)
                .addRow("Abigail", "456", 11L,  10.0, 20.0)
                .addRow(null,      "789", null, 15.0, 11.0)
                .addRow(null,      "789", null, 12.0, 11.0)
                .addRow(null,      "Yyy", null, null, 11.0)
                .addRow(null,      "123", 15L,  15.0, 10.0)
                ,
                dataFrame);

        // validates that different ways of cell value retrieval respects logical order
        Assert.assertEquals(13.0, dataFrame.getObject("Baz", 2));
        Assert.assertEquals(13.0, dataFrame.getDouble("Baz", 2), 0.000001);
        Assert.assertEquals(0, new DoubleValue(13.0).compareTo(dataFrame.getValue("Baz", 2)));

        Assert.assertEquals(13.0, dataFrame.getObject(2, 3));
        Assert.assertEquals(0, new DoubleValue(13.0).compareTo(dataFrame.getValue(2, 3)));
    }

    @Test
    public void multiColumnSortMixedOrder()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDecimalColumn("Baz").addDoubleColumn("Qux")
                .addRow("Abigail", "456",  11L, BigDecimal.valueOf(100, 1), 20.0)
                .addRow("Carol",   "Xyz",  15L, BigDecimal.valueOf(280, 1), 40.0)
                .addRow("Abigail", "123",  11L, BigDecimal.valueOf(150, 1), 10.0)
                .addRow("Bob",     "Def",  13L, BigDecimal.valueOf(130, 1), 25.0)
                .addRow("Carol",   "Zzz",  14L, BigDecimal.valueOf(140, 1), 40.0)
                .addRow("Abigail", "789",  11L, BigDecimal.valueOf(120, 1), 11.0);

        dataFrame.sortBy(Lists.immutable.of("Name", "Bar", "Baz"), Lists.immutable.of(DESC, DESC, ASC));

        DataFrameUtil.assertEquals(new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDecimalColumn("Baz").addDoubleColumn("Qux")
                .addRow("Carol",   "Xyz",  15L, BigDecimal.valueOf(280, 1), 40.0)
                .addRow("Carol",   "Zzz",  14L, BigDecimal.valueOf(140, 1), 40.0)
                .addRow("Bob",     "Def",  13L, BigDecimal.valueOf(130, 1), 25.0)
                .addRow("Abigail", "456",  11L, BigDecimal.valueOf(100, 1), 20.0)
                .addRow("Abigail", "789",  11L, BigDecimal.valueOf(120, 1), 11.0)
                .addRow("Abigail", "123",  11L, BigDecimal.valueOf(150, 1), 10.0)
                ,
                dataFrame);

        // validates that different ways of cell value retrieval respects logical order
        BigDecimal expectedCellValue = BigDecimal.valueOf(13.0);
        Assert.assertEquals(expectedCellValue, dataFrame.getObject("Baz", 2));
        Assert.assertEquals(expectedCellValue, dataFrame.getDecimal("Baz", 2));
        Assert.assertEquals(0, new DecimalValue(expectedCellValue).compareTo(dataFrame.getValue("Baz", 2)));

        Assert.assertEquals(expectedCellValue, dataFrame.getObject(2, 3));
        Assert.assertEquals(0, new DecimalValue(expectedCellValue).compareTo(dataFrame.getValue(2, 3)));
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

        dataFrame.sortBy(Lists.immutable.of("Quuz"));

        DataFrame expected = new DataFrame("Expected FrameOfData")
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
    public void sortEmptyDataFrameByColumns()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz");

        dataFrame.sortBy(Lists.immutable.of("Foo", "Baz"), Lists.immutable.of(ASC, DESC));

        Assert.assertTrue("Sorted!", true);
    }

    @Test
    public void sortEmptyDataFrameByExpressionDescending()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz");

        dataFrame.sortByExpression("Baz * 2.5");

        Assert.assertTrue("Sorted!", true);
    }

    @Test
    public void sortByExpressionDescending()
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

        DataFrameUtil.assertEquals(new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addLongColumn("Record").addStringColumn("Foo").addLongColumn("Bar")
                .addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Carol",   1, "Xyz",  15L, 28.0, 40.0)
                .addRow("Abigail", 2, "123",  15L, 15.0, 10.0)
                .addRow("Carol",   4, "Zzz",  14L, 14.0, 40.0)
                .addRow("Bob",     3, "Def",  13L, 13.0, 25.0)
                .addRow("Abigail", 5, "789",  12L, 12.0, 11.0)
                .addRow("Abigail", 0, "456",  11L, 10.0, 20.0)
                ,
                dataFrame.sortByExpression("Bar * 2 + Baz", DESC));
    }

    @Test
    public void multiColumnSortWithNulls()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow(null,      "456",  11L, 10.0, 20.0)
                .addRow("Carol",   "Xyz",  15L, 28.0, 40.0)
                .addRow(null,      "123",  15L, 15.0, 10.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Zzz",  14L, 14.0, 40.0)
                .addRow("Abigail", "789",  12L, 12.0, 11.0);

        dataFrame.sortBy(Lists.immutable.of("Name", "Bar"));

        DataFrame expected = new DataFrame("Expected FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow(null,      "456",  11L, 10.0, 20.0)
                .addRow(null,      "123",  15L, 15.0, 10.0)
                .addRow("Abigail", "789",  12L, 12.0, 11.0)
                .addRow("Bob",     "Def",  13L, 13.0, 25.0)
                .addRow("Carol",   "Zzz",  14L, 14.0, 40.0)
                .addRow("Carol",   "Xyz",  15L, 28.0, 40.0)
                ;

        DataFrameUtil.assertEquals(expected, dataFrame);
    }

    @Test
    public void columnSortIsStable()
    {
        DataFrame df = new DataFrame("DF")
            .addLongColumn("ID").addStringColumn("Foo").addLongColumn("Bar")
            .addRow(0, "A", 1L)
            .addRow(1, "B", 2L)
            .addRow(2, "B", 1L)
            .addRow(3, "A", 1L)
            .addRow(4, "A", 2L)
            .addRow(5, "B", 1L)
            .addRow(6, "B", 3L)
            .addRow(7, "B", 2L)
            .addRow(8, "B", 2L)
            .addRow(9, "A", 2L)
            ;

        df.sortBy(Lists.immutable.of("Foo", "Bar"), Lists.mutable.of(ASC, DESC));

        DataFrameUtil.assertEquals(new DataFrame("DF")
                .addLongColumn("ID").addStringColumn("Foo").addLongColumn("Bar")
                .addRow(4, "A", 2L)
                .addRow(9, "A", 2L)
                .addRow(0, "A", 1L)
                .addRow(3, "A", 1L)
                .addRow(6, "B", 3L)
                .addRow(1, "B", 2L)
                .addRow(7, "B", 2L)
                .addRow(8, "B", 2L)
                .addRow(2, "B", 1L)
                .addRow(5, "B", 1L)
                ,
                df);
    }

    @Test
    public void expressionSortIsStable()
    {
        DataFrame df = new DataFrame("DF")
            .addLongColumn("ID").addStringColumn("Foo").addLongColumn("Bar")
            .addRow(0, "A", 1L)
            .addRow(1, "B", 2L)
            .addRow(2, "B", 1L)
            .addRow(3, "A", 1L)
            .addRow(4, "A", 2L)
            .addRow(5, "B", 1L)
            .addRow(6, "B", 3L)
            .addRow(7, "B", 2L)
            .addRow(8, "B", 2L)
            .addRow(9, "A", 2L)
            ;

        df.sortByExpression("Foo + toString(Bar)", DESC);

        DataFrameUtil.assertEquals(new DataFrame("DF")
                .addLongColumn("ID").addStringColumn("Foo").addLongColumn("Bar")
                .addRow(6, "B", 3L)
                .addRow(1, "B", 2L)
                .addRow(7, "B", 2L)
                .addRow(8, "B", 2L)
                .addRow(2, "B", 1L)
                .addRow(5, "B", 1L)
                .addRow(4, "A", 2L)
                .addRow(9, "A", 2L)
                .addRow(0, "A", 1L)
                .addRow(3, "A", 1L)
                ,
                df);
    }
}
