package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class DfColumnCompareTest
{
    private static final double TOLERANCE = 0.000001;

    @Test
    public void simpleCompare()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("col1").addStringColumn("col2").addLongColumn("col3").addDateColumn("col4").addDoubleColumn("col5")
                .addRow("A", "B", 3, LocalDate.of(2020, 10, 23), 103.03)
                .addRow("B", "B", 1, LocalDate.of(2020, 10, 21), 100.01)
                .addRow("C", "B", 2, LocalDate.of(2020, 10, 22), 101.01)
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("col1").addStringColumn("column2").addLongColumn("number").addDateColumn("date").addDoubleColumn("amount")
                .addRow("A", "C", 2, LocalDate.of(2020, 10, 22), 101.01)
                .addRow("X", "B", 1, LocalDate.of(2020, 10, 21), 100.01)
                .addRow("A", "A", 3, LocalDate.of(2020, 10, 23), 105.05)
                ;

        DfCellComparator col1Comparator = df1.columnComparator(df2, "col1", "col1");
        Assert.assertTrue(col1Comparator.compare(0, 0).compared() == 0);
        Assert.assertTrue(col1Comparator.compare(1, 1).compared() < 0);
        Assert.assertTrue(col1Comparator.compare(2, 2).compared() > 0);

        Assert.assertEquals(0, col1Comparator.compare(0, 0).delta());

        Assert.assertTrue(col1Comparator.compare(0, 1).compared() < 0);
        Assert.assertTrue(col1Comparator.compare(0, 2).compared() == 0);

        Assert.assertTrue(col1Comparator.compare(1, 0).compared() > 0);
        Assert.assertTrue(col1Comparator.compare(1, 2).compared() > 0);

        DfCellComparator col2Comparator = df1.columnComparator(df2, "col2", "column2");
        Assert.assertTrue(col2Comparator.compare(0, 0).compared() < 0);
        Assert.assertTrue(col2Comparator.compare(1, 1).compared() == 0);
        Assert.assertTrue(col2Comparator.compare(2, 2).compared() > 0);

        DfCellComparator col3Comparator = df1.columnComparator(df2, "col3", "number");
        Assert.assertTrue(col3Comparator.compare(0, 0).compared() > 0);
        Assert.assertTrue(col3Comparator.compare(1, 1).compared() == 0);
        Assert.assertTrue(col3Comparator.compare(2, 2).compared() < 0);

        Assert.assertTrue(col3Comparator.compare(2, 0).compared() == 0);
        Assert.assertTrue(col3Comparator.compare(2, 1).compared() > 0);

        Assert.assertEquals(1L, col3Comparator.compare(0, 0).delta());
        Assert.assertEquals(0L, col3Comparator.compare(1, 1).delta());
        Assert.assertEquals(-1L, col3Comparator.compare(2, 2).delta());

        DfCellComparator col4Comparator = df1.columnComparator(df2, "col4", "date");
        Assert.assertTrue(col4Comparator.compare(0, 0).compared() > 0);
        Assert.assertTrue(col4Comparator.compare(1, 1).compared() == 0);
        Assert.assertTrue(col4Comparator.compare(2, 2).compared() < 0);

        Assert.assertTrue(col4Comparator.compare(0, 1).compared() > 0);
        Assert.assertTrue(col4Comparator.compare(0, 2).compared() == 0);

        DfCellComparator col5Comparator = df1.columnComparator(df2, "col5", "amount");
        Assert.assertTrue(col5Comparator.compare(0, 0).compared() > 0);
        Assert.assertTrue(col5Comparator.compare(1, 1).compared() == 0);
        Assert.assertTrue(col5Comparator.compare(2, 2).compared() < 0);

        Assert.assertEquals(2.02, col5Comparator.compare(0, 0).dDelta(), TOLERANCE);
        Assert.assertEquals(0.0, col5Comparator.compare(1, 1).dDelta(), TOLERANCE);
        Assert.assertEquals(-4.04, col5Comparator.compare(2, 2).dDelta(), TOLERANCE);
    }

    @Test
    public void compareToSameDataFrame()
    {
        DataFrame df1 = new DataFrame("DF1")
            .addStringColumn("col1").addStringColumn("col2").addLongColumn("col3")
            .addRow("A", "B", 3)
            .addRow("B", "B", 1)
            .addRow("C", "B", 2)
            ;

        DfCellComparator coCo1 = df1.columnComparator(df1, "col1", "col2");

        Assert.assertTrue(coCo1.compare(0, 0).compared() < 0);
        Assert.assertTrue(coCo1.compare(1, 1).compared() == 0);
        Assert.assertTrue(coCo1.compare(2, 2).compared() > 0);

        DfCellComparator coCo2 = df1.columnComparator(df1, "col3", "col3");

        Assert.assertTrue(coCo2.compare(0, 0).compared() == 0);
        Assert.assertTrue(coCo2.compare(1, 1).compared() == 0);
        Assert.assertTrue(coCo2.compare(2, 2).compared() == 0);
        Assert.assertTrue(coCo2.compare(0, 1).compared() > 0);
        Assert.assertTrue(coCo2.compare(0, 2).compared() > 0);
        Assert.assertTrue(coCo2.compare(2, 1).compared() > 0);
        Assert.assertTrue(coCo2.compare(1, 2).compared() < 0);
        Assert.assertTrue(coCo2.compare(2, 0).compared() < 0);
    }

    @Test
    public void compareSortedDataFrames()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("col1").addStringColumn("col2").addLongColumn("col3")
                .addRow("A", "B", 3)
                .addRow("B", "B", 1)
                .addRow("C", "B", 2)
                ;

        df1.sortBy(Lists.immutable.of("col3"));

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("col1").addStringColumn("column2").addLongColumn("number")
                .addRow("A", "C", 1)
                .addRow("X", "B", 3)
                .addRow("A", "A", 2)
                ;

        df2.sortBy(Lists.immutable.of("number"));
//            "B", "B", "1"
//            "C", "B", "2"
//            "A", "B", "3"
//
//            "A", "C", "1"
//            "A", "A", "2"
//            "X", "B", "3"

        DfCellComparator col1Comparator = df1.columnComparator(df2, "col1", "col1");
        Assert.assertTrue(col1Comparator.compare(0, 0).compared() > 0);
        Assert.assertTrue(col1Comparator.compare(1, 1).compared() > 0);
        Assert.assertTrue(col1Comparator.compare(2, 2).compared() < 0);

        DfCellComparator col2Comparator = df1.columnComparator(df2, "col2", "column2");
        Assert.assertTrue(col2Comparator.compare(0, 0).compared() < 0);
        Assert.assertTrue(col2Comparator.compare(1, 1).compared() > 0);
        Assert.assertTrue(col2Comparator.compare(2, 2).compared() == 0);
    }

    @Test
    public void compareWithNullValues()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("col1").addLongColumn("col2").addDateColumn("col3").addDoubleColumn("col4")
                .addRow("A",  null, LocalDate.of(2020, 6, 20), 100.15)
                .addRow(null,    3,                      null, 100.15)
                .addRow(null,    2,                      null,   null)
                .addRow("D",  null, LocalDate.of(2020, 6, 22),   null)
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("col1").addLongColumn("number").addDateColumn("date").addDoubleColumn("amount")
                .addRow(null,    1,                      null, 101.15)
                .addRow("X",  null,                      null,   null)
                .addRow(null,    4, LocalDate.of(2020, 6, 21),   null)
                .addRow("D",  null, LocalDate.of(2020, 6, 22), 100.15)
                ;

        DfCellComparator col1Comparator = df1.columnComparator(df2, "col1", "col1");
        Assert.assertTrue(col1Comparator.compare(0, 0).rightIsNull());
        Assert.assertTrue(col1Comparator.compare(1, 1).leftIsNull());
        Assert.assertTrue(col1Comparator.compare(2, 2).bothAreNulls());
        Assert.assertTrue(col1Comparator.compare(3, 3).noNulls());

        DfCellComparator col2Comparator = df1.columnComparator(df2, "col2", "number");
        Assert.assertTrue(col2Comparator.compare(0, 0).leftIsNull());
        Assert.assertTrue(col2Comparator.compare(1, 1).rightIsNull());
        Assert.assertTrue(col2Comparator.compare(2, 2).noNulls());
        Assert.assertTrue(col2Comparator.compare(3, 3).bothAreNulls());

        DfCellComparator col3Comparator = df1.columnComparator(df2, "col3", "date");
        Assert.assertTrue(col3Comparator.compare(0, 0).rightIsNull());
        Assert.assertTrue(col3Comparator.compare(1, 1).bothAreNulls());
        Assert.assertTrue(col3Comparator.compare(2, 2).leftIsNull());
        Assert.assertTrue(col3Comparator.compare(3, 3).noNulls());

        DfCellComparator col4Comparator = df1.columnComparator(df2, "col4", "amount");
        Assert.assertTrue(col4Comparator.compare(0, 0).noNulls());
        Assert.assertTrue(col4Comparator.compare(1, 1).rightIsNull());
        Assert.assertTrue(col4Comparator.compare(2, 2).bothAreNulls());
        Assert.assertTrue(col4Comparator.compare(3, 3).leftIsNull());
    }

    @Test
    public void compareWithOverflow()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addLongColumn("order").addLongColumn("col1").addDoubleColumn("col2")
                .addRow(1, Long.MAX_VALUE, Double.MAX_VALUE)
                .addRow(2, Long.MAX_VALUE, Double.MAX_VALUE)
                .addRow(3, Long.MAX_VALUE, Double.MAX_VALUE)
                .addRow(4, Long.MIN_VALUE, -Double.MAX_VALUE)
                .addRow(5, Long.MIN_VALUE, -Double.MAX_VALUE)
                .addRow(6, (Long.MAX_VALUE / 3) * 2, (Double.MAX_VALUE / 3.0) * 2.0)
                .addRow(7, (Long.MIN_VALUE / 4) * 3, (-Double.MAX_VALUE / 4.0) * 3.0)
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addLongColumn("order").addLongColumn("number").addDoubleColumn("value")
                .addRow(1, Long.MAX_VALUE, Double.MAX_VALUE)
                .addRow(2,  -1,  -1.0)
                .addRow(3, 100, 100.0)
                .addRow(4,  10,  10.0)
                .addRow(5,  -1,  -1.0)
                .addRow(6, (Long.MIN_VALUE / 3) * 2, (-Double.MAX_VALUE / 3.0) * 2.0)
                .addRow(7, (Long.MAX_VALUE / 4) * 3, (Double.MAX_VALUE / 4.0) * 3.0)
                ;

        DfCellComparator col1Comparator = df1.columnComparator(df2, "col1", "number");

        Assert.assertTrue(col1Comparator.compare(0, 0).compared() == 0);
        Assert.assertTrue(col1Comparator.compare(1, 1).compared() > 0);
        Assert.assertTrue(col1Comparator.compare(2, 2).compared() > 0);
        Assert.assertTrue(col1Comparator.compare(3, 3).compared() < 0);
        Assert.assertTrue(col1Comparator.compare(4, 4).compared() < 0);
        Assert.assertTrue(col1Comparator.compare(5, 5).compared() > 0);
        Assert.assertTrue(col1Comparator.compare(6, 6).compared() < 0);

        DfCellComparator col2Comparator = df1.columnComparator(df2, "col2", "value");

        Assert.assertTrue(col2Comparator.compare(0, 0).compared() == 0);
        Assert.assertTrue(col2Comparator.compare(1, 1).compared() > 0);
        Assert.assertTrue(col2Comparator.compare(2, 2).compared() > 0);
        Assert.assertTrue(col2Comparator.compare(3, 3).compared() < 0);
        Assert.assertTrue(col2Comparator.compare(4, 4).compared() < 0);
        Assert.assertTrue(col2Comparator.compare(5, 5).compared() > 0);
        Assert.assertTrue(col2Comparator.compare(6, 6).compared() < 0);

        Assert.assertTrue(Double.isInfinite(col2Comparator.compare(5, 5).dDelta()));
        Assert.assertTrue(col2Comparator.compare(5, 5).dDelta() > 0);

        Assert.assertTrue(Double.isInfinite(col2Comparator.compare(6, 6).dDelta()));
        Assert.assertTrue(col2Comparator.compare(6, 6).dDelta() < 0);
    }
}
