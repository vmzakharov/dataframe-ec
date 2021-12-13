package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.block.function.primitive.IntIntToIntFunction;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class DfColumnCompareTest
{
    @Test
    public void simpleCompare()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("col1").addStringColumn("col2").addLongColumn("col3").addDateColumn("col4")
                .addRow("A", "B", 3, LocalDate.of(2020, 10, 23))
                .addRow("B", "B", 1, LocalDate.of(2020, 10, 21))
                .addRow("C", "B", 2, LocalDate.of(2020, 10, 22))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("col1").addStringColumn("column2").addLongColumn("number").addDateColumn("date")
                .addRow("A", "C", 2, LocalDate.of(2020, 10, 22))
                .addRow("X", "B", 1, LocalDate.of(2020, 10, 21))
                .addRow("A", "A", 3, LocalDate.of(2020, 10, 23))
                ;

        IntIntToIntFunction col1Comparator = df1.columnComparator(df2, "col1", "col1");
        Assert.assertTrue(col1Comparator.valueOf(0, 0) == 0);
        Assert.assertTrue(col1Comparator.valueOf(1, 1) < 0);
        Assert.assertTrue(col1Comparator.valueOf(2, 2) > 0);

        Assert.assertTrue(col1Comparator.valueOf(0, 1) < 0);
        Assert.assertTrue(col1Comparator.valueOf(0, 2) == 0);

        Assert.assertTrue(col1Comparator.valueOf(1, 0) > 0);
        Assert.assertTrue(col1Comparator.valueOf(1, 2) > 0);

        IntIntToIntFunction col2Comparator = df1.columnComparator(df2, "col2", "column2");
        Assert.assertTrue(col2Comparator.valueOf(0, 0) < 0);
        Assert.assertTrue(col2Comparator.valueOf(1, 1) == 0);
        Assert.assertTrue(col2Comparator.valueOf(2, 2) > 0);

        IntIntToIntFunction col3Comparator = df1.columnComparator(df2, "col3", "number");
        Assert.assertTrue(col3Comparator.valueOf(0, 0) > 0);
        Assert.assertTrue(col3Comparator.valueOf(1, 1) == 0);
        Assert.assertTrue(col3Comparator.valueOf(2, 2) < 0);

        Assert.assertTrue(col3Comparator.valueOf(2, 0) == 0);
        Assert.assertTrue(col3Comparator.valueOf(2, 1) > 0);

        IntIntToIntFunction col4Comparator = df1.columnComparator(df2, "col4", "date");
        Assert.assertTrue(col4Comparator.valueOf(0, 0) > 0);
        Assert.assertTrue(col4Comparator.valueOf(1, 1) == 0);
        Assert.assertTrue(col4Comparator.valueOf(2, 2) < 0);

        Assert.assertTrue(col4Comparator.valueOf(0, 1) > 0);
        Assert.assertTrue(col4Comparator.valueOf(0, 2) == 0);
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

        IntIntToIntFunction col1Comparator = df1.columnComparator(df2, "col1", "col1");
        Assert.assertTrue(col1Comparator.valueOf(0, 0) > 0);
        Assert.assertTrue(col1Comparator.valueOf(1, 1) > 0);
        Assert.assertTrue(col1Comparator.valueOf(2, 2) < 0);

        IntIntToIntFunction col2Comparator = df1.columnComparator(df2, "col2", "column2");
        Assert.assertTrue(col2Comparator.valueOf(0, 0) < 0);
        Assert.assertTrue(col2Comparator.valueOf(1, 1) > 0);
        Assert.assertTrue(col2Comparator.valueOf(2, 2) == 0);
    }

    @Test
    public void stringCompareWithNullValues()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("col1").addStringColumn("col2").addLongColumn("col3")
                .addRow(null,  "B", null)
                .addRow("B",  null, 3)
                .addRow("C",   "B", 1)
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("col1").addStringColumn("col2").addLongColumn("number")
                .addRow("A",   "C", 1)
                .addRow("X",  null, null)
                .addRow(null,  "A", 2)
                ;

        IntIntToIntFunction col1Comparator = df1.columnComparator(df2, "col1", "col1");
        Assert.assertTrue(col1Comparator.valueOf(0, 0) < 0);
        Assert.assertTrue(col1Comparator.valueOf(1, 1) < 0);
        Assert.assertTrue(col1Comparator.valueOf(2, 2) > 0);

        IntIntToIntFunction col2Comparator = df1.columnComparator(df2, "col2", "col2");
        Assert.assertTrue(col2Comparator.valueOf(0, 0) < 0);
        Assert.assertTrue(col2Comparator.valueOf(1, 1) == 0);
        Assert.assertTrue(col2Comparator.valueOf(2, 2) > 0);

        IntIntToIntFunction col3Comparator = df1.columnComparator(df2, "col3", "number");
        Assert.assertTrue(col3Comparator.valueOf(0, 0) < 0);
        Assert.assertTrue(col3Comparator.valueOf(1, 1) > 0);
        Assert.assertTrue(col3Comparator.valueOf(2, 2) < 0);

        Assert.assertTrue(col3Comparator.valueOf(0, 1) == 0);
    }
}
