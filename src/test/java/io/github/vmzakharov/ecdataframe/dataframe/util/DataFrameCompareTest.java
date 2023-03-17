package io.github.vmzakharov.ecdataframe.dataframe.util;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class DataFrameCompareTest
{
    @Test
    public void simpleMatch()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        Assert.assertTrue(new DataFrameCompare().equal(df1, df2));
        Assert.assertTrue(new DataFrameCompare().equal(df2, df1));
        Assert.assertTrue(new DataFrameCompare().equal(df1, df1));
    }

    @Test
    public void simpleMismatch()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 555.55, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        Assert.assertFalse(new DataFrameCompare().equal(df1, df2));
        Assert.assertFalse(new DataFrameCompare().equal(df2, df1));
    }

    @Test
    public void decimalMismatch()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(555, 5))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 555.55, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        Assert.assertFalse(new DataFrameCompare().equal(df1, df2));
        Assert.assertFalse(new DataFrameCompare().equal(df2, df1));
    }

    @Test
    public void mismatchWithNulls()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(555, 5))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow(null, 11, 555.55, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        Assert.assertFalse(new DataFrameCompare().equal(df1, df2));
        Assert.assertFalse(new DataFrameCompare().equal(df2, df1));
    }

    @Test
    public void decimalMismatchWithNulls()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(555, 5))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Alice", 10, 101.01, null)
                .addRow("Bob", 11, 555.55, BigDecimal.valueOf(125, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        Assert.assertFalse(new DataFrameCompare().equal(df1, df2));
        Assert.assertFalse(new DataFrameCompare().equal(df2, df1));
    }

    @Test
    public void simpleMatchIgnoringOrder()
    {
        DataFrame df1 = new DataFrame("DF1")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                ;

        DataFrame df2 = new DataFrame("DF2")
                .addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDecimalColumn("Qux")
                .addRow("Carl", 12, 103.03, BigDecimal.valueOf(127, 2))
                .addRow("Bob", 11, 102.02, BigDecimal.valueOf(125, 2))
                .addRow("Alice", 10, 101.01, BigDecimal.valueOf(123, 2))
                ;

        Assert.assertFalse(new DataFrameCompare().equal(df1, df2));
        Assert.assertFalse(new DataFrameCompare().equal(df2, df1));

        Assert.assertTrue(new DataFrameCompare().equalIgnoreOrder(df1, df2));
        Assert.assertTrue(new DataFrameCompare().equalIgnoreOrder(df2, df1));

        Assert.assertTrue(new DataFrameCompare().equalIgnoreOrder(df1, df1));
    }
}
