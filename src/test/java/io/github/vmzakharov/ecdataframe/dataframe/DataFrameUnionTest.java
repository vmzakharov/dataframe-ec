package io.github.vmzakharov.ecdataframe.dataframe;

import org.junit.Test;

public class DataFrameUnionTest
{
    @Test
    public void simpleUnion()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Alice", 5, 23.45)
                .addRow("Bob", 10, 12.34)
                .addRow("Carl", 11, 56.78)
                .addRow("Deb", 0, 7.89);

        DataFrame df2 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Grace", 1, 13.45)
                .addRow("Heidi", 2, 22.34)
                .addRow("Ivan", 3, 36.78)
                .addRow("Judy", 4, 47.89);

        DataFrame union = df1.union(df2);

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Alice", 5, 23.45)
                .addRow("Bob",  10, 12.34)
                .addRow("Carl", 11, 56.78)
                .addRow("Deb",   0,  7.89)
                .addRow("Grace", 1, 13.45)
                .addRow("Heidi", 2, 22.34)
                .addRow("Ivan",  3, 36.78)
                .addRow("Judy",  4, 47.89);

        DataFrameUtil.assertEquals(expected, union);
    }

    @Test
    public void unionWithConstantComputedColumn()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count", "7").addStringColumn("Text").addDoubleColumn("Value", "2.20")
                .addRow("Alice", null, "abc")
                .addRow("Bob",   null, "xyz")
                ;

        DataFrame df2 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addStringColumn("Text").addDoubleColumn("Value", "1.10")
                .addRow("Grace", 1, "def")
                .addRow("Heidi", 2, "jkl")
                ;

        DataFrame union = df1.union(df2);

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Name").addLongColumn("Count").addStringColumn("Text").addDoubleColumn("Value")
                .addRow("Alice", 7, "abc", 2.20)
                .addRow("Bob",   7, "xyz", 2.20)
                .addRow("Grace", 1, "def", 1.10)
                .addRow("Heidi", 2, "jkl", 1.10)
                ;

        DataFrameUtil.assertEquals(expected, union);
    }

    @Test
    public void unionWithComputedColumns()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Alice", 5, 23.45)
                .addRow("Bob", 10, 12.34)
                ;

        df1.addStringColumn("aName", "\"A\" + Name").addLongColumn("MoreCount", "Count + 1").addDoubleColumn("MoreValue", "Value * 10");

        DataFrame df2 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Ivan", 3, 36.78)
                .addRow("Judy", 4, 47.89)
                ;

        df2.addStringColumn("aName", "\"B\" + Name").addLongColumn("MoreCount", "Count + 2").addDoubleColumn("MoreValue", "Value + 1");

        DataFrame union = df1.union(df2);

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addStringColumn("aName").addLongColumn("MoreCount").addDoubleColumn("MoreValue")
                .addRow("Alice", 5, 23.45, "AAlice",  6, 234.5)
                .addRow("Bob",  10, 12.34, "ABob",   11, 123.4)
                .addRow("Ivan",  3, 36.78, "BIvan",   5, 37.78)
                .addRow("Judy",  4, 47.89, "BJudy",   6, 48.89)
                ;

        DataFrameUtil.assertEquals(expected, union);
    }

    @Test(expected = RuntimeException.class)
    public void unionWithMismatchedSchemasFails()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Col1").addLongColumn("Col2").addDoubleColumn("Abracadabra")
                .addRow("Alice", 5, 23.45)
                .addRow("Bob",  10, 12.34)
                ;

        DataFrame df2 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Ivan", 3, 36.78)
                .addRow("Judy", 4, 47.89)
                ;

        df1.union(df2);
    }

    @Test(expected = RuntimeException.class)
    public void unionWithMismatchedSchemasFails2()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Alice", 5, 23.45)
                .addRow("Bob",  10, 12.34)
                ;

        DataFrame df2 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addStringColumn("Meh")
                .addRow("Ivan", 3, 36.78, "Meh")
                .addRow("Judy", 4, 47.89, "Meh")
                ;

        df1.union(df2);
    }

    @Test(expected = RuntimeException.class)
    public void unionWithMismatchedTypesFails()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Alice", 5, 23.45)
                .addRow("Bob",  10, 12.34)
                ;

        DataFrame df2 = new DataFrame("df1")
                .addStringColumn("Name").addStringColumn("Count").addDoubleColumn("Value")
                .addRow("Ivan", "Dracula", 36.78)
                .addRow("Judy", "Monte Cristo", 47.89)
                ;

        df1.union(df2);
    }
}
