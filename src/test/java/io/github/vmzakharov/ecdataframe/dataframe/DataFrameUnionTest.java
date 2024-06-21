package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

public class DataFrameUnionTest
{
    @Test
    public void simpleUnion()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addIntColumn("IntValue")
                .addRow("Alice", 5, 23.45, 50)
                .addRow("Bob", 10, 12.34, 100)
                .addRow("Carl", 11, 56.78, 110)
                .addRow("Deb", 0, 7.89, 0);

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addIntColumn("IntValue")
                .addRow("Grace", 1, 13.45, 10)
                .addRow("Heidi", 2, 22.34, 20)
                .addRow("Ivan", 3, 36.78, 30)
                .addRow("Judy", 4, 47.89, 40);

        DataFrame union = df1.union(df2);

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addIntColumn("IntValue")
                .addRow("Alice", 5, 23.45, 50)
                .addRow("Bob",  10, 12.34, 100)
                .addRow("Carl", 11, 56.78, 110)
                .addRow("Deb",   0,  7.89, 0)
                .addRow("Grace", 1, 13.45, 10)
                .addRow("Heidi", 2, 22.34, 20)
                .addRow("Ivan",  3, 36.78, 30)
                .addRow("Judy",  4, 47.89, 40);

        DataFrameUtil.assertEquals(expected, union);
    }

    @Test
    public void unionWithNulls()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addDateColumn("Date")
                .addRow(null, 5, 23.45, LocalDate.of(2020, 7, 10))
                .addRow("Bob", null, 12.34, LocalDate.of(2020, 7, 10))
                .addRow("Carl", 11, null, LocalDate.of(2020, 7, 10))
                .addRow("Deb", 0, 7.89, null)
                ;

        DataFrame df2 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addDateColumn("Date")
                .addRow("Grace", 1, 13.45, null)
                .addRow("Heidi", 2, null, LocalDate.of(2020, 8, 11))
                .addRow("Ivan", null, 36.78, LocalDate.of(2020, 8, 12))
                .addRow(null, 4, 47.89, LocalDate.of(2020, 8, 10))
                ;

        DataFrame union = df1.union(df2);

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addDateColumn("Date")
                .addRow(null, 5, 23.45, LocalDate.of(2020, 7, 10))
                .addRow("Bob", null, 12.34, LocalDate.of(2020, 7, 10))
                .addRow("Carl", 11, null, LocalDate.of(2020, 7, 10))
                .addRow("Deb", 0, 7.89, null)
                .addRow("Grace", 1, 13.45, null)
                .addRow("Heidi", 2, null, LocalDate.of(2020, 8, 11))
                .addRow("Ivan", null, 36.78, LocalDate.of(2020, 8, 12))
                .addRow(null, 4, 47.89, LocalDate.of(2020, 8, 10));

        DataFrameUtil.assertEquals(expected, union);
    }

    @Test
    public void unionWithComputedNulls()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addDateColumn("Date")
                .addRow(null, 5, 23.45, LocalDate.of(2020, 7, 10))
                .addRow("Bob", null, 12.34, LocalDate.of(2020, 7, 10))
                .addRow("Carl", 11, null, LocalDate.of(2020, 7, 10))
                .addRow("Deb", 0, 7.89, null)
                ;
        df1
                .addStringColumn("NameX", "Name + 'X'")
                .addLongColumn("CountMore", "Count + 1")
                .addDoubleColumn("TwoValues", "2 * Value");

        DataFrame df2 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addDateColumn("Date")
                .addRow("Grace", 1, 13.45, null)
                .addRow("Heidi", 2, null, LocalDate.of(2020, 8, 11))
                .addRow("Ivan", null, 36.78, LocalDate.of(2020, 8, 12))
                .addRow(null, 4, 47.89, LocalDate.of(2020, 8, 10))
                ;

        df2
                .addStringColumn("NameX", "'X' + Name")
                .addLongColumn("CountMore", "Count + Count")
                .addDoubleColumn("TwoValues", "Value * 2");

        DataFrame union = df1.union(df2);

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addDateColumn("Date")
                .addStringColumn("NameX")
                .addLongColumn("CountMore")
                .addDoubleColumn("TwoValues")
                .addRow(null, 5, 23.45, LocalDate.of(2020, 7, 10), null, 6, 46.90)
                .addRow("Bob", null, 12.34, LocalDate.of(2020, 7, 10), "BobX", null, 24.68)
                .addRow("Carl", 11, null, LocalDate.of(2020, 7, 10), "CarlX", 12, null)
                .addRow("Deb", 0, 7.89, null, "DebX", 1, 15.78)
                .addRow("Grace", 1, 13.45, null, "XGrace", 2, 26.90)
                .addRow("Heidi", 2, null, LocalDate.of(2020, 8, 11), "XHeidi", 4, null)
                .addRow("Ivan", null, 36.78, LocalDate.of(2020, 8, 12), "XIvan", null, 73.56)
                .addRow(null, 4, 47.89, LocalDate.of(2020, 8, 10), null, 8, 95.78);

        DataFrameUtil.assertEquals(expected, union);
    }

    @Test
    public void unionOfSorted()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Alice", 10, 23.45)
                .addRow("Bob", 5, 12.34)
                .addRow("Carl", 11, 56.78)
                .addRow("Deb", 0, 7.89);

        df1.sortBy(Lists.immutable.of("Count"));

        DataFrame df2 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Grace", 5, 13.45)
                .addRow("Heidi", 2, 22.34)
                .addRow("Ivan", 3, 36.78)
                .addRow("Judy", 4, 47.89);

        df2.sortBy(Lists.immutable.of("Count"));

        DataFrame union = df1.union(df2);

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Alice", 10, 23.45)
                .addRow("Bob", 5, 12.34)
                .addRow("Carl", 11, 56.78)
                .addRow("Deb", 0, 7.89)
                .addRow("Grace", 5, 13.45)
                .addRow("Heidi", 2, 22.34)
                .addRow("Ivan", 3, 36.78)
                .addRow("Judy", 4, 47.89)
                ;

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

        DataFrame df2 = new DataFrame("df2")
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

    @Test
    public void unionOfEmptyWithComputedColumns()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                ;

        df1.addStringColumn("aName", "\"A\" + Name").addLongColumn("MoreCount", "Count + 1").addDoubleColumn("MoreValue", "Value * 10");

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Ivan", 3, 36.78)
                .addRow("Judy", 4, 47.89)
                ;

        df2.addStringColumn("aName", "\"B\" + Name").addLongColumn("MoreCount", "Count + 2").addDoubleColumn("MoreValue", "Value + 1");

        DataFrame df3 = new DataFrame("df3")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                ;

        df3.addStringColumn("aName", "\"C\" + Name").addLongColumn("MoreCount", "Count + 3").addDoubleColumn("MoreValue", "Value + 11");

        DataFrame union = df1.union(df2).union(df3);

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addStringColumn("aName").addLongColumn("MoreCount").addDoubleColumn("MoreValue")
                .addRow("Ivan",  3, 36.78, "BIvan",   5, 37.78)
                .addRow("Judy",  4, 47.89, "BJudy",   6, 48.89)
                ;

        DataFrameUtil.assertEquals(expected, union);
    }

    @Test
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

        Assertions.assertThrows(RuntimeException.class, () -> df1.union(df2));
    }

    @Test
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

        Assertions.assertThrows(RuntimeException.class, () -> df1.union(df2));
    }

    @Test
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

        Assertions.assertThrows(RuntimeException.class, () -> df1.union(df2));
    }
}
