package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.Test;

import java.time.LocalDate;

public class DataFrameJoinTest
{
    @Test
    public void simpleJoin()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Blinky", "red",     7)
                .addRow("Pinky",  "pink",    8)
                .addRow("Inky",   "cyan",    9)
                .addRow("Clyde",  "orange", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   2)
                .addRow("Orange",     "orange", 4)
                .addRow("Mint",       "cyan",   3)
                .addRow("Apple",      "red",    1)
                ;

        DataFrame joined = df1.join(df2, "Bar", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                .addRow("Inky",   "cyan",    9, "Mint",       3)
                .addRow("Clyde",  "orange", 10, "Orange",     4)
                .addRow("Pinky",  "pink",    8, "Grapefruit", 2)
                .addRow("Blinky", "red",     7, "Apple",      1)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void duplicateKeys()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Blinky", "pink",  7)
                .addRow("Pinky",  "cyan",  8)
                .addRow("Inky",   "cyan",  9)
                .addRow("Clyde",  "pink", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                .addRow("Grapefruit", "cyan", 2)
                .addRow("Orange",     "pink", 4)
                .addRow("Mint",       "cyan", 3)
                .addRow("Apple",      "pink", 1)
                ;

        DataFrame joined = df1.join(df2, "Bar", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                .addRow("Pinky",  "cyan",  8, "Grapefruit", 2)
                .addRow("Inky",   "cyan",  9, "Mint",       3)
                .addRow("Blinky", "pink",  7, "Orange",     4)
                .addRow("Clyde",  "pink", 10, "Apple",      1)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void multipleKeyJoin()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addStringColumn("Letter").addLongColumn("Baz")
                .addRow("Blinky", "red",    "A",  7)
                .addRow("Pinky",  "pink",   "B",  8)
                .addRow("Inky",   "cyan",   "C",  9)
                .addRow("Clyde",  "orange", "D", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addStringColumn("Code").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   "B", 2)
                .addRow("Orange",     "orange", "D", 4)
                .addRow("Mint",       "cyan",   "C", 3)
                .addRow("Apple",      "red",    "A", 1)
                ;

        DataFrame joined = df1.join(df2, Lists.immutable.of("Bar", "Letter"), Lists.immutable.of("Color", "Code"));

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addStringColumn("Letter").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                .addRow("Inky",   "cyan",   "C",  9, "Mint",       3)
                .addRow("Clyde",  "orange", "D", 10, "Orange",     4)
                .addRow("Pinky",  "pink",   "B",  8, "Grapefruit", 2)
                .addRow("Blinky", "red",    "A",  7, "Apple",      1)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void multipleKeyJoinWithDateKey()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addDateColumn("Date").addLongColumn("Baz")
                .addRow("Blinky", "red",    LocalDate.of(2021, 11, 12),  7)
                .addRow("Pinky",  "pink",   LocalDate.of(2021, 11, 13),  8)
                .addRow("Inky",   "cyan",   LocalDate.of(2021, 11, 14),  9)
                .addRow("Clyde",  "orange", LocalDate.of(2021, 11, 15), 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addDateColumn("Code").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   LocalDate.of(2021, 11, 13), 2)
                .addRow("Orange",     "orange", LocalDate.of(2021, 11, 15), 4)
                .addRow("Mint",       "cyan",   LocalDate.of(2021, 11, 14), 3)
                .addRow("Apple",      "red",    LocalDate.of(2021, 11, 12), 1)
                ;

        DataFrame joined = df1.join(df2, Lists.immutable.of("Bar", "Date"), Lists.immutable.of("Color", "Code"));

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addDateColumn("Date").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                .addRow("Inky",   "cyan",   LocalDate.of(2021, 11, 14),  9, "Mint",       3)
                .addRow("Clyde",  "orange", LocalDate.of(2021, 11, 15), 10, "Orange",     4)
                .addRow("Pinky",  "pink",   LocalDate.of(2021, 11, 13),  8, "Grapefruit", 2)
                .addRow("Blinky", "red",    LocalDate.of(2021, 11, 12),  7, "Apple",      1)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void multipleKeyJoinWithMismatchedKeys()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addStringColumn("Letter").addLongColumn("Baz")
                .addRow("Pinky",  "pink",   "B",  8)
                .addRow("Inky",   "cyan",   "C",  9)
                .addRow("Clyde",  "orange", "D", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addStringColumn("Code").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   "B", 2)
                .addRow("Orange",     "orange", "D", 4)
                .addRow("Apple",      "red",    "A", 1)
                ;

        DataFrame joined = df1.join(df2, Lists.immutable.of("Bar", "Letter"), Lists.immutable.of("Color", "Code"));

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addStringColumn("Letter").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                .addRow("Clyde",  "orange", "D", 10, "Orange",     4)
                .addRow("Pinky",  "pink",   "B",  8, "Grapefruit", 2)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void multipleKeyOuterJoinWithMismatchedKeys()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addStringColumn("Letter").addLongColumn("Baz")
                .addRow("Pinky",  "pink",   "B",  8)
                .addRow("Inky",   "cyan",   "C",  9)
                .addRow("Clyde",  "orange", "D", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addStringColumn("Code").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   "B", 2)
                .addRow("Orange",     "orange", "D", 4)
                .addRow("Apple",      "red",    "A", 1)
                ;

        DataFrame joined = df1.outerJoin(df2, Lists.immutable.of("Bar", "Letter"), Lists.immutable.of("Color", "Code"));

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addStringColumn("Letter").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                .addRow("Inky",   "cyan",   "C",    9,  null,     null)
                .addRow("Clyde",  "orange", "D",   10, "Orange",     4)
                .addRow("Pinky",  "pink",   "B",    8, "Grapefruit", 2)
                .addRow(null,     "red",    "A", null, "Apple",      1)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void joinByLongColumn()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addLongColumn("Baz")
                .addRow("Blinky",  7)
                .addRow("Pinky",   8)
                .addRow("Inky",    9)
                .addRow("Clyde",  10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addLongColumn("Number")
                .addRow("Grapefruit",  8)
                .addRow("Orange",     10)
                .addRow("Mint",        9)
                .addRow("Apple",       7)
                ;

        DataFrame joined = df1.join(df2, "Baz", "Number");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addLongColumn("Baz").addStringColumn("Name")
                .addRow("Blinky", 7, "Apple")
                .addRow("Pinky",  8, "Grapefruit")
                .addRow("Inky",   9, "Mint")
                .addRow("Clyde", 10, "Orange")
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void joinByComputedColumns()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("AbcBar").addLongColumn("Baz")
                .addRow("Blinky", "abcred",     7)
                .addRow("Pinky",  "abcpink",    8)
                .addRow("Inky",   "abccyan",    9)
                ;

        df1.addStringColumn("Bar", "substr(AbcBar, 3)");

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   2)
                .addRow("Mint",       "cyan",   3)
                .addRow("Apple",      "red",    1)
                ;

        DataFrame joined = df1.join(df2, "Bar", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("AbcBar").addLongColumn("Baz").addStringColumn("Bar").addStringColumn("Name").addLongColumn("Number")
                .addRow("Inky",   "abccyan", 9, "cyan", "Mint",       3)
                .addRow("Pinky",  "abcpink", 8, "pink", "Grapefruit", 2)
                .addRow("Blinky", "abcred",  7, "red",  "Apple",      1)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void joinWithComputedColumns()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz", "123")
                .addRow("Blinky", "red")
                .addRow("Pinky",  "pink")
                .addRow("Inky",   "cyan")
                .addRow("Clyde",  "orange")
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number").addStringColumn("Counted", "'One ' + Name")
                .addRow("Grapefruit", "pink",   2)
                .addRow("Orange",     "orange", 4)
                .addRow("Mint",       "cyan",   3)
                .addRow("Apple",      "red",    1)
                ;

        DataFrame joined = df1.join(df2, "Bar", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number").addStringColumn("Counted")
                .addRow("Inky",   "cyan",   123, "Mint",       3, "One Mint")
                .addRow("Clyde",  "orange", 123, "Orange",     4, "One Orange")
                .addRow("Pinky",  "pink",   123, "Grapefruit", 2, "One Grapefruit")
                .addRow("Blinky", "red",    123, "Apple",      1, "One Apple")
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void joinWithMismatchedKeys()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Blinky", "red",     7)
                .addRow("Inky",   "cyan",    9)
                .addRow("Clyde",  "orange", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   2)
                .addRow("Orange",     "orange", 4)
                .addRow("Apple",      "red",    1)
                ;

        DataFrame joined = df1.join(df2, "Bar", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                .addRow("Clyde",  "orange", 10, "Orange",     4)
                .addRow("Blinky", "red",     7, "Apple",      1)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void joinDifferentSizes()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Blinky", "red",     7)
                .addRow("Pinky",  "pink",    8)
                .addRow("Inky",   "cyan",    9)
                .addRow("Clyde",  "orange", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   2)
                .addRow("Mint",       "cyan",   3)
                ;

        DataFrame joined = df1.join(df2, "Bar", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                .addRow("Inky",   "cyan",    9, "Mint",       3)
                .addRow("Pinky",  "pink",    8, "Grapefruit", 2)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void joinToEmpty()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Blinky", "red",     7)
                .addRow("Pinky",  "pink",    8)
                .addRow("Inky",   "cyan",    9)
                .addRow("Clyde",  "orange", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                ;

        DataFrame joined = df1.join(df2, "Bar", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void joinEmptyToEmpty()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                ;

        DataFrame joined = df1.join(df2, "Bar", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void outerJoinWithAllRowsMatching()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Blinky", "red",     7)
                .addRow("Pinky",  "pink",    8)
                .addRow("Inky",   "cyan",    9)
                .addRow("Clyde",  "orange", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   2)
                .addRow("Orange",     "orange", 4)
                .addRow("Mint",       "cyan",   3)
                .addRow("Apple",      "red",    1)
                ;

        DataFrame joined = df1.outerJoin(df2, "Bar", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                .addRow("Inky",   "cyan",    9, "Mint",       3)
                .addRow("Clyde",  "orange", 10, "Orange",     4)
                .addRow("Pinky",  "pink",    8, "Grapefruit", 2)
                .addRow("Blinky", "red",     7, "Apple",      1)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void outerJoinWithMismatchedKeys()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Blinky", "red",     7)
                .addRow("Inky",   "cyan",    9)
                .addRow("Clyde",  "orange", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   2)
                .addRow("Orange",     "orange", 4)
                .addRow("Apple",      "red",    1)
                ;

        DataFrame joined = df1.outerJoin(df2, "Bar", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                .addRow("Inky",   "cyan",    9, null,         0)
                .addRow("Clyde",  "orange", 10, "Orange",     4)
                .addRow(null,     "pink", null, "Grapefruit", 2)
                .addRow("Blinky", "red",     7, "Apple",      1)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void outerJoinWithOneSideHanging()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Inky",   "cyan",    9)
                .addRow("Clyde",  "orange", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   2)
                .addRow("Orange",     "orange", 4)
                .addRow("Apple",      "red",    1)
                ;

        DataFrame joined = df1.outerJoin(df2, "Bar", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                .addRow("Inky",   "cyan",    9, null,         0)
                .addRow("Clyde",  "orange", 10, "Orange",     4)
                .addRow(null,     "pink", null, "Grapefruit", 2)
                .addRow(null,     "red",     0, "Apple",      1)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void outerJoinWithBothSidesHanging()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz")
                .addRow("Blinky", "red",     7)
                .addRow("Inky",   "cyan",    9)
                .addRow("Clyde",  "orange", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   2)
                .addRow("Orange",     "orange", 4)
                .addRow("Mint",       "cyan",   3)
                ;

        DataFrame joined = df1.outerJoin(df2, "Bar", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Bar").addLongColumn("Baz").addStringColumn("Name").addLongColumn("Number")
                .addRow("Inky",   "cyan",    9, "Mint",       3)
                .addRow("Clyde",  "orange", 10, "Orange",     4)
                .addRow(null,     "pink", null, "Grapefruit", 2)
                .addRow("Blinky", "red",     7, null,         0)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void simpleJoinWithNameCollision()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                .addRow("Pinky",  "pink",    8)
                .addRow("Clyde",  "orange", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number")
                .addRow("Grapefruit", "pink",   2)
                .addRow("Orange",     "orange", 4)
                ;

        DataFrame joined = df1.join(df2, "Color", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Number").addStringColumn("Name_B").addLongColumn("Number_B")
                .addRow("Clyde",  "orange", 10, "Orange",     4)
                .addRow("Pinky",  "pink",    8, "Grapefruit", 2)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }

    @Test
    public void simpleJoinWithNameConfusion()
    {
        DataFrame df1 = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Foo_B_B").addLongColumn("Foo_B_B_B")
                .addRow("Pinky",  "pink",    8)
                .addRow("Clyde",  "orange", 10)
                ;

        DataFrame df2 = new DataFrame("df2")
                .addStringColumn("Foo").addStringColumn("Color").addLongColumn("Foo_B")
                .addRow("Grapefruit", "pink",   2)
                .addRow("Orange",     "orange", 4)
                ;

        DataFrame joined = df1.join(df2, "Foo_B_B", "Color");

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Foo").addStringColumn("Foo_B_B").addLongColumn("Foo_B_B_B").addStringColumn("Foo_B").addLongColumn("Foo_B_B_B_B")
                .addRow("Clyde",  "orange", 10, "Orange",     4)
                .addRow("Pinky",  "pink",    8, "Grapefruit", 2)
                ;

        DataFrameUtil.assertEquals(expected, joined);
    }
}
