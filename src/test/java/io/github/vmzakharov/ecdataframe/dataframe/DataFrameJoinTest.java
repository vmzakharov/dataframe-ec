package io.github.vmzakharov.ecdataframe.dataframe;

import org.junit.Test;

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
                .addStringColumn("Foo").addLongColumn("Bar").   addStringColumn("Name")
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
                .addRow("Inky",   "cyan",   123, "Mint",       3, "One Mint"      )
                .addRow("Clyde",  "orange", 123, "Orange",     4, "One Orange"    )
                .addRow("Pinky",  "pink",   123, "Grapefruit", 2, "One Grapefruit")
                .addRow("Blinky", "red",    123, "Apple",      1, "One Apple"     )
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
