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
}
