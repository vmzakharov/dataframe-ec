package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.factory.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class BasicDataFrameTest
{
    @Test
    public void createSimpleDataFrame()
    {
       DataFrame df = new DataFrame("df1");
       df.addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value");
       df
           .addRow("Alice", 5, 23.45)
           .addRow("Bob",  10, 12.34)
           .addRow("Carl", 11, 56.78)
           .addRow("Deb",   0,  7.89);

       Assert.assertEquals("df1", df.getName());

        Assert.assertEquals(3, df.columnCount());
        Assert.assertEquals(4, df.rowCount());

        Assert.assertEquals("Alice", df.getObject(0, 0));
        Assert.assertEquals(10L, df.getObject(1, 1));
        Assert.assertEquals(56.78, df.getObject(2, 2));

        Assert.assertEquals(11L, df.getObject("Count", 2));
        Assert.assertEquals("Alice", df.getObject("Name", 0));

        Assert.assertEquals(10L, df.getLong("Count", 1));
        Assert.assertEquals(56.78, df.getDouble("Value", 2), 0.0);

        Assert.assertEquals("Alice", df.getValueAsString(0, 0));
        Assert.assertEquals("\"Alice\"", df.getValueAsStringLiteral(0, 0));

        Assert.assertEquals("11", df.getValueAsString(2, 1));
        Assert.assertEquals("11", df.getValueAsStringLiteral(2, 1));
    }

    @Test
    public void addColumn()
    {
        DataFrame dataFrame = new DataFrame("df1")
                .addColumn("String", ValueType.STRING)
                .addColumn("Long", ValueType.LONG)
                .addColumn("Double", ValueType.DOUBLE)
                .addColumn("Date", ValueType.DATE)
                .addColumn("StringComp", ValueType.STRING, "String + \"-meep\"")
                .addColumn("LongComp", ValueType.LONG, "Long * 2")
                .addColumn("DoubleComp", ValueType.DOUBLE, "Double + 10.0")
//                .addColumn("DateComp", ValueType.DATE, "Date")
                ;

        dataFrame.addRow("Beep", 10, 20.0, LocalDate.of(2020, 10, 20));

        Assert.assertEquals("Beep", dataFrame.getString("String", 0));
        Assert.assertEquals("Beep-meep", dataFrame.getString("StringComp", 0));
        Assert.assertEquals(10, dataFrame.getLong("Long", 0));
        Assert.assertEquals(20, dataFrame.getLong("LongComp", 0));
        Assert.assertEquals(20.0, dataFrame.getDouble("Double", 0), 0.000001);
        Assert.assertEquals(30.0, dataFrame.getDouble("DoubleComp", 0), 0.000001);
        Assert.assertEquals(LocalDate.of(2020, 10, 20), dataFrame.getDate("Date", 0));
    }

    @Test
    public void dropColumn()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
            .addRow("Alice", 5, 23.45)
            .addRow("Deb",   0,  7.89);

        df.dropColumn("Count");

        DataFrame expected = new DataFrame("expected")
            .addStringColumn("Name").addDoubleColumn("Value")
            .addRow("Alice", 23.45)
            .addRow("Deb",    7.89);

        DataFrameUtil.assertEquals(expected, df);
    }

    @Test
    public void dropManyColumns()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addStringColumn("Foo")
            .addRow("Alice", 5, 23.45, "abc")
            .addRow("Deb",   0,  7.89, "xyz");

        df.dropColumns(Lists.immutable.of("Value", "Count"));

        DataFrame expected = new DataFrame("expected")
            .addStringColumn("Name").addStringColumn("Foo")
            .addRow("Alice", "abc")
            .addRow("Deb",   "xyz");

        DataFrameUtil.assertEquals(expected, df);
    }

    @Test
    public void keepColumns()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addStringColumn("Foo")
            .addRow("Alice", 5, 23.45, "abc")
            .addRow("Deb",   0,  7.89, "xyz");

        df.keepColumns(Lists.immutable.of("Name", "Foo"));

        DataFrame expected = new DataFrame("expected")
            .addStringColumn("Name").addStringColumn("Foo")
            .addRow("Alice", "abc")
            .addRow("Deb",   "xyz");

        DataFrameUtil.assertEquals(expected, df);
    }

    @Test
    public void dropFirstAndLastColumn()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
            .addRow("Alice", 5, 23.45)
            .addRow("Deb",   0,  7.89);

        df.dropColumn("Name").dropColumn("Value");
        Assert.assertFalse(df.hasColumn("Name"));
        Assert.assertFalse(df.hasColumn("Value"));

        DataFrame expected = new DataFrame("expected")
            .addLongColumn("Count")
            .addRow(5)
            .addRow(0);

        DataFrameUtil.assertEquals(expected, df);
    }

    @Test(expected = RuntimeException.class)
    public void dropNonExistingColumn()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
            .addRow("Alice", 5, 23.45)
            .addRow("Deb",   0,  7.89);

        df.dropColumn("Giraffe");
    }

    @Test(expected = RuntimeException.class)
    public void dropNonExistingColumn2()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
            .addRow("Alice", 5, 23.45)
            .addRow("Deb",   0,  7.89);

        df.dropColumns(Lists.immutable.of("Name", "Giraffe"));
    }

    @Test(expected = RuntimeException.class)
    public void keepNonExistingColumns()
    {
        DataFrame df = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Alice", 5, 23.45)
                .addRow("Deb",   0,  7.89);

        df.keepColumns(Lists.immutable.of("Name", "Giraffe"));
    }
}
