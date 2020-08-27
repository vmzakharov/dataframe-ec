package io.github.vmzakharov.ecdataframe.dataframe;

import org.junit.Assert;
import org.junit.Test;

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
}
