package org.modelscript.dataframe;

import org.junit.Assert;
import org.junit.Test;

public class SimpleDataFrameTest
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
    }
}
