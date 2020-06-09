package org.modelscript.dataframe;

import org.junit.Assert;

public class DataFrameUtil
{
    static void assertEquals(DataFrame expected, DataFrame actual)
    {

        if (expected.rowCount() != actual.rowCount() || expected.columnCount() != actual.columnCount())
        {
            Assert.fail("Dimensions don't match: expected rows " + expected.rowCount() + ", cols " + expected.columnCount()
                    + ", actual rows " + actual.rowCount() + ", cols " + actual.columnCount());
        }

        int colCount = expected.columnCount();
        int rowCount = expected.rowCount();

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            for (int colIndex = 0; colIndex < colCount; colIndex++)
            {
                Assert.assertEquals("Different values in row " + rowIndex + ", column " + colIndex,
                        expected.getObject(rowIndex, colIndex), actual.getObject(rowIndex, colIndex));
            }
        }
    }

    static public boolean assertEqualsIgnoreOrder(DataFrame expected, DataFrame actual)
    {

        Assert.fail("assert method not implemented");
        return false;
    }
}
