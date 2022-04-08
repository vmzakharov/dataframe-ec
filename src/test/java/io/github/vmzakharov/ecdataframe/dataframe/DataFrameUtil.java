package io.github.vmzakharov.ecdataframe.dataframe;

import org.junit.Assert;

final public class DataFrameUtil
{
    private DataFrameUtil()
    {
        // Utility class
    }

    static public void assertEquals(DataFrame expected, DataFrame actual)
    {

        if (expected.rowCount() != actual.rowCount() || expected.columnCount() != actual.columnCount())
        {
            Assert.fail("Dimensions don't match: expected rows " + expected.rowCount() + ", cols " + expected.columnCount()
                    + ", actual rows " + actual.rowCount() + ", cols " + actual.columnCount());
        }

        Assert.assertEquals("Column names",
                expected.getColumns().collect(DfColumn::getName), actual.getColumns().collect(DfColumn::getName)
        );

        Assert.assertEquals("Column types",
                expected.getColumns().collect(DfColumn::getType), actual.getColumns().collect(DfColumn::getType)
        );

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

    static public void assertEqualsIgnoreOrder(DataFrame expected, DataFrame actual)
    {
        Assert.fail("assert method not implemented");
    }
}
