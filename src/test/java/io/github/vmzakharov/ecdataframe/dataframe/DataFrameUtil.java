package io.github.vmzakharov.ecdataframe.dataframe;

import org.junit.Assert;

import java.math.BigDecimal;

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
                Object expectedValue = expected.getObject(rowIndex, colIndex);
                Object actualValue = actual.getObject(rowIndex, colIndex);

                if (expectedValue instanceof BigDecimal)
                {
                    Assert.assertTrue("Invalid type of the actual value: " + actualValue.getClass().getName()
                                    + " expected: BigDecimal",
                            actualValue instanceof BigDecimal);

                    Assert.assertEquals("Different values in row " + rowIndex + ", column " + colIndex
                                    + " expected: " + expectedValue + ", actual: " + actualValue
                                    + " (showing the result of expected.compareTo(actual)",
                            0, ((BigDecimal) expectedValue).compareTo((BigDecimal) actualValue));
                }
                else
                {
                    Assert.assertEquals("Different values in row " + rowIndex + ", column " + colIndex,
                            expectedValue, actualValue);
                }
            }
        }
    }

    static public void assertEqualsIgnoreOrder(DataFrame expected, DataFrame actual)
    {
        Assert.fail("assert method not implemented");
    }
}
