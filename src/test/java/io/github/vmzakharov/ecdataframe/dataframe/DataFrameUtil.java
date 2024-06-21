package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.util.DataFrameCompare;

import static org.junit.jupiter.api.Assertions.*;

final public class DataFrameUtil
{
    private DataFrameUtil()
    {
        // Utility class
    }

    static public void assertEquals(DataFrame expected, DataFrame actual)
    {
        DataFrameCompare comparer = new DataFrameCompare();

        if (!comparer.equal(expected, actual))
        {
            fail(comparer.reason());
        }
    }

    static public void assertEquals(DataFrame expected, DataFrame actual, double tolerance)
    {
        DataFrameCompare comparer = new DataFrameCompare();

        if (!comparer.equal(expected, actual, tolerance))
        {
            fail(comparer.reason());
        }
    }
}
