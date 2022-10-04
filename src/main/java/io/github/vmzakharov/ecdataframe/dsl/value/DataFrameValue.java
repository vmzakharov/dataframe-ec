package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.ErrorReporter;

public class DataFrameValue
implements Value
{
    private final DataFrame dataFrame;

    public DataFrameValue(DataFrame newDataFrame)
    {
        this.dataFrame = newDataFrame;
    }

    public DataFrame dataFrameValue()
    {
        return this.dataFrame;
    }

    @Override
    public String asStringLiteral()
    {
        return "DataFrame [" + this.dataFrame.getName() + "] " + this.dataFrame.asCsvString();
    }

    @Override
    public ValueType getType()
    {
        return ValueType.DATA_FRAME;
    }

    @Override
    public String stringValue()
    {
        return this.dataFrame.asCsvString();
    }

    @Override
    public int compareTo(Value o)
    {
        throw ErrorReporter.unsupported("Data Frame value comparison is not supported");
    }
}
