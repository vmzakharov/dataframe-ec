package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.util.ErrorReporter;

public class DataFrameValue
implements Value
{
    private final DataFrame dataFrame;

    public DataFrameValue(DataFrame newDataFrame)
    {
        ErrorReporter.reportAndThrowIf(newDataFrame == null, "DataFrame value cannot contain null, a void value should be used instead");
        this.dataFrame = newDataFrame;
    }

    public DataFrame dataFrameValue()
    {
        return this.dataFrame;
    }

    @Override
    public String asStringLiteral()
    {
        return "DataFrame [" + this.dataFrame.getName() + ", rows: " + this.dataFrame.rowCount() + ", columns: " + this.dataFrame.columnCount() + "] ";
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
