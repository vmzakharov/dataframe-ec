package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class DataFrameValue
extends AbstractValue
{
    private final DataFrame dataFrame;

    public DataFrameValue(DataFrame newDataFrame)
    {
        this.throwExceptionIfNull(newDataFrame);
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
        throw exceptionByKey("DSL_DF_COMPARE_UNSUPPORTED").getUnsupported();
    }
}
