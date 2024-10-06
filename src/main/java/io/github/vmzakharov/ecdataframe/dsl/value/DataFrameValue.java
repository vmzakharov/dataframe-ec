package io.github.vmzakharov.ecdataframe.dsl.value;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public record DataFrameValue(DataFrame dataFrame)
implements Value
{
    public DataFrameValue
    {
        this.throwExceptionIfNull(dataFrame);
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
