package org.modelscript.expr.value;

import org.modelscript.dataframe.DataFrame;

public class DataFrameValue
implements Value
{
    private final DataFrame dataFrame;

    public DataFrameValue(DataFrame newDataFrame)
    {
        this.dataFrame = newDataFrame;
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
}
