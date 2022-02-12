package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;

public class DfStringColumnComputed
extends DfObjectColumnComputed<String>
implements DfStringColumn
{
    public DfStringColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
    {
        super(newDataFrame, newName, newExpressionAsString);
    }

    @Override
    public String getTypedObject(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        return result.isVoid() ? null : result.stringValue();
    }
}
