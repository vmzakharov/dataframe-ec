package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import java.time.LocalDate;

public class DfDateColumnComputed
extends DfObjectColumnComputed<LocalDate>
implements DfDateColumn
{
    public DfDateColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
    {
        super(newDataFrame, newName, newExpressionAsString);
    }

    @Override
    public LocalDate getTypedObject(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        return result.isVoid() ? null : ((DateValue) result).dateValue();
    }
}
