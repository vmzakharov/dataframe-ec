package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DateTimeValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class DfDateTimeColumnComputed
extends DfObjectColumnComputed<LocalDateTime>
implements DfDateTimeColumn
{
    public DfDateTimeColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
    {
        super(newDataFrame, newName, newExpressionAsString);
    }

    @Override
    public LocalDateTime getTypedObject(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        return result.isVoid() ? null : ((DateTimeValue) result).dateTimeValue();
    }
}
