package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DecimalValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import java.math.BigDecimal;

public class DfDecimalColumnComputed
extends DfObjectColumnComputed<BigDecimal>
implements DfDecimalColumn
{
    public DfDecimalColumnComputed(DataFrame newDataFrame, String newName, String newExpressionAsString)
    {
        super(newDataFrame, newName, newExpressionAsString);
    }

    @Override
    public BigDecimal getTypedObject(int rowIndex)
    {
        Value result = this.getValue(rowIndex);

        return result.isVoid() ? null : ((DecimalValue) result).decimalValue();
    }
}
