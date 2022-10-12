package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.compare.DecimalComparisonResult;
import io.github.vmzakharov.ecdataframe.dsl.value.DecimalValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

import java.math.BigDecimal;

public interface DfDecimalColumn
extends DfObjectColumn<BigDecimal>
{
    @Override
    default String getValueAsString(int rowIndex)
    {
        BigDecimal value = this.getTypedObject(rowIndex);
        return value == null ? "" : value.toString();
    }

    default ValueType getType()
    {
        return ValueType.DECIMAL;
    }

    @Override
    default Value objectToValue(BigDecimal anObject)
    {
        return new DecimalValue(anObject);
    }

    @Override
    default void addRowToColumn(int rowIndex, DfColumn target)
    {
        target.addObject(this.getTypedObject(rowIndex));
    }

    @Override
    default DfCellComparator columnComparator(DfColumn otherColumn)
    {
        DfDecimalColumn otherStringColumn = (DfDecimalColumn) otherColumn;

        return (thisRowIndex, otherRowIndex) -> new DecimalComparisonResult(
                this.getTypedObject(this.dataFrameRowIndex(thisRowIndex)),
                otherStringColumn.getTypedObject(otherStringColumn.dataFrameRowIndex(otherRowIndex)));
    }
}
