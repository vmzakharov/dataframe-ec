package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DateTimeValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public interface DfDateTimeColumn
extends DfObjectColumn<LocalDateTime>
{
    DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    @Override
    default String getValueAsString(int rowIndex)
    {
        LocalDateTime value = this.getTypedObject(rowIndex);
        return value == null ? "" : this.FORMATTER.format(value);
    }

    default ValueType getType()
    {
        return ValueType.DATE_TIME;
    }

    @Override
    default Value objectToValue(LocalDateTime anObject)
    {
        return new DateTimeValue(anObject);
    }

    @Override
    default void addRowToColumn(int rowIndex, DfColumn target)
    {
        ((DfDateTimeColumnStored) target).addMyType(this.getTypedObject(rowIndex));
    }

    @Override
    default DfCellComparator columnComparator(DfColumn otherColumn)
    {
        DfDateTimeColumn otherStringColumn = (DfDateTimeColumn) otherColumn;

        return (thisRowIndex, otherRowIndex) -> new ComparisonResult.DateTimeComparisonResult(
                this.getTypedObject(this.dataFrameRowIndex(thisRowIndex)),
                otherStringColumn.getTypedObject(otherStringColumn.dataFrameRowIndex(otherRowIndex)));
    }
}
