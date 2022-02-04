package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public interface DfDateColumn
extends DfObjectColumn<LocalDate>
{
    DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE;

    @Override
    default String getValueAsString(int rowIndex)
    {
        LocalDate value = this.getTypedObject(rowIndex);
        return value == null ? "" : this.FORMATTER.format(value);
    }

    default ValueType getType()
    {
        return ValueType.DATE;
    }

    @Override
    default void addRowToColumn(int rowIndex, DfColumn target)
    {
        ((DfDateColumnStored) target).addMyType(this.getTypedObject(rowIndex));
    }

    @Override
    default DfCellComparator columnComparator(DfColumn otherColumn)
    {
        DfDateColumn otherStringColumn = (DfDateColumn) otherColumn;

        return (thisRowIndex, otherRowIndex) -> new ComparisonResult.DateComparisonResult(
                this.getTypedObject(this.dataFrameRowIndex(thisRowIndex)),
                otherStringColumn.getTypedObject(otherStringColumn.dataFrameRowIndex(otherRowIndex)));
    }
}
