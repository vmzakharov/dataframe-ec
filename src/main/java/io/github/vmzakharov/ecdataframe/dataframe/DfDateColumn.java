package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public interface DfDateColumn
extends DfColumn
{
    static public DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE;

    LocalDate getDate(int rowIndex);

    @Override
    default String getValueAsString(int rowIndex)
    {
        LocalDate value = this.getDate(rowIndex);
        return value == null ? "" : this.FORMATTER.format(value);
    }

    @Override
    default Object getObject(int rowIndex)
    {
        return this.getDate(rowIndex);
    }

    @Override
    default Value getValue(int rowIndex)
    {
        return new DateValue(this.getDate(rowIndex));
    }

    default ValueType getType()
    {
        return ValueType.DATE;
    }

    @Override
    default void addRowToColumn(int rowIndex, DfColumn target)
    {
        ((DfDateColumnStored) target).addDate(this.getDate(rowIndex));
    }

    @Override
    default boolean isNull(int rowIndex)
    {
        return this.getDate(rowIndex) == null;
    }

    @Override
    default DfCellComparator columnComparator(DfColumn otherColumn)
    {
        DfDateColumn otherStringColumn = (DfDateColumn) otherColumn;

        return (thisRowIndex, otherRowIndex) -> new ComparisonResult.DateComparisonResult(
                this.getDate(this.dataFrameRowIndex(thisRowIndex)),
                otherStringColumn.getDate(otherStringColumn.dataFrameRowIndex(otherRowIndex)));
    }
}
