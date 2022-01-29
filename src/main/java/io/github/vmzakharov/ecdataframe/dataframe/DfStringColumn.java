package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ImmutableList;

public interface DfStringColumn
extends DfColumn
{
    String getString(int rowIndex);

    @Override
    default String getValueAsString(int rowIndex)
    {
        return this.getString(rowIndex);
    }

    @Override
    default String getValueAsStringLiteral(int rowIndex)
    {
        String value = this.getString(rowIndex);
        return value == null ? "" : '"' + this.getValueAsString(rowIndex) + '"';
    }

    @Override
    default Object getObject(int rowIndex)
    {
        return this.getString(rowIndex);
    }

    @Override
    default boolean isNull(int rowIndex)
    {
        return this.getObject(rowIndex) == null;
    }

    @Override
    default Value getValue(int rowIndex)
    {
        return new StringValue(this.getString(rowIndex));
    }

    ImmutableList<String> toList();

    default ValueType getType()
    {
        return ValueType.STRING;
    }

    @Override
    default void addRowToColumn(int rowIndex, DfColumn target)
    {
        ((DfStringColumnStored) target).addString(this.getString(rowIndex));
    }

    @Override
    default DfCellComparator columnComparator(DfColumn otherColumn)
    {
        DfStringColumn otherStringColumn = (DfStringColumn) otherColumn;

        return (thisRowIndex, otherRowIndex) -> new ComparisonResult.StringComparisonResult(
                this.getString(this.dataFrameRowIndex(thisRowIndex)),
                otherStringColumn.getString(otherStringColumn.dataFrameRowIndex(otherRowIndex)));
    }
}
