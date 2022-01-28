package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

abstract public class DfDateColumn
extends DfObjectColumn<LocalDate>
{
    private DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE;

    public DfDateColumn(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    abstract public LocalDate getDate(int rowIndex);

    @Override
    public String getValueAsString(int rowIndex)
    {
        LocalDate value = this.getDate(rowIndex);
        return value == null ? "" : this.formatter.format(value);
    }

//    @Override
//    public String getValueAsStringLiteral(int rowIndex)
//    {
//        return this.getValueAsString(rowIndex);
//    }

    @Override
    public Object getObject(int rowIndex)
    {
        return this.getDate(rowIndex);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        return new DateValue(this.getDate(rowIndex));
    }

    public ValueType getType()
    {
        return ValueType.DATE;
    }

    @Override
    public void addRowToColumn(int rowIndex, DfColumn target)
    {
        ((DfDateColumnStored) target).addDate(this.getDate(rowIndex));
    }

    @Override
    public boolean isNull(int rowIndex)
    {
        return this.getDate(rowIndex) == null;
    }

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        DfDateColumn mergedCol = (DfDateColumn) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItems(this.toList());
        mergedCol.addAllItems(((DfDateColumn) other).toList());
        return mergedCol;
    }

    @Override
    public DfCellComparator columnComparator(DfColumn otherColumn)
    {
        DfDateColumn otherStringColumn = (DfDateColumn) otherColumn;

        return (thisRowIndex, otherRowIndex) -> new ComparisonResult.DateComparisonResult(
                this.getDate(this.dataFrameRowIndex(thisRowIndex)),
                otherStringColumn.getDate(otherStringColumn.dataFrameRowIndex(otherRowIndex)));
    }
}
