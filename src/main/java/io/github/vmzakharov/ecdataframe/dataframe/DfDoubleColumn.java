package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.DoubleIterable;
import org.eclipse.collections.api.list.primitive.ImmutableDoubleList;

abstract public class DfDoubleColumn
extends DfColumnAbstract
{
    public DfDoubleColumn(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    abstract public double getDouble(int rowIndex);

    @Override
    public String getValueAsString(int rowIndex)
    {
        return Double.toString(this.getDouble(rowIndex));
    }

    public abstract ImmutableDoubleList toDoubleList();

    public ValueType getType()
    {
        return ValueType.DOUBLE;
    }

    @Override
    public void addRowToColumn(int rowIndex, DfColumn target)
    {
        if (this.isNull(rowIndex))
        {
            target.addEmptyValue();
        }
        else
        {
            ((DfDoubleColumnStored) target).addDouble(this.getDouble(rowIndex));
        }
    }

    protected abstract void addAllItems(DoubleIterable items);

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        DfDoubleColumn mergedCol = (DfDoubleColumn) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItemsFrom(this);
        mergedCol.addAllItemsFrom((DfDoubleColumn) other);

        return mergedCol;
    }

    protected abstract void addAllItemsFrom(DfDoubleColumn doubleColumn);

    @Override
    public DfCellComparator columnComparator(DfColumn otherColumn)
    {
        DfDoubleColumn otherDoubleColumn = (DfDoubleColumn) otherColumn;

        return (thisRowIndex, otherRowIndex) -> {
            int thisMappedIndex = this.dataFrameRowIndex(thisRowIndex);
            int otherMappedIndex = otherDoubleColumn.dataFrameRowIndex(otherRowIndex);

            return new ComparisonResult.DoubleComparisonResult(
                    () -> this.getDouble(thisMappedIndex),
                    () -> otherDoubleColumn.getDouble(otherMappedIndex),
                    this.isNull(thisMappedIndex),
                    otherDoubleColumn.isNull(otherMappedIndex)
            );
        };
    }
}
