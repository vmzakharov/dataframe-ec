package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.compare.DoubleComparisonResult;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.primitive.ImmutableDoubleList;
import org.eclipse.collections.impl.factory.primitive.DoubleLists;
import org.eclipse.collections.impl.list.Interval;

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

    public ImmutableDoubleList toDoubleList()
    {
        if (this.getDataFrame().rowCount() == 0)
        {
            return DoubleLists.immutable.empty();
        }

        return Interval
            .zeroTo(this.getDataFrame().rowCount() - 1)
            .collectDouble(this::getDouble)
            .toList()
            .toImmutable();
    }

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

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        DfDoubleColumn mergedCol = (DfDoubleColumn) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItemsFrom(this);
        mergedCol.addAllItemsFrom((DfDoubleColumn) other);

        return mergedCol;
    }

    public DfColumn copyTo(DataFrame target)
    {
        DfDoubleColumn targetCol = (DfDoubleColumn) this.copyColumnSchema(target);

        targetCol.addAllItemsFrom(this);
        return targetCol;
    }

    protected abstract void addAllItemsFrom(DfDoubleColumn doubleColumn);

    @Override
    public Object aggregate(AggregateFunction aggregateFunction)
    {
        if (this.getSize() == 0)
        {
            return aggregateFunction.defaultDoubleIfEmpty();
        }

        try
        {
            return aggregateFunction.applyToDoubleColumn(this);
        }
        catch (NullPointerException npe)
        {
            return null;
        }
    }

    @Override
    public DfCellComparator columnComparator(DfColumn otherColumn)
    {
        DfDoubleColumn otherDoubleColumn = (DfDoubleColumn) otherColumn;

        return (thisRowIndex, otherRowIndex) -> {
            int thisMappedIndex = this.dataFrameRowIndex(thisRowIndex);
            int otherMappedIndex = otherDoubleColumn.dataFrameRowIndex(otherRowIndex);

            return new DoubleComparisonResult(
                    () -> this.getDouble(thisMappedIndex),
                    () -> otherDoubleColumn.getDouble(otherMappedIndex),
                    this.isNull(thisMappedIndex),
                    otherDoubleColumn.isNull(otherMappedIndex)
            );
        };
    }
}
