package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.compare.DoubleComparisonResult;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.DoubleIterable;
import org.eclipse.collections.api.list.primitive.ImmutableDoubleList;
import org.eclipse.collections.impl.factory.primitive.DoubleLists;
import org.eclipse.collections.impl.list.primitive.IntInterval;

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
        return this.asDoubleIterable().toList().toImmutable();
    }

    public DoubleIterable asDoubleIterable()
    {
        int rowCount = this.getDataFrame().rowCount();
        if (rowCount == 0)
        {
            return DoubleLists.immutable.empty();
        }

        return IntInterval.zeroTo(rowCount - 1)
                .asLazy()
                .collectDouble(this::getDouble);
    }

    @Override
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
        DfDoubleColumnStored mergedCol = (DfDoubleColumnStored) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItemsFrom(this);
        mergedCol.addAllItemsFrom((DfDoubleColumn) other);

        return mergedCol;
    }

    @Override
    public DfColumn copyTo(DataFrame target)
    {
        DfDoubleColumnStored targetCol = (DfDoubleColumnStored) this.copyColumnSchemaAndEnsureCapacity(target);

        targetCol.addAllItemsFrom(this);
        return targetCol;
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
