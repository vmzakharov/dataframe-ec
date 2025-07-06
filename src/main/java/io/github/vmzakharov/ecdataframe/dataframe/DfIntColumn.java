package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.compare.LongComparisonResult;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.list.primitive.ImmutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.list.primitive.IntInterval;

abstract public class DfIntColumn
extends DfColumnAbstract
{
    public DfIntColumn(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    abstract public int getInt(int rowIndex);

    @Override
    public String getValueAsString(int rowIndex)
    {
        return Integer.toString(this.getInt(rowIndex));
    }

    public ImmutableIntList toIntList()
    {
        return this.asIntIterable().toList().toImmutable();
    }

    public IntIterable asIntIterable()
    {
        int rowCount = this.getDataFrame().rowCount();
        if (rowCount == 0)
        {
            return IntLists.immutable.empty();
        }

        return IntInterval
                .zeroTo(rowCount - 1)
                .asLazy()
                .collectInt(this::getInt);
    }

    @Override
    public ValueType getType()
    {
        return ValueType.INT;
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
            ((DfIntColumnStored) target).addInt(this.getInt(rowIndex), false);
        }
    }

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        DfIntColumnStored mergedCol = (DfIntColumnStored) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItemsFrom(this);
        mergedCol.addAllItemsFrom((DfIntColumn) other);

        return mergedCol;
    }

    @Override
    public DfColumn copyTo(DataFrame target)
    {
        DfIntColumnStored targetCol = (DfIntColumnStored)  this.copyColumnSchemaAndEnsureCapacity(target);

        targetCol.addAllItemsFrom(this);
        return targetCol;
    }

    @Override
    public DfCellComparator columnComparator(DfColumn otherColumn)
    {
        DfIntColumn otherIntColumn = (DfIntColumn) otherColumn;

        return (thisRowIndex, otherRowIndex) -> {
            int thisMappedIndex = this.dataFrameRowIndex(thisRowIndex);
            int otherMappedIndex = otherIntColumn.dataFrameRowIndex(otherRowIndex);

            return new LongComparisonResult(
                () -> this.getInt(thisMappedIndex),
                () -> otherIntColumn.getInt(otherMappedIndex),
                this.isNull(thisMappedIndex),
                otherIntColumn.isNull(otherMappedIndex)
            );
        };
    }
}
