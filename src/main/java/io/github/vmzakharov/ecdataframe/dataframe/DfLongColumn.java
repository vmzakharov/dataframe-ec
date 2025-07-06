package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.compare.LongComparisonResult;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.list.primitive.IntInterval;

abstract public class DfLongColumn
extends DfColumnAbstract
{
    public DfLongColumn(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    abstract public long getLong(int rowIndex);

    @Override
    public String getValueAsString(int rowIndex)
    {
        return Long.toString(this.getLong(rowIndex));
    }

    public ImmutableLongList toLongList()
    {
        return this.asLongIterable().toList().toImmutable();
    }

    public LongIterable asLongIterable()
    {
        int rowCount = this.getDataFrame().rowCount();
        if (rowCount == 0)
        {
            return LongLists.immutable.empty();
        }

        return IntInterval.zeroTo(rowCount - 1)
                .asLazy()
                .collectLong(this::getLong);
    }

    @Override
    public ValueType getType()
    {
        return ValueType.LONG;
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
            ((DfLongColumnStored) target).addLong(this.getLong(rowIndex), false);
        }
    }

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        DfLongColumnStored mergedCol = (DfLongColumnStored) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItemsFrom(this);
        mergedCol.addAllItemsFrom((DfLongColumn) other);

        return mergedCol;
    }

    @Override
    public DfColumn copyTo(DataFrame target)
    {
        DfLongColumnStored targetCol = (DfLongColumnStored) this.copyColumnSchemaAndEnsureCapacity(target);

        targetCol.addAllItemsFrom(this);
        return targetCol;
    }

    @Override
    public DfCellComparator columnComparator(DfColumn otherColumn)
    {
        DfLongColumn otherLongColumn = (DfLongColumn) otherColumn;

        return (thisRowIndex, otherRowIndex) -> {
            int thisMappedIndex = this.dataFrameRowIndex(thisRowIndex);
            int otherMappedIndex = otherLongColumn.dataFrameRowIndex(otherRowIndex);

            return new LongComparisonResult(
                () -> this.getLong(thisMappedIndex),
                () -> otherLongColumn.getLong(otherMappedIndex),
                this.isNull(thisMappedIndex),
                otherLongColumn.isNull(otherMappedIndex)
            );
        };
    }
}
