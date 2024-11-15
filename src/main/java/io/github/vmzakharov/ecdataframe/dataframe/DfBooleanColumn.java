package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.compare.BooleanComparisonResult;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.BooleanIterable;
import org.eclipse.collections.api.list.primitive.ImmutableBooleanList;
import org.eclipse.collections.impl.factory.primitive.BooleanLists;
import org.eclipse.collections.impl.list.primitive.IntInterval;

abstract public class DfBooleanColumn
extends DfColumnAbstract
{
    public DfBooleanColumn(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    abstract public boolean getBoolean(int rowIndex);

    @Override
    public String getValueAsString(int rowIndex)
    {
        return Boolean.toString(this.getBoolean(rowIndex));
    }

    public ImmutableBooleanList toBooleanList()
    {
        return this.asBooleanIterable().toList().toImmutable();
    }

    public BooleanIterable asBooleanIterable()
    {
        int rowCount = this.getDataFrame().rowCount();
        if (rowCount == 0)
        {
            return BooleanLists.immutable.empty();
        }

        return IntInterval
                .zeroTo(rowCount - 1)
                .asLazy()
                .collectBoolean(this::getBoolean);
    }

    @Override
    public ValueType getType()
    {
        return ValueType.BOOLEAN;
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
            ((DfBooleanColumnStored) target).addBoolean(this.getBoolean(rowIndex), false);
        }
    }

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        DfBooleanColumn mergedCol = (DfBooleanColumn) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItemsFrom(this);
        mergedCol.addAllItemsFrom((DfBooleanColumn) other);

        return mergedCol;
    }

    @Override
    public DfColumn copyTo(DataFrame target)
    {
        DfBooleanColumn targetCol = (DfBooleanColumn)  this.copyColumnSchemaAndEnsureCapacity(target);

        targetCol.addAllItemsFrom(this);
        return targetCol;
    }

    protected abstract void addAllItemsFrom(DfBooleanColumn items);

    @Override
    public DfCellComparator columnComparator(DfColumn otherColumn)
    {
        DfBooleanColumn otherBooleanColumn = (DfBooleanColumn) otherColumn;

        return (thisRowIndex, otherRowIndex) -> {
            int thisMappedIndex = this.dataFrameRowIndex(thisRowIndex);
            int otherMappedIndex = otherBooleanColumn.dataFrameRowIndex(otherRowIndex);

            return new BooleanComparisonResult(
                () -> this.getBoolean(thisMappedIndex),
                () -> otherBooleanColumn.getBoolean(otherMappedIndex),
                this.isNull(thisMappedIndex),
                otherBooleanColumn.isNull(otherMappedIndex)
            );
        };
    }
}
