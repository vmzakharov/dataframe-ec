package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.compare.DoubleComparisonResult;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.FloatIterable;
import org.eclipse.collections.api.list.primitive.ImmutableFloatList;
import org.eclipse.collections.impl.factory.primitive.FloatLists;
import org.eclipse.collections.impl.list.primitive.IntInterval;

abstract public class DfFloatColumn
extends DfColumnAbstract
{
    public DfFloatColumn(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    abstract public float getFloat(int rowIndex);

    @Override
    public String getValueAsString(int rowIndex)
    {
        return Float.toString(this.getFloat(rowIndex));
    }

    public ImmutableFloatList toFloatList()
    {
        return this.asFloatIterable().toList().toImmutable();
    }

    public FloatIterable asFloatIterable()
    {
        int rowCount = this.getDataFrame().rowCount();
        if (rowCount == 0)
        {
            return FloatLists.immutable.empty();
        }

        return IntInterval.zeroTo(rowCount - 1)
                .asLazy()
                .collectFloat(this::getFloat);
    }

    @Override
    public ValueType getType()
    {
        return ValueType.FLOAT;
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
            ((DfFloatColumnStored) target).addFloat(this.getFloat(rowIndex));
        }
    }

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        DfFloatColumn mergedCol = (DfFloatColumn) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItemsFrom(this);
        mergedCol.addAllItemsFrom((DfFloatColumn) other);

        return mergedCol;
    }

    @Override
    public DfColumn copyTo(DataFrame target)
    {
        DfFloatColumn targetCol = (DfFloatColumn)  this.copyColumnSchemaAndEnsureCapacity(target);

        targetCol.addAllItemsFrom(this);
        return targetCol;
    }

    protected abstract void addAllItemsFrom(DfFloatColumn items);

    @Override
    public DfCellComparator columnComparator(DfColumn otherColumn)
    {
        DfFloatColumn otherFloatColumn = (DfFloatColumn) otherColumn;

        return (thisRowIndex, otherRowIndex) -> {
            int thisMappedIndex = this.dataFrameRowIndex(thisRowIndex);
            int otherMappedIndex = otherFloatColumn.dataFrameRowIndex(otherRowIndex);

            return new DoubleComparisonResult(
                () -> this.getFloat(thisMappedIndex),
                () -> otherFloatColumn.getFloat(otherMappedIndex),
                this.isNull(thisMappedIndex),
                otherFloatColumn.isNull(otherMappedIndex)
            );
        };
    }
}
