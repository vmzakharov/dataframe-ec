package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;

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

    @Override
    public Object getObject(int rowIndex)
    {
        return this.getLong(rowIndex);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        return new LongValue(this.getLong(rowIndex));
    }

    public abstract ImmutableLongList toLongList();

    public ValueType getType()
    {
        return ValueType.LONG;
    }

    @Override
    public void addRowToColumn(int rowIndex, DfColumn target)
    {
        ((DfLongColumnStored) target).addLong(this.getLong(rowIndex));
    }

    abstract public long sum();

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        DfLongColumn mergedCol = (DfLongColumn) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItems(this.toLongList());
        mergedCol.addAllItems(((DfLongColumn) other).toLongList());

        return mergedCol;
    }

    protected abstract void addAllItems(LongIterable items);
}
