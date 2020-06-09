package org.modelscript.dataframe;

import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.modelscript.expr.value.LongValue;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.value.ValueType;

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
}
