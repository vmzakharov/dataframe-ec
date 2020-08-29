package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.primitive.ImmutableDoubleList;

abstract public class DfDoubleColumn
extends DfColumnAbstract
{
    public DfDoubleColumn(String newName)
    {
        super(newName);
    }

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

    @Override
    public Object getObject(int rowIndex)
    {
        return this.getDouble(rowIndex);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        return new DoubleValue(this.getDouble(rowIndex));
    }

    public abstract ImmutableDoubleList toDoubleList();

    public ValueType getType()
    {
        return ValueType.DOUBLE;
    }

    @Override
    public void addRowToColumn(int rowIndex, DfColumn target)
    {
        ((DfDoubleColumnStored) target).addDouble(this.getDouble(rowIndex));
    }

    abstract public double sum();
}
