package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.NumberValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.DoubleIterable;
import org.eclipse.collections.api.list.primitive.ImmutableDoubleList;
import org.eclipse.collections.api.list.primitive.MutableDoubleList;
import org.eclipse.collections.impl.factory.primitive.DoubleLists;

public class DfDoubleColumnStored
extends DfDoubleColumn
implements DfColumnStored
{
    private MutableDoubleList values = DoubleLists.mutable.of();

    public DfDoubleColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isNumber())
        {
            this.addDouble(((NumberValue) value).doubleValue());
        }
        else
        {
            throw new RuntimeException(
                    "Attempting to add a value of type " + value.getType()
                    + " to a double column " + this.getName()
                    + ": " + value.asStringLiteral());
        }
    }

    public void addDouble(double d)
    {
        this.values.add(d);
    }

    @Override
    public void addObject(Object newObject)
    {
        if (newObject == null)
        {
            this.addDouble(0.0);
        }
        else
        {
            this.addDouble(((Number) newObject).doubleValue());
        }
    }

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

    public double getDouble(int rowIndex)
    {
        return this.values.get(rowIndex);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        return new DoubleValue(this.getDouble(rowIndex));
    }

    @Override
    public double sum()
    {
        return this.values.sum();
    }

    @Override
    public int getSize()
    {
        return this.values.size();
    }

    @Override
    public void setObject(int rowIndex, Object anObject)
    {
        this.values.set(rowIndex, (Double) anObject);
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(0.0);
    }

    @Override
    public void incrementFrom(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
    {
        double stored = this.values.get(targetRowIndex);
        this.values.set(targetRowIndex, stored + ((DfDoubleColumn) sourceColumn).getDouble(sourceRowIndex));
    }

    @Override
    public void ensureCapacity(int newCapacity)
    {
        this.values = DoubleLists.mutable.withInitialCapacity(newCapacity);
    }

    @Override
    protected void addAllItems(DoubleIterable items)
    {
        this.values.addAll(items);
    }

    @Override
    public boolean isStored()
    {
        return true;
    }

    @Override
    public ImmutableDoubleList toDoubleList()
    {
        return this.values.toImmutable();
    }
}
