package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.NumberValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.DoubleIterable;
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

    public DfDoubleColumnStored(DataFrame owner, String newName, DoubleIterable newValues)
    {
        this(owner, newName);
        this.values.addAll(newValues);
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
            this.throwAddingIncompatibleValueException(value);
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
            this.addDouble(Double.NaN);
        }
        else
        {
            this.addDouble(((Number) newObject).doubleValue());
        }
    }

    @Override
    public Value getValue(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            return Value.VOID;
        }

        return new DoubleValue(this.getDoubleWithoutNullCheck(rowIndex));
    }

    @Override
    public String getValueAsString(int rowIndex)
    {
        return Double.toString(this.getDouble(rowIndex));
    }

    @Override
    public Object getObject(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            return null;
        }

        return this.getDoubleWithoutNullCheck(rowIndex);
    }

    @Override
    public double getDouble(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            throw new NullPointerException("Null value at " + this.getName() + "[" + rowIndex + "]");
        }

        return this.getDoubleWithoutNullCheck(rowIndex);
    }

    private double getDoubleWithoutNullCheck(int rowIndex)
    {
        return this.values.get(rowIndex);
    }

    @Override
    public int getSize()
    {
        return this.values.size();
    }

    @Override
    public void setObject(int rowIndex, Object anObject)
    {
        if (anObject == null)
        {
            this.values.set(rowIndex, Double.NaN);
        }
        else
        {
            this.values.set(rowIndex, (Double) anObject);
        }
    }

    public void setDouble(int rowIndex, double value)
    {
        this.values.set(rowIndex, value);
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(Double.NaN);
    }

    @Override
    public void aggregateValueInto(int rowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregator)
    {
        aggregator.aggregateValueIntoDouble(this, rowIndex, sourceColumn, sourceRowIndex);
    }

    @Override
    public void ensureInitialCapacity(int newCapacity)
    {
        this.values = DoubleLists.mutable.withInitialCapacity(newCapacity);
    }

    @Override
    protected void addAllItemsFrom(DfDoubleColumn doubleColumn)
    {
        int size = doubleColumn.getSize();
        for (int rowIndex = 0; rowIndex < size; rowIndex++)
        {
            if (doubleColumn.isNull(rowIndex))
            {
                this.addEmptyValue();
            }
            else
            {
                this.addDouble(doubleColumn.getDouble(rowIndex));
            }
        }
    }

    @Override
    public boolean isNull(int rowIndex)
    {
        return Double.isNaN(this.values.get(rowIndex));
    }

    @Override
    public boolean isStored()
    {
        return true;
    }
}
