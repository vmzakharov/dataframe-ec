package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.FloatValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.FloatIterable;
import org.eclipse.collections.api.list.primitive.MutableFloatList;
import org.eclipse.collections.impl.factory.primitive.FloatLists;

public class DfFloatColumnStored
extends DfFloatColumn
implements DfColumnStored
{
    private MutableFloatList values = FloatLists.mutable.of();

    public DfFloatColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
    }

    public DfFloatColumnStored(DataFrame owner, String newName, FloatIterable newValues)
    {
        this(owner, newName);
        this.values.addAll(newValues);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isFloat())
        {
            this.addFloat(((FloatValue) value).floatValue());
        }
        else
        {
            this.throwAddingIncompatibleValueException(value);
        }
    }

    public void addFloat(float d)
    {
        this.values.add(d);
    }

    @Override
    public void addObject(Object newObject)
    {
        if (newObject == null)
        {
            this.addFloat(Float.NaN);
        }
        else
        {
            this.addFloat((Float) newObject);
        }
    }

    @Override
    public Value getValue(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            return Value.VOID;
        }

        return new FloatValue(this.getFloatWithoutNullCheck(rowIndex));
    }

    @Override
    public String getValueAsString(int rowIndex)
    {
        return Float.toString(this.getFloat(rowIndex));
    }

    @Override
    public Object getObject(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            return null;
        }

        return this.getFloatWithoutNullCheck(rowIndex);
    }

    @Override
    public float getFloat(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            throw new NullPointerException("Null value at " + this.getName() + "[" + rowIndex + "]");
        }

        return this.getFloatWithoutNullCheck(rowIndex);
    }

    private float getFloatWithoutNullCheck(int rowIndex)
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
            this.values.set(rowIndex, Float.NaN);
        }
        else
        {
            this.values.set(rowIndex, (Float) anObject);
        }
    }

    public void setFloat(int rowIndex, float value)
    {
        this.values.set(rowIndex, value);
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(Float.NaN);
    }

    @Override
    public void aggregateValueInto(int rowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregator)
    {
        aggregator.aggregateValueIntoFloat(this, rowIndex, sourceColumn, sourceRowIndex);
    }

    @Override
    public void ensureInitialCapacity(int newCapacity)
    {
        this.values = FloatLists.mutable.withInitialCapacity(newCapacity);
    }

    @Override
    protected void addAllItemsFrom(DfFloatColumn doubleColumn)
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
                this.addFloat(doubleColumn.getFloat(rowIndex));
            }
        }
    }

    @Override
    public boolean isNull(int rowIndex)
    {
        return Float.isNaN(this.values.get(rowIndex));
    }

    @Override
    public boolean isStored()
    {
        return true;
    }
}
