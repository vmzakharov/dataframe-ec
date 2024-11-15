package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.BooleanIterable;
import org.eclipse.collections.api.list.primitive.MutableBooleanList;
import org.eclipse.collections.impl.factory.primitive.BooleanLists;

public class DfBooleanColumnStored
extends DfBooleanColumn
implements DfColumnStored
{
    static private final boolean NULL_FILLER = false;

    private MutableBooleanList nullMap = BooleanLists.mutable.empty();
    private MutableBooleanList values = BooleanLists.mutable.empty();

    public DfBooleanColumnStored(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    public DfBooleanColumnStored(DataFrame newDataFrame, String newName, BooleanIterable newValues)
    {
        this(newDataFrame, newName);

        this.values.addAll(newValues);

        this.nullMap = BooleanLists.mutable.withInitialCapacity(this.values.size());
        for (int i = 0; i < this.values.size(); i++)
        {
            this.nullMap.add(false);
        }
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isBoolean())
        {
            this.addBoolean(((BooleanValue) value).isTrue(), false);
        }
        else if (value.isVoid())
        {
            this.addEmptyValue();
        }
        else
        {
            this.throwAddingIncompatibleValueException(value);
        }
    }

    public void addBoolean(boolean aBoolean, boolean isNullValue)
    {
        this.values.add(aBoolean);
        this.nullMap.add(isNullValue);
    }

    @Override
    public boolean getBoolean(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            throw new NullPointerException("Null value at " + this.getName() + "[" + rowIndex + "]");
        }

        return this.getBooleanWithoutNullCheck(rowIndex);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            return Value.VOID;
        }

        return BooleanValue.valueOf(this.getBooleanWithoutNullCheck(rowIndex));
    }

    @Override
    public void addObject(Object newObject)
    {
        if (newObject == null)
        {
            this.values.add(NULL_FILLER);
            this.nullMap.add(true);
        }
        else
        {
            this.values.add((Boolean) newObject);
            this.nullMap.add(false);
        }
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
            this.values.set(rowIndex, NULL_FILLER);
            this.setNull(rowIndex);
        }
        else
        {
            this.values.set(rowIndex, (Boolean) anObject);
            this.clearNull(rowIndex);
        }
    }

    public void setBoolean(int rowIndex, boolean value)
    {
        this.values.set(rowIndex, value);
        this.clearNull(rowIndex);
    }

    @Override
    public void aggregateValueInto(int rowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregator)
    {
        // aggregator.aggregateValueIntoInt(this, rowIndex, sourceColumn,  sourceRowIndex);
    }

    private void clearNull(int rowIndex)
    {
        this.nullMap.set(rowIndex, false);
    }

    private void setNull(int rowIndex)
    {
        this.nullMap.set(rowIndex, true);
    }

    @Override
    public Object getObject(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            return null;
        }

        return this.getBooleanWithoutNullCheck(rowIndex);
    }

    private boolean getBooleanWithoutNullCheck(int rowIndex)
    {
        return this.values.get(rowIndex);
    }

    @Override
    public boolean isNull(int rowIndex)
    {
        return this.nullMap.get(rowIndex);
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(NULL_FILLER);
        this.nullMap.add(true);
    }

    @Override
    public void ensureInitialCapacity(int newCapacity)
    {
        this.values = BooleanLists.mutable.withInitialCapacity(newCapacity);
        this.nullMap = BooleanLists.mutable.withInitialCapacity(newCapacity);
    }

    @Override
    protected void addAllItemsFrom(DfBooleanColumn intColumn)
    {
        int size = intColumn.getSize();
        for (int rowIndex = 0; rowIndex < size; rowIndex++)
        {
            if (intColumn.isNull(rowIndex))
            {
                this.addEmptyValue();
            }
            else
            {
                this.addBoolean(intColumn.getBoolean(rowIndex), false);
            }
        }
    }
}
