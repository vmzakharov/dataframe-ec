package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.list.primitive.MutableBooleanList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.BooleanLists;
import org.eclipse.collections.impl.factory.primitive.IntLists;

public class DfIntColumnStored
extends DfIntColumn
implements DfColumnStored
{
    static private final int NULL_FILLER = Integer.MIN_VALUE; // not the actual null marker, but makes debugging easier

    private MutableBooleanList nullMap = BooleanLists.mutable.empty();
    private MutableIntList values = IntLists.mutable.empty();

    public DfIntColumnStored(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    public DfIntColumnStored(DataFrame newDataFrame, String newName, IntIterable newValues)
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
        if (value.isInt())
        {
            this.addInt(((IntValue) value).intValue(), false);
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

    public void addInt(int anInt, boolean isNullValue)
    {
        this.values.add(anInt);
        this.nullMap.add(isNullValue);
    }

    @Override
    public int getInt(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            throw new NullPointerException("Null value at " + this.getName() + "[" + rowIndex + "]");
        }

        return this.getIntWithoutNullCheck(rowIndex);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            return Value.VOID;
        }

        return new IntValue(this.getInt(rowIndex));
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
            this.values.add(((Number) newObject).intValue());
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
            this.values.set(rowIndex, (Integer) anObject);
            this.clearNull(rowIndex);
        }
    }

    public void setInt(int rowIndex, int value)
    {
        this.values.set(rowIndex, value);
        this.clearNull(rowIndex);
    }

    @Override
    public void aggregateValueInto(int rowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregator)
    {
        aggregator.aggregateValueIntoInt(this, rowIndex, sourceColumn,  sourceRowIndex);
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

        return this.getIntWithoutNullCheck(rowIndex);
    }

    private int getIntWithoutNullCheck(int rowIndex)
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
        this.values = IntLists.mutable.withInitialCapacity(newCapacity);
        this.nullMap = BooleanLists.mutable.withInitialCapacity(newCapacity);
    }

    @Override
    protected void addAllItemsFrom(DfIntColumn intColumn)
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
                this.addInt(intColumn.getInt(rowIndex), false);
            }
        }
    }
}
