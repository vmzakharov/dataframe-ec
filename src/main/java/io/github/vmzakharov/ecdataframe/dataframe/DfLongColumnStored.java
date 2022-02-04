package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.api.list.primitive.MutableBooleanList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.BooleanLists;
import org.eclipse.collections.impl.factory.primitive.LongLists;

public class DfLongColumnStored
extends DfLongColumn
implements DfColumnStored
{
    static private final long NULL_FILLER = Long.MIN_VALUE; // not the actual null marker, but makes debugging easier

    private MutableBooleanList nullMap = BooleanLists.mutable.of();
    private MutableLongList values = LongLists.mutable.of();

    public DfLongColumnStored(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    public DfLongColumnStored(DataFrame newDataFrame, String newName, LongIterable newValues)
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
        if (value.isLong())
        {
            this.addLong(((LongValue) value).longValue(), false);
        }
        else if (value.isVoid())
        {
            this.addEmptyValue();
        }
        else
        {
            throw new RuntimeException(
                    "Attempting to add a value of type " + value.getType()
                    + " to a long integer column " + this.getName()
                    + ": " + value.asStringLiteral());
        }
    }

    public void addLong(long aLong, boolean isNullValue)
    {
        this.values.add(aLong);
        this.nullMap.add(isNullValue);
    }

    public long getLong(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            throw new NullPointerException("Null value at " + this.getName() + "[" + rowIndex + "]");
        }

        return this.getLongWithoutNullCheck(rowIndex);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            return Value.VOID;
        }

        return new LongValue(this.getLong(rowIndex));
    }

    @Override
    public ImmutableLongList toLongList()
    {
        return this.values.toImmutable();
    }

    @Override
    public Number aggregate(AggregateFunction aggregateFunction)
    {
        return aggregateFunction.applyLongIterable(this.values);
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
            this.values.add(((Number) newObject).longValue());
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
            this.values.set(rowIndex, (Long) anObject);
            this.clearNull(rowIndex);
        }
    }

    public void setLong(int rowIndex, long value)
    {
        this.values.set(rowIndex, value);
    }

    @Override
    public void applyAggregator(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregator)
    {
        long stored = this.isNull(targetRowIndex) ? aggregator.longInitialValue() : this.values.get(targetRowIndex);
        this.values.set(targetRowIndex,
                aggregator.longAccumulator(stored, aggregator.getLongValue(sourceColumn, sourceRowIndex))
        );
        this.clearNull(targetRowIndex);
    }

    private void clearNull(int rowIndex)
    {
        this.nullMap.set(rowIndex, false);
    }

    private void setNull(int rowIndex)
    {
        this.nullMap.set(rowIndex, true);
    }

    public Object getObject(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            return null;
        }

        return this.getLongWithoutNullCheck(rowIndex);
    }

    private long getLongWithoutNullCheck(int rowIndex)
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
        this.values = LongLists.mutable.withInitialCapacity(newCapacity);
        this.nullMap = BooleanLists.mutable.withInitialCapacity(newCapacity);
    }

    @Override
    protected void addAllItemsFrom(DfLongColumn longColumn)
    {
        this.values.addAll(longColumn.toLongList());

        int size = longColumn.getSize();
        for (int rowIndex = 0; rowIndex < size; rowIndex++)
        {
            this.nullMap.add(longColumn.isNull(rowIndex));
        }
    }
}
