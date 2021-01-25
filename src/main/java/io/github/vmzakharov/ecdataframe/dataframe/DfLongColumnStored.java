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
    private boolean nullsEnabled = false;
    private MutableBooleanList nullMap = null;

    private MutableLongList values = LongLists.mutable.of();

    public DfLongColumnStored(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isLong())
        {
            this.addLong(((LongValue) value).longValue());
        }
        else
        {
            throw new RuntimeException(
                    "Attempting to add a value of type " + value.getType()
                    + " to a long integer column " + this.getName()
                    + ": " + value.asStringLiteral());
        }
    }

    public void addLong(long aLong)
    {
        this.values.add(aLong);
    }

    public long getLong(int rowIndex)
    {
        if (this.nullsAreEnabled() && this.isNull(rowIndex))
        {
            throw new NullPointerException();
        }
        return this.values.get(rowIndex);
    }

    @Override
    public ImmutableLongList toLongList()
    {
        return this.values.toImmutable();
    }

    @Override
    public long aggregate(AggregateFunction aggregateFunction)
    {
        return aggregateFunction.applyLongIterable(this.values);
    }

    @Override
    public void addObject(Object newObject)
    {
        if (newObject == null)
        {
            this.addLong(0L);

            if (this.nullsAreEnabled())
            {
                this.nullMap.add(true);
            }
        }
        else
        {
            this.addLong(((Number) newObject).longValue());
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
            this.setNull(rowIndex);
            this.values.set(rowIndex, 0L);
        }
        else
        {
            this.values.set(rowIndex, (Long) anObject);
            this.clearNull(rowIndex);
        }
    }

    @Override
    public void applyAggregator(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregator)
    {
        long stored = this.isNull(targetRowIndex) ? aggregator.longInitialValue() : this.values.get(targetRowIndex);
        this.values.set(targetRowIndex, aggregator.longAccumulator(
                stored,
                ((DfLongColumn) sourceColumn).getLong(sourceRowIndex)));
        this.clearNull(targetRowIndex);
    }

    private void clearNull(int rowIndex)
    {
        if (this.nullsAreEnabled())
        {
            this.nullMap.set(rowIndex, false);
        }
    }

    private void setNull(int rowIndex)
    {
        if (this.nullsAreEnabled())
        {
            this.nullMap.set(rowIndex, true);
        }
    }

    private boolean isNull(int rowIndex)
    {
        return this.nullsEnabled && this.nullMap.get(rowIndex);
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(0L);

        if (this.nullsEnabled)
        {
            this.nullMap.add(true);
        }
    }

    @Override
    public void ensureCapacity(int newCapacity)
    {
        this.values = LongLists.mutable.withInitialCapacity(newCapacity);
    }

    protected void addAllItems(LongIterable items)
    {
        this.values.addAll(items);
    }

    public void enableNulls()
    {
        this.nullsEnabled = true;
        this.nullMap = BooleanLists.mutable.withInitialCapacity(this.getSize());
    }

    public void disableNulls()
    {
        this.nullsEnabled = false;
        this.nullMap = null;
    }

    public boolean nullsAreEnabled()
    {
        return this.nullsEnabled;
    }
}
