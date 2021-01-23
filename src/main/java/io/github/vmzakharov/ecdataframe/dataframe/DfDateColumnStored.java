package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.set.Pool;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.time.LocalDate;

public class DfDateColumnStored
extends DfDateColumn
implements DfColumnStored
{
    private MutableList<LocalDate> values = Lists.mutable.of();

    private Pool<LocalDate> pool = null;

    public DfDateColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
    }

    public void addDate(LocalDate d)
    {
        if (this.pool == null)
        {
            this.values.add(d);
        }
        else
        {
            this.values.add(this.pool.put(d));
        }
    }

    @Override
    public void enablePooling()
    {
        this.pool = new UnifiedSet<>();
    }

    @Override
    public void seal()
    {
        this.pool = null;
    }

    @Override
    public LocalDate getDate(int rowIndex)
    {
        return this.values.get(rowIndex);
    }

    @Override
    public ImmutableList<LocalDate> toList()
    {
        return this.values.toImmutable();
    }

    @Override
    public void addObject(Object newObject)
    {
        this.addDate((LocalDate) newObject);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isVoid())
        {
            this.addObject(null);
        }
        else if (value.isDate())
        {
            this.addDate(((DateValue) value).dateValue());
        }
        else
        {
            throw new RuntimeException(
                    "Attempting to add a value of type " + value.getType()
                            + " to a date column " + this.getName()
                            + ": " + value.asStringLiteral());
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
        this.values.set(rowIndex, (LocalDate) anObject);
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(null);
    }

    @Override
    public void applyAggregator(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregateFunction)
    {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void ensureCapacity(int newCapacity)
    {
        this.values = Lists.mutable.withInitialCapacity(newCapacity);
    }

    @Override
    protected void addAllItems(ListIterable<LocalDate> items)
    {
        this.values.addAllIterable(items);
    }
}
