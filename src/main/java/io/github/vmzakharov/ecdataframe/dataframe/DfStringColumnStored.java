package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.set.Pool;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

public class DfStringColumnStored
extends DfStringColumn
implements DfColumnStored
{
    private MutableList<String> values = Lists.mutable.of();

    private Pool<String> pool = null;

    public DfStringColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isVoid())
        {
            this.addObject(null);
        }
        else if (value.isString())
        {
            this.addString(value.stringValue());
        }
        else
        {
            throw new RuntimeException(
                    "Attempting to add a value of type " + value.getType()
                    + " to a string column " + this.getName()
                    + ": " + value.asStringLiteral());
        }
    }

    public void addString(String s)
    {
        if (pool == null)
        {
            this.values.add(s);
        }
        else
        {
            this.values.add(this.pool.put(s));
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
    public void addObject(Object newObject)
    {
        this.addString((String) newObject);
    }

    @Override
    public String getValueAsString(int rowIndex)
    {
        return this.getString(rowIndex);
    }

    public String getString(int rowIndex)
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
        this.values.set(rowIndex, (String) anObject);
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(null);
    }

    @Override
    public ImmutableList<String> toList()
    {
        return this.values.toImmutable();
    }

    @Override
    public void applyAggregator(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregateFunction)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Number aggregate(AggregateFunction aggregator)
    {
        return aggregator.<String>applyIterable(this.values);
    }

    @Override
    public void ensureCapacity(int newCapacity)
    {
        this.values = Lists.mutable.withInitialCapacity(newCapacity);
    }

    @Override
    protected void addAllItems(ListIterable<String> items)
    {
        this.values.addAllIterable(items);
    }
}
