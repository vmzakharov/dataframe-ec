package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.set.Pool;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

abstract public class DfObjectColumnStored<T>
extends DfObjectColumnAbstract<T>
implements DfColumnStored
{
    private MutableList<T> values = Lists.mutable.of();

    private Pool<T> pool = null;

    public DfObjectColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
    }

    public DfObjectColumnStored(DataFrame owner, String newName, ListIterable<T> newValues)
    {
        super(owner, newName);
        this.values.addAllIterable(newValues);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            return Value.VOID;
        }

        return this.objectToValue(this.getTypedObject(rowIndex));
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
            this.addObject(value.stringValue());
        }
        else
        {
            throw new RuntimeException(
                    "Attempting to add a value of type " + value.getType()
                    + " to a string column " + this.getName()
                    + ": " + value.asStringLiteral());
        }
    }

    protected void addMyType(T anObject)
    {
        if (this.pool == null)
        {
            this.values.add(anObject);
        }
        else
        {
            this.values.add(this.pool.put(anObject));
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
    public int getSize()
    {
        return this.values.size();
    }

    @Override
    public void setObject(int rowIndex, Object anObject)
    {
        this.values.set(rowIndex, (T) anObject);
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(null);
    }

    @Override
    public Object getObject(int rowIndex)
    {
        return this.values.get(rowIndex);
    }

    @Override
    public T getTypedObject(int rowIndex)
    {
        return this.values.get(rowIndex);
    }

    @Override
    public boolean isNull(int rowIndex)
    {
        return this.values.get(rowIndex) == null;
    }

    @Override
    public ImmutableList<T> toList()
    {
        return this.values.toImmutable();
    }

    @Override
    public Number aggregate(AggregateFunction aggregator)
    {
        return aggregator.<T>applyIterable(this.values);
    }

    @Override
    public void ensureInitialCapacity(int newCapacity)
    {
        this.values = Lists.mutable.withInitialCapacity(newCapacity);
    }

    @Override
    protected void addAllItems(ListIterable<T> items)
    {
        this.values.addAllIterable(items);
    }
}
